// Package peel contains the actual application specific logic for each
// command which can be performed. It's designed to be able to be used as an
// actual client for bananaq if desired.
package peel

import (
	"time"

	"github.com/mediocregopher/bananaq/core"
	"github.com/mediocregopher/radix.v2/util"
)

// Peel contains all the information needed to actually implement the
// application logic of bananaq. it is intended to be used both as the server
// component and as a client for external applications which want to be able to
// interact with the database directly. All methods on Peel are thread-safe.
type Peel struct {
	c *core.Core
}

// New initializes a Peel struct with a Core, using the given redis Cmder as a
// backer. The Cmder can be either a radix.v2 *pool.Pool or *cluster.Cluster.
// core.Opts may be nil to use default values.
func New(cmder util.Cmder, o *core.Opts) (Peel, error) {
	c, err := core.New(cmder, o)
	return Peel{c}, err
}

// Run performs all the background work needed to support Peel. It will block
// until an error is reached, and then return that. At that point the caller
// should handle the error (i.e. probably log it), and then decide whether to
// stop execution or call Run again.
func (p Peel) Run() error {
	return p.c.Run()
}

// QAddCommand describes the parameters which can be passed into the QAdd
// command
type QAddCommand struct {
	Queue    string    // Required
	Expire   time.Time // Required
	Contents string    // Required
}

// QAdd adds an event to a queue. The Expire will be used to generate an ID.
// IDs are monotonically increasing and unique across the cluster, so the ID may
// correspond to a very slightly greater time than the given Expire. That
// (potentially slightly greater) time will also be used as the point in time
// after which the event is no longer valid.
func (p Peel) QAdd(c QAddCommand) (core.ID, error) {
	e, err := p.c.NewEvent(core.NewTS(c.Expire), c.Contents)
	if err != nil {
		return 0, err
	}

	// We always store the event data itself with an extra 30 seconds until it
	// expires, just in case a consumer gets it just as its expire time hits
	if err := p.c.SetEvent(e, 30*time.Second); err != nil {
		return 0, err
	}

	keyAvailID := queueAvailableByID(c.Queue)
	keyAvailEx := queueAvailableByExpire(c.Queue)

	qa := core.QueryActions{
		KeyBase: keyAvailID.Base,
		QueryActions: []core.QueryAction{
			{
				QuerySelector: &core.QuerySelector{
					Key:    keyAvailID,
					Events: []core.Event{e},
				},
			},
			{
				QueryAddTo: &core.QueryAddTo{
					Keys: []core.Key{keyAvailID},
				},
			},
			{
				QueryAddTo: &core.QueryAddTo{
					Keys:          []core.Key{keyAvailEx},
					ExpireAsScore: true,
				},
			},
		},
	}
	if _, err := p.c.Query(qa); err != nil {
		return 0, err
	}

	p.c.KeyNotify(keyAvailID)

	return e.ID, nil
}

// QGetCommand describes the parameters which can be passed into the QGet
// command
type QGetCommand struct {
	Queue         string // Required
	ConsumerGroup string // Required
	AckDeadline   time.Time
	BlockUntil    time.Time
}

// TODO I'm *probably* only actually using inProgByID and done to get their
// "newest" event that has been processed. They're effectively being used as
// pointers within the "queue". I might be able to instead set them as keys
// instead of sorted sets.  which would be cleaner and save some space. I think
// I still need inProbByAck to be a sorted set, since that needs to be ranged
// over, and redo definitely needs to be a set.

// QGet retrieves an available event from the given queue for the given consumer
// group.
//
// If AckDeadline is given, then the consumer has until then to QAck the
// Event before it is placed back in the queue for this consumer group. If
// AckDeadline is not set, then the Event will never be placed back, and QAck
// isn't necessary.
//
// An empty event is returned if there are no available events for the queue.
func (p Peel) QGet(c QGetCommand) (core.Event, error) {
	if c.BlockUntil.IsZero() {
		return p.qgetDirect(c)
	}

	now := time.Now()
	timeoutCh := time.After(c.BlockUntil.Sub(now))
	keyAvailID := queueAvailableByID(c.Queue)

	for {
		stopCh := make(chan struct{})
		pushCh := p.c.KeyWait(keyAvailID, stopCh)

		if e, err := p.qgetDirect(c); err != nil || e.ID != 0 {
			return e, err
		}

		select {
		case <-pushCh:
		case <-timeoutCh:
			return core.Event{}, nil
		}

		close(stopCh)
	}
}

func (p Peel) qgetDirect(c QGetCommand) (core.Event, error) {
	now := time.Now()
	keyAvailID := queueAvailableByID(c.Queue)
	keyInProgID := queueInProgressByID(c.Queue, c.ConsumerGroup)
	keyInProgAck := queueInProgressByAck(c.Queue, c.ConsumerGroup)
	keyRedo := queueRedo(c.Queue, c.ConsumerGroup)
	keyDone := queueDone(c.Queue, c.ConsumerGroup)
	keyInUse := queueInUseByExpire(c.Queue, c.ConsumerGroup)

	// Depending on if Expire is set, we might add the event to the inProgs or
	// done
	var inProgOrDone []core.QueryAction
	if !c.AckDeadline.IsZero() {
		inProgOrDone = []core.QueryAction{
			{
				QueryAddTo: &core.QueryAddTo{
					Keys: []core.Key{keyInProgID},
				},
			},
			{
				QueryAddTo: &core.QueryAddTo{
					Keys:  []core.Key{keyInProgAck},
					Score: core.NewTS(c.AckDeadline),
				},
			},
			{
				QueryAddTo: &core.QueryAddTo{
					Keys:          []core.Key{keyInUse},
					ExpireAsScore: true,
				},
			},
		}
	} else {
		inProgOrDone = []core.QueryAction{
			{
				QueryAddTo: &core.QueryAddTo{
					Keys: []core.Key{keyDone},
				},
			},
			{
				QueryAddTo: &core.QueryAddTo{
					Keys:          []core.Key{keyInUse},
					ExpireAsScore: true,
				},
			},
		}
	}

	breakIfFound := core.QueryAction{
		Break: true,
		QueryConditional: core.QueryConditional{
			IfInput: true,
		},
	}

	var qq []core.QueryAction
	// First, if there's any Events in redo, we grab the first one from there
	// and move it to inProg/done
	qq = append(qq,
		core.QueryAction{
			QuerySelector: &core.QuerySelector{
				Key:            keyRedo,
				PosRangeSelect: []int64{0, 0},
			},
		},
		core.QueryAction{
			RemoveFrom: []core.Key{keyRedo},
		},
	)
	qq = append(qq, inProgOrDone...)
	qq = append(qq, breakIfFound)

	// Otherwise, we grab the most recent Events from both inProgByID and done
	qq = append(qq,
		core.QueryAction{
			QuerySelector: &core.QuerySelector{
				Key:            keyInProgID,
				PosRangeSelect: []int64{-1, -1},
			},
		},
		core.QueryAction{
			QuerySelector: &core.QuerySelector{
				Key:            keyDone,
				PosRangeSelect: []int64{-1, -1},
				Union:          true,
			},
		},
		core.QueryAction{
			QuerySelector: &core.QuerySelector{
				Key: keyAvailID,
				QueryEventRangeSelect: &core.QueryEventRangeSelect{
					QueryScoreRange: core.QueryScoreRange{
						MinFromInput: true,
						MinExcl:      true,
					},
					Limit: 1,
				},
			},
			QueryConditional: core.QueryConditional{
				IfInput: true,
			},
		},
	)

	// If we got an event from before, add it to inProgs/done and return
	qq = append(qq, inProgOrDone...)
	qq = append(qq, breakIfFound)

	// The queue has no activity, simply get the first event in avail. Only
	// applies if both done and inProg are actually empty. If they're not and
	// we're here it means that the queue has simply been fully processed
	// thusfar
	qq = append(qq, core.QueryAction{
		QuerySelector: &core.QuerySelector{
			Key:            keyAvailID,
			PosRangeSelect: []int64{0, 0},
		},
		QueryConditional: core.QueryConditional{
			And: []core.QueryConditional{
				{
					IfEmpty: &keyDone,
				},
				{
					IfEmpty: &keyInProgID,
				},
			},
		},
	})
	qq = append(qq, inProgOrDone...)

	qa := core.QueryActions{
		KeyBase:      keyAvailID.Base,
		QueryActions: qq,
		Now:          now,
	}

	ee, err := p.c.Query(qa)
	if err != nil {
		return core.Event{}, err
	} else if len(ee) == 0 {
		return core.Event{}, nil
	}

	return p.c.GetEvent(ee[0].ID)
}

// QAckCommand describes the parameters which can be passed into the QAck
// command
type QAckCommand struct {
	Queue         string     // Required
	ConsumerGroup string     // Required
	Event         core.Event // Required, Contents field optional
}

// QAck acknowledges that an event has been successfully processed and should
// not be re-processed. Only applicable for Events which were gotten through a
// QGet with an AckDeadline. Returns true if the Event was successfully
// acknowledged. false will be returned if the deadline was missed, and
// therefore some other consumer may re-process the Event later.
func (p Peel) QAck(c QAckCommand) (bool, error) {
	now := time.Now()

	keyInProgID := queueInProgressByID(c.Queue, c.ConsumerGroup)
	keyInProgAck := queueInProgressByAck(c.Queue, c.ConsumerGroup)
	keyDone := queueDone(c.Queue, c.ConsumerGroup)

	qa := core.QueryActions{
		KeyBase: keyDone.Base,
		QueryActions: []core.QueryAction{
			{
				QuerySelector: &core.QuerySelector{
					Key: keyInProgAck,
					QueryEventScoreSelect: &core.QueryEventScoreSelect{
						Event: c.Event,
						Min:   core.NewTS(now),
					},
				},
			},
			{
				Break: true,
				QueryConditional: core.QueryConditional{
					IfNoInput: true,
				},
			},
			{
				RemoveFrom: []core.Key{keyInProgID, keyInProgAck},
			},
			{
				QueryAddTo: &core.QueryAddTo{
					Keys: []core.Key{keyDone},
				},
			},
		},
		Now: now,
	}

	ee, err := p.c.Query(qa)
	if err != nil {
		return false, err
	}
	return len(ee) > 0, nil
}

// Clean finds all the events which were retrieved for the given
// queue/consumerGroup which weren't ack'd by the deadline, and makes them
// available to be retrieved again. This should be called periodically for all
// queue/consumerGroups.
func (p Peel) Clean(queue, consumerGroup string) error {
	now := time.Now()
	nowTS := core.NewTS(now)

	keyInProgID := queueInProgressByID(queue, consumerGroup)
	keyInProgAck := queueInProgressByAck(queue, consumerGroup)
	keyDone := queueDone(queue, consumerGroup)
	keyRedo := queueRedo(queue, consumerGroup)
	keyInUse := queueInUseByExpire(queue, consumerGroup)

	qsr := core.QueryScoreRange{
		Max: nowTS,
	}
	qa := core.QueryActions{
		KeyBase: keyDone.Base,
		QueryActions: []core.QueryAction{
			// First find all events who missed their ack deadline, remove them
			// from both inProgs and add them to redo
			{
				QuerySelector: &core.QuerySelector{
					Key: keyInProgAck,
					QueryEventRangeSelect: &core.QueryEventRangeSelect{
						QueryScoreRange: qsr,
					},
				},
			},
			{
				RemoveFrom: []core.Key{keyInProgAck, keyInProgID},
			},
			{
				QueryAddTo: &core.QueryAddTo{
					Keys: []core.Key{keyRedo},
				},
			},

			// Next find all events in inUse that have outright expired,
			// remove them from all of our sets
			{
				QuerySelector: &core.QuerySelector{
					Key: keyInUse,
					QueryEventRangeSelect: &core.QueryEventRangeSelect{
						QueryScoreRange: qsr,
					},
				},
			},
			{
				RemoveFrom: []core.Key{
					keyInProgAck,
					keyInProgID,
					keyDone,
					keyRedo,
					keyInUse,
				},
			},
		},
		Now: now,
	}

	_, err := p.c.Query(qa)
	return err
}

// CleanAvailable cleans up the stored Events for a given queue across all
// consumer groups, removing those which have expired.
func (p Peel) CleanAvailable(queue string) error {
	now := time.Now()
	nowTS := core.NewTS(now)

	keyAvailID := queueAvailableByID(queue)
	keyAvailEx := queueAvailableByExpire(queue)
	qa := core.QueryActions{
		KeyBase: keyAvailID.Base,
		QueryActions: []core.QueryAction{
			{
				QuerySelector: &core.QuerySelector{
					Key: keyAvailEx,
					QueryEventRangeSelect: &core.QueryEventRangeSelect{
						QueryScoreRange: core.QueryScoreRange{
							Max: nowTS,
						},
					},
				},
			},
			{
				RemoveFrom: []core.Key{keyAvailID, keyAvailEx},
			},
		},
		Now: now,
	}

	_, err := p.c.Query(qa)
	return err
}

// TODO CleanAll

// QStatusCommand describes the parameters which can be passed into the QStatus
// command
type QStatusCommand struct {
	Queue         string // Required
	ConsumerGroup string // Required
}

// QueueStats are available statistics about a queue (per consumer group) at any
// given moment. Note that the only events which are considered for any queue
// are those which haven't expired or been cleaned up
type QueueStats struct {
	// Number of events for the queue in the system. Will be the same regardless
	// of consumer group
	Total uint64

	// Number of events the consumer group has yet to process for the queue
	Available uint64

	// Number of events currently being worked on by the consumer group
	InProgress uint64

	// Number of events awaiting being re-attempted by the consumer group
	Redo uint64

	// Number of events the consumer group has successfully processed
	Done uint64
}

// QStatus returns information about the given queues relative to the given
// consumer group. See the QueueStats docs for more on what exactly is returned.
func (p Peel) QStatus(c QStatusCommand) (QueueStats, error) {
	keyAvailID := queueAvailableByID(c.Queue)
	keyInProgID := queueInProgressByID(c.Queue, c.ConsumerGroup)
	keyRedo := queueRedo(c.Queue, c.ConsumerGroup)
	keyDone := queueDone(c.Queue, c.ConsumerGroup)

	var qs QueueStats
	counts, err := p.c.SetCounts(keyAvailID, keyInProgID, keyRedo, keyDone)
	if err != nil {
		return qs, err
	}

	qs.Total = counts[0]
	qs.InProgress = counts[1]
	qs.Redo = counts[2]
	qs.Done = counts[3]
	qs.Available = qs.Total - qs.InProgress - qs.Redo - qs.Done
	return qs, nil
}
