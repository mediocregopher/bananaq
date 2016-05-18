// Package peel contains the actual application specific logic for each
// command which can be performed. It's designed to be able to be used as an
// actual client for bananaq if desired.
package peel

import (
	"time"

	"github.com/mediocregopher/bananaq/core"
)

// Peel contains all the information needed to actually implement the
// application logic of bananaq. it is intended to be used both as the server
// component and as a client for external applications which want to be able to
// interact with the database directly. It can be initialized manually, or using
// the New method. All methods on Peel are thread-safe.
type Peel struct {
	c *core.Core
}

// New initializes a Peel struct with a Core, using the given redis address and
// pool size. The redis address can be a standalone node or a node in a cluster.
func New(redisAddr string, poolSize int) (Peel, error) {
	c, err := core.New(redisAddr, poolSize)
	return Peel{c}, err
}

// TODO Peel Run command

// Client describes the information attached to any given client of bananaq.
// For most commands this isn't actually necessary, but for some it's actually
// used (these will be documented as such)
// TODO this probably isn't actually necessary
type Client struct {
	ID string
}

// QAddCommand describes the parameters which can be passed into the QAdd
// command
type QAddCommand struct {
	Client
	Queue    string    // Required
	Expire   time.Time // Required
	Contents string    // Required
}

// QAdd adds an event to a queue. The Expire will be used to generate an ID.
// IDs are unique across the cluster, so the ID may correspond to a very
// slightly greater time than the given Expire. That (potentially slightly
// greater) time will also be used as the point in time after which the event is
// no longer valid.
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

	es := queueAvailable(c.Queue)

	qa := core.QueryActions{
		EventSetBase: es.Base,
		QueryActions: []core.QueryAction{
			{
				QuerySelector: &core.QuerySelector{
					EventSet: es,
					Events:   []core.Event{e},
				},
			},
			{
				QueryAddTo: &core.QueryAddTo{
					EventSets: []core.EventSet{es},
				},
			},
		},
	}
	if _, err := p.c.Query(qa); err != nil {
		return 0, err
	}

	p.c.EventSetNotify(es)

	return e.ID, nil
}

// QGetCommand describes the parameters which can be passed into the QGet
// command
type QGetCommand struct {
	Queue         string // Required
	ConsumerGroup string // Required
	AckDeadline   time.Time
	BlockUntil    time.Time
	ConsumerID    string
}

// TODO this API is kind of gross, the lenght of its docstring is evidence of
// that. I'm doing a lot of work and adding a lot of API complexity to support
// this Consumers field in the QStatus. I should consider how necessary that
// really is, and if it's worth it. It's definitely a useful thing, but if I can
// come up with another way to get that info that's similartly convenient,
// that'd be ideal.

// TODO this method is kind of very broken. In a really unfortunate way. If I
// add an event to a queue, qget that event, then add another event with a //
// sooner expiration than the first, I'll never see that second event. I
// literally don't know how to fix this at this point...
//
// So the issue is there's two ways I could go about this:
//
// * Have the id as the expiration. This leads to the above problem. I like
//   having the expiration as the ID, it leads to some problems with the order
//   the events are returned in. But it *feels* clean, which may or may not be
//   worth anything
//
// * Have the id and expiration as two separate numbers. The problem with that
//   plan is that it makes things kind of weird. For some sorted sets I need the
//   events sorted by id, but I also need to clean events out of them by
//   expiration. The biggest problem is with available. It *needs* to be ordered
//   by insertion time, no matter what. But cleaning by expiration is also
//   definitely required too. I think solving this problem (figuring out how to
//   clean available by expiration, but order it by insertion) will solve most
//   of my problems.
//
//		- The naive solution is to just order by id, then iterate over it when
//		  cleaning needs to be done. This obviously super sucks. It could be
//		  made slightly better by only pulling chunks off the start in a loop,
//		  and stopping the loop once I reach a chunk with no expired events.
//		  If there's some cluster of events with some outlandish expirations
//		  this could get thrown for a loop though, also it's gross.
//
//		- Another sorta neive solution is to keep two sorted sets per queue.
//		  This also rather sucks.
//
//		- So what this problem ultimately breaks down into is that I have two
//		  times per event that I care about. The first is the time it was
//		  created. The second is the time it expires. I need to keep track of
//		  events sorted by both. Do I?
//
//		  With the creation time what I ultimately need to do is be able to say
//		  "If this event was created before this other one, it should be handed
//		  out first." This isn't a user-based requirement. It allows me to do
//		  things like say "if this event is 'done' for a particular consumer
//		  group, then all events before it must also have been done (ignoring
//		  redo, which is simply a mechanism to fill in the holes). So the
//		  requirement over creation times is mainly a mechanism for making
//		  lookup of "next" events efficient. Other mechanisms may exist, but I
//		  haven't thought of any.
//
//		  With expire time what I ultimately need is to be able to do is say "If
//		  an event's expire is before the current time, it's not available for
//		  any operation". This also isn't a user-based requirement, I suppose.
//		  It allows me to do things like say "if this event has expired, then
//		  all events before must also have expired, and can be gc'd". So the
//		  requirement over expire times is mainly a mechanism for determining if
//		  an event should be considered valid. There's definitely other ways to
//		  do this, like brute force checking all the time. But none are super
//		  great for the actual cleanup step.
//
//		- (This might be solving the wrong problem) A sub solution here is to
//		  keep a set of events per consumer groups ordered by expiration, and
//		  the rest can be whatever. Whenever any events are pulled off of
//		  available they go in that, Then when cleanup happens I can look at
//		  only that one.
//
// * I come up with a new setup for doing all of this

// While trying to solve the above problem, I realized something. I'm *probably*
// only actually using inProgByID and done to get their "newest" event that has
// been processed. They're effectively being used as pointers within the
// "queue". I might be able to instead set them as keys instead of sorted sets.
// which would be cleaner and save some space. I think I still need inProbByAck
// to be a sorted set, since that needs to be ranged over, and redo definitely
// needs to be a set.

// QGet retrieves an available event from the given queue for the given consumer
// group.
//
// If AckDeadline is given, then the consumer has until then to QAck the
// Event before it is placed back in the queue for this consumer group. If
// AckDeadline is not set, then the Event will never be placed back, and QAck
// isn't necessary.
//
// If no new event is currently available, but BlockUntil is given, this call
// will block until an event becomes available, or until BlockUntil is reached,
// whichever happens first. If BlockUntil is not given this call always returns
// immediately, regardless of available events.
//
// If ConsumerID is given alongside BlockUntil, then it will be used as a unique
// identifier to keep track of this consumer for the purposes of the Consumers
// field in the QStatus return.
//
// An empty event is returned if there are no available events for the queue.
func (p Peel) QGet(c QGetCommand) (core.Event, error) {
	if c.BlockUntil.IsZero() {
		return p.qgetDirect(c)
	}

	now := time.Now()
	timeoutCh := time.After(c.BlockUntil.Sub(now))
	esAvail := queueAvailable(c.Queue)

	for {
		stopCh := make(chan struct{})
		pushCh := p.c.EventSetWait(esAvail, stopCh)

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
	esAvail := queueAvailable(c.Queue)
	esInProgID := queueInProgressByID(c.Queue, c.ConsumerGroup)
	esInProgAck := queueInProgressByAck(c.Queue, c.ConsumerGroup)
	esRedo := queueRedo(c.Queue, c.ConsumerGroup)
	esDone := queueDone(c.Queue, c.ConsumerGroup)

	// Depending on if Expire is set, we might add the event to the inProgs or
	// done
	var inProgOrDone []core.QueryAction
	if !c.AckDeadline.IsZero() {
		inProgOrDone = []core.QueryAction{
			{
				QueryAddTo: &core.QueryAddTo{
					EventSets: []core.EventSet{esInProgID},
				},
			},
			{
				QueryAddTo: &core.QueryAddTo{
					EventSets: []core.EventSet{esInProgAck},
					Score:     core.NewTS(c.AckDeadline),
				},
			},
		}
	} else {
		inProgOrDone = []core.QueryAction{
			{
				QueryAddTo: &core.QueryAddTo{
					EventSets: []core.EventSet{esDone},
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

	mostRecentSelect := core.QueryEventRangeSelect{
		QueryScoreRange: core.QueryScoreRange{
			Min:     core.NewTS(now),
			MinExcl: true,
			Max:     0,
		},
		Limit:   1,
		Reverse: true,
	}
	oldestSelect := mostRecentSelect
	oldestSelect.Reverse = false

	var qq []core.QueryAction
	// First, if there's any Events in redo, we grab the first one from there
	// and move it to inProg/done
	qq = append(qq,
		core.QueryAction{
			QuerySelector: &core.QuerySelector{
				EventSet:              esRedo,
				QueryEventRangeSelect: &oldestSelect,
			},
		},
		core.QueryAction{
			RemoveFrom: []core.EventSet{esRedo},
		},
	)
	qq = append(qq, inProgOrDone...)
	qq = append(qq, breakIfFound)

	// Otherwise, we grab the most recent Events from both inProgByID and done
	qq = append(qq,
		core.QueryAction{
			QuerySelector: &core.QuerySelector{
				EventSet:              esInProgID,
				QueryEventRangeSelect: &mostRecentSelect,
			},
		},
		core.QueryAction{
			QuerySelector: &core.QuerySelector{
				EventSet:              esDone,
				QueryEventRangeSelect: &mostRecentSelect,
				Union: true,
			},
		},
		core.QueryAction{
			QuerySelector: &core.QuerySelector{
				EventSet: esAvail,
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
			EventSet:       esAvail,
			PosRangeSelect: []int64{0, 0},
		},
		QueryConditional: core.QueryConditional{
			And: []core.QueryConditional{
				{
					IfEmpty: &esDone,
				},
				{
					IfEmpty: &esInProgID,
				},
			},
		},
	})
	qq = append(qq, inProgOrDone...)

	qa := core.QueryActions{
		EventSetBase: esAvail.Base,
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
	Client
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

	esInProgID := queueInProgressByID(c.Queue, c.ConsumerGroup)
	esInProgAck := queueInProgressByAck(c.Queue, c.ConsumerGroup)
	esDone := queueDone(c.Queue, c.ConsumerGroup)

	qa := core.QueryActions{
		EventSetBase: esDone.Base,
		QueryActions: []core.QueryAction{
			{
				QuerySelector: &core.QuerySelector{
					EventSet: esInProgAck,
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
				RemoveFrom: []core.EventSet{esInProgID, esInProgAck},
			},
			{
				QueryAddTo: &core.QueryAddTo{
					EventSets: []core.EventSet{esDone},
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

	esInProgID := queueInProgressByID(queue, consumerGroup)
	esInProgAck := queueInProgressByAck(queue, consumerGroup)
	esDone := queueDone(queue, consumerGroup)
	esRedo := queueRedo(queue, consumerGroup)

	qsr := core.QueryScoreRange{
		Max: nowTS,
	}
	qa := core.QueryActions{
		EventSetBase: esDone.Base,
		QueryActions: []core.QueryAction{
			// First find all events who missed their ack deadline, remove them
			// from both inProgs and add them to redo
			{
				QuerySelector: &core.QuerySelector{
					EventSet: esInProgAck,
					QueryEventRangeSelect: &core.QueryEventRangeSelect{
						QueryScoreRange: qsr,
					},
				},
			},
			{
				RemoveFrom: []core.EventSet{esInProgAck, esInProgID},
			},
			{
				QueryAddTo: &core.QueryAddTo{
					EventSets: []core.EventSet{esRedo},
				},
			},

			// Next find all events in inProgID that have outright expired,
			// remove them from inProgAck (since that's the only way to find
			// them there) and inProgID while we're there
			{
				QuerySelector: &core.QuerySelector{
					EventSet: esInProgID,
					QueryEventRangeSelect: &core.QueryEventRangeSelect{
						QueryScoreRange: qsr,
					},
				},
			},
			{
				RemoveFrom: []core.EventSet{esInProgAck, esInProgID},
			},

			// Finally remove all that have outright expired from done and redo
			{
				QueryRemoveByScore: &core.QueryRemoveByScore{
					QueryScoreRange: qsr,
					EventSets: []core.EventSet{
						esDone,
						esRedo,
					},
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

	esAvail := queueAvailable(queue)
	qa := core.QueryActions{
		EventSetBase: esAvail.Base,
		QueryActions: []core.QueryAction{
			{
				QueryRemoveByScore: &core.QueryRemoveByScore{
					QueryScoreRange: core.QueryScoreRange{
						Max: nowTS,
					},
					EventSets: []core.EventSet{
						esAvail,
					},
				},
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
	Client
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
	esAvail := queueAvailable(c.Queue)
	esInProgID := queueInProgressByID(c.Queue, c.ConsumerGroup)
	esRedo := queueRedo(c.Queue, c.ConsumerGroup)
	esDone := queueDone(c.Queue, c.ConsumerGroup)

	var qs QueueStats
	counts, err := p.c.EventSetCounts(esAvail, esInProgID, esRedo, esDone)
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
