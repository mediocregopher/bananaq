// Package peel is a client for bananaq which connects directly to the backing
// redis instance(s) instead of using the normal server. This can be used by any
// number of clients along-side any number of server instances. None of them
// need to coordinate with each other.
//
// Initialization
//
// A new peel takes in either a *pool.Pool or a *cluster.Cluster from the
// radix.v2 package, and can be initialized like so:
//
// 	rpool, err := pool.New("tcp", "127.0.0.1:6379", 10)
//	if err != nil {
//		panic(err)
//	}
//
//	p, err := peel.New(rpool, nil)
//	if err != nil {
//		panic(err)
//	}
//
// Running
//
// All peels require that you call the Run method in them in order for them to
// work properly. Run will block until an error is reached. Additionally, you
// will need to periodically call Clean on the queues and consumer groups you
// care about, or CleanAll for a less precise approach. It's not strictly
// necessary to call Clean, but if you don't do it you'll be wasting memory in
// redis.
//
//	go func() { panic(p.Run() }
//	go func() {
//		for range <-time.Tick(20 * time.Second) {
//			if err := p.CleanAll(); err != nil {
//				panic(err)
//			}
//		}
//	}
//
// After that
//
// Once initialization is done, and you're successfully running Peel, you can
// call any of its methods with any arguments. All command methods are
// completely thread-safe
//
//	_, err := p.QAdd(peel.QAddCommand{
//		Queue: "foo",
//		Expire: time.Now().Add(10 * time.Minute),
//		Contents: "some stuff",
//	})
//
package peel

import (
	"fmt"
	"strconv"
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

// QAdd adds an event to a queue. Once Expire is reached the event will no
// longer be considered valid in the queue, and will eventually be cleaned up.
func (p Peel) QAdd(c QAddCommand) (core.ID, error) {
	now := core.NewTS(time.Now())
	e, err := p.c.NewEvent(now, core.NewTS(c.Expire), c.Contents)
	if err != nil {
		return core.ID{}, err
	}

	// We always store the event data itself with an extra 30 seconds until it
	// expires, just in case a consumer gets it just as its expire time hits
	if err = p.c.SetEvent(e, 30*time.Second); err != nil {
		return core.ID{}, err
	}

	ewAvail, err := queueAvailable(c.Queue)
	if err != nil {
		return core.ID{}, err
	}

	qa := core.QueryActions{
		KeyBase:      ewAvail.base,
		QueryActions: ewAvail.add(e.ID, e.ID.T),
		Now:          now,
	}
	if _, err := p.c.Query(qa); err != nil {
		return core.ID{}, err
	}

	p.c.KeyNotify(ewAvail.byArb)

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

	ewAvail, err := queueAvailable(c.Queue)
	if err != nil {
		return core.Event{}, err
	}

	now := time.Now()
	timeoutCh := time.After(c.BlockUntil.Sub(now))

	for {
		stopCh := make(chan struct{})
		pushCh := p.c.KeyWait(ewAvail.byArb, stopCh)

		if e, err := p.qgetDirect(c); err != nil || (e != core.Event{}) {
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
	ewAvail, err := queueAvailable(c.Queue)
	if err != nil {
		return core.Event{}, err
	}

	ewInProg, ewRedo, keyPtr, err := queueCGroupKeys(c.Queue, c.ConsumerGroup)
	if err != nil {
		return core.Event{}, err
	}

	now := core.NewTS(time.Now())

	// Depending on if Expire is set, we might add the event to the inProg in
	// addition to setting ptr
	maybeDone := make([]core.QueryAction, 0, 3)
	maybeDone = append(maybeDone, core.QueryAction{
		QuerySingleSet: &core.QuerySingleSet{
			Key:     keyPtr,
			IfNewer: true,
		},
	})
	if !c.AckDeadline.IsZero() {
		addToInProg := ewInProg.addFromInput(core.NewTS(c.AckDeadline))
		maybeDone = append(maybeDone, addToInProg...)
	}
	maybeDone = append(maybeDone, core.QueryAction{
		Break: true,
		QueryConditional: core.QueryConditional{
			IfInput: true,
		},
	})

	var qq []core.QueryAction

	// First, if there's any IDs in redo, we try to grab the first one from
	// there
	qq = append(qq, ewRedo.clean(now)...)
	qq = append(qq, ewRedo.after(0, 1))
	qq = append(qq, ewRedo.removeFromInput())
	qq = append(qq, maybeDone...)

	// Otherwise grab the next event from avail after our pointer. Gotta clean
	// avail first though. If we get an event, set our pointer and return
	qq = append(qq, ewAvail.clean(now)...)
	qq = append(qq,
		core.QueryAction{
			SingleGet: &keyPtr,
		},
		ewAvail.afterInput(1),
	)
	qq = append(qq, maybeDone...)

	// The queue has no activity, simply get the first event in avail. Only
	// applies if our pointer is actually empty. If it's not and we're here it
	// means that the queue has simply been fully processed thusfar
	qq = append(qq, core.QueryAction{
		Break: true,
		QueryConditional: core.QueryConditional{
			IfNotEmpty: &keyPtr,
		},
	})
	qq = append(qq, ewAvail.after(0, 1))
	qq = append(qq, maybeDone...)

	qa := core.QueryActions{
		KeyBase:      ewAvail.base,
		QueryActions: qq,
		Now:          now,
	}

	res, err := p.c.Query(qa)
	if err != nil {
		return core.Event{}, err
	} else if len(res.IDs) == 0 {
		return core.Event{}, nil
	}

	return p.c.GetEvent(res.IDs[0])
}

// QAckCommand describes the parameters which can be passed into the QAck
// command
type QAckCommand struct {
	Queue         string  // Required
	ConsumerGroup string  // Required
	EventID       core.ID // Required
}

// QAck acknowledges that an event has been successfully processed and should
// not be re-processed. Only applicable for Events which were gotten through a
// QGet with an AckDeadline. Returns true if the Event was successfully
// acknowledged. false will be returned if the deadline was missed, and
// therefore some other consumer may re-process the Event later.
func (p Peel) QAck(c QAckCommand) (bool, error) {
	now := core.NewTS(time.Now())

	ewInProg, err := queueInProgress(c.Queue, c.ConsumerGroup)
	if err != nil {
		return false, err
	}

	var qq []core.QueryAction
	qq = append(qq, ewInProg.clean(now)...)
	qq = append(qq, core.QueryAction{
		QuerySelector: &core.QuerySelector{
			Key: ewInProg.byArb,
			QueryIDScoreSelect: &core.QueryIDScoreSelect{
				ID:  c.EventID,
				Min: now,
			},
		},
	})
	qq = append(qq, ewInProg.removeFromInput())

	qa := core.QueryActions{
		KeyBase:      ewInProg.base,
		QueryActions: qq,
		Now:          now,
	}

	res, err := p.c.Query(qa)
	if err != nil {
		return false, err
	}
	return len(res.IDs) > 0, nil
}

// TODO maybe peel should just do the cleaning automatically?

// Clean finds all the events which were retrieved for the given
// queue/consumerGroup which weren't ack'd by the deadline, and makes them
// available to be retrieved again. This should be called periodically for all
// queue/consumerGroups.
func (p Peel) Clean(queue, consumerGroup string) error {
	now := core.NewTS(time.Now())

	ewInProg, ewRedo, keyPtr, err := queueCGroupKeys(queue, consumerGroup)
	if err != nil {
		return err
	}

	// First clean expired events from everything
	var qq []core.QueryAction
	qq = append(qq, ewInProg.clean(now)...)
	qq = append(qq, ewRedo.clean(now)...)

	// find all events who missed their ack deadline, remove them
	// from both inProgs and add them to redo
	qq = append(qq, ewInProg.before(now, 0))
	qq = append(qq, ewInProg.removeFromInput())
	qq = append(qq, ewRedo.addFromInput(0)...)

	qa := core.QueryActions{
		KeyBase:      keyPtr.Base,
		QueryActions: qq,
		Now:          now,
	}

	_, err = p.c.Query(qa)
	return err
}

// CleanAvailable cleans up the stored Events for a given queue across all
// consumer groups, removing those which have expired.
func (p Peel) CleanAvailable(queue string) error {
	now := core.NewTS(time.Now())

	ewAvail, err := queueAvailable(queue)
	if err != nil {
		return err
	}

	qa := core.QueryActions{
		KeyBase:      ewAvail.base,
		QueryActions: ewAvail.clean(now),
		Now:          now,
	}

	_, err = p.c.Query(qa)
	return err
}

// CleanAll will call CleanAvailable on all known queues and Clean on all of
// their known consumer groups. Will stop execution at the first error
func (p Peel) CleanAll() error {
	qcg, err := p.AllQueuesConsumerGroups()
	if err != nil {
		return err
	}

	for q, cgs := range qcg {
		if err = p.CleanAvailable(q); err != nil {
			return err
		}
		for _, cg := range cgs {
			if err = p.Clean(q, cg); err != nil {
				return err
			}
		}
	}
	return err
}

// ConsumerGroupStats are available statistics about a queue/consumer group.
type ConsumerGroupStats struct {
	// Number of events the consumer group has yet to process for the queue
	Available uint64

	// Number of events currently being worked on by the consumer group
	InProgress uint64

	// Number of events awaiting being re-attempted by the consumer group
	Redo uint64
}

// QueueStats are available statistics about a queue across all consumer groups
type QueueStats struct {
	// Number of events for the queue in the system. Will be the same regardless
	// of consumer group. Does NOT include expired events.
	Total uint64

	// Statistics for each consumer group known for the queue. The key will be
	// the consumer group's name
	ConsumerGroupStats map[string]ConsumerGroupStats
}

func (p Peel) qstatus(queue string, cgroups []string) (QueueStats, error) {
	now := core.NewTS(time.Now())
	ewAvail, err := queueAvailable(queue)
	if err != nil {
		return QueueStats{}, err
	}

	var qq []core.QueryAction
	qq = append(qq, ewAvail.clean(now)...)
	qq = append(qq, ewAvail.countNotExpired(now))

	for _, cg := range cgroups {
		var ewInProg, ewRedo exWrap
		var keyPtr core.Key
		if ewInProg, ewRedo, keyPtr, err = queueCGroupKeys(queue, cg); err != nil {
			return QueueStats{}, err
		}
		qq = append(qq,
			// reset the input
			core.QueryAction{
				SingleGet: &keyPtr,
			},
			ewAvail.countAfterInput(),
			ewInProg.countNotExpired(now),
			ewRedo.countNotExpired(now),
		)
	}

	qa := core.QueryActions{
		KeyBase:      ewAvail.base,
		QueryActions: qq,
		Now:          now,
	}

	res, err := p.c.Query(qa)
	if err != nil {
		return QueueStats{}, err
	}

	qs := QueueStats{
		Total:              res.Counts[0],
		ConsumerGroupStats: map[string]ConsumerGroupStats{},
	}
	res.Counts = res.Counts[1:]

	for _, cg := range cgroups {
		qs.ConsumerGroupStats[cg] = ConsumerGroupStats{
			Available:  res.Counts[0],
			InProgress: res.Counts[1],
			Redo:       res.Counts[2],
		}
		res.Counts = res.Counts[3:]
	}
	return qs, nil
}

// QStatusCommand describes the parameters which can be passed into the QStatus
// command
type QStatusCommand struct {
	QueuesConsumerGroups map[string][]string
}

// QStatus returns information about the all queues and their consumer groups.
// QueuesConsumerGroups may be set to specify specific queue/consumer group
// combinations to retrieve, otherwise all known queues/consumer groups will be
// retrieved.
func (p Peel) QStatus(c QStatusCommand) (map[string]QueueStats, error) {
	var qcg map[string][]string
	var err error
	if len(c.QueuesConsumerGroups) > 0 {
		qcg = c.QueuesConsumerGroups
	} else {
		if qcg, err = p.AllQueuesConsumerGroups(); err != nil {
			return nil, err
		}
	}

	ret := map[string]QueueStats{}
	for q, cgs := range qcg {
		qs, err := p.qstatus(q, cgs)
		if err != nil {
			return nil, err
		}
		ret[q] = qs
	}
	return ret, nil
}

// Helper method for QInfo. Given an integer and a string or another integer,
// returns the max of the given int and the length of the given string or the
// string form of the given integer
//
//	maxLength(2, "foo", 0) // 3
//	maxLength(2, "", 400) // 3
//	maxLength(2, "", 4) // 2
//
func maxLength(oldMax int, elStr string, elInt uint64) int {
	if elStrL := len(elStr); elStrL > oldMax {
		return elStrL
	}
	if elIntL := len(strconv.FormatUint(elInt, 10)); elIntL > oldMax {
		return elIntL
	}
	return oldMax
}

func cgStatsInfos(cgsm map[string]ConsumerGroupStats) []string {
	var cgL, availL, inProgL, redoL int

	for cg, cgs := range cgsm {
		cgL = maxLength(cgL, cg, 0)
		availL = maxLength(availL, "", cgs.Available)
		inProgL = maxLength(inProgL, "", cgs.InProgress)
		redoL = maxLength(redoL, "", cgs.Redo)
	}

	fmtStr := fmt.Sprintf(
		"consumerGroup:%%-%dq avail:%%-%dd inProg:%%-%dd redo:%%-%dd",
		cgL,
		availL,
		inProgL,
		redoL,
	)

	var r []string
	for cg, cgs := range cgsm {
		r = append(r, fmt.Sprintf(fmtStr, cg, cgs.Available, cgs.InProgress, cgs.Redo))
	}
	return r
}

// QInfo returns a human readable version of the information from QStatus. It
// uses the same arguments.
func (p Peel) QInfo(c QStatusCommand) ([]string, error) {
	m, err := p.QStatus(c)
	if err != nil {
		return nil, err
	}

	var r []string
	for q, qs := range m {
		r = append(r, fmt.Sprintf("queue:%q total:%d", q, qs.Total))
		r = append(r, cgStatsInfos(qs.ConsumerGroupStats)...)
	}
	return r, nil
}
