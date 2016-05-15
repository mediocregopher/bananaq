package peel

import (
	. "testing"
	"time"

	"github.com/levenlabs/golib/testutil"
	"github.com/mediocregopher/bananaq/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testPeel Peel

func init() {
	var err error
	if testPeel, err = New("127.0.0.1:6379", 1); err != nil {
		panic(err)
	}
	// TODO when peel has a Run method, use that here
	go func() { panic(testPeel.c.Run()) }()
}

func randClient() Client {
	return Client{ID: testutil.RandStr()}
}

func eventSetElems(t *T, es core.EventSet) []core.Event {
	qa := core.QueryActions{
		EventSetBase: es.Base,
		QueryActions: []core.QueryAction{
			{
				QuerySelector: &core.QuerySelector{
					EventSet:              es,
					QueryEventRangeSelect: &core.QueryEventRangeSelect{},
				},
			},
		},
	}
	ee, err := testPeel.c.Query(qa)
	require.Nil(t, err)
	return ee
}

func assertEventSet(t *T, es core.EventSet, ids ...core.ID) {
	ee := eventSetElems(t, es)
	eeIDs := map[core.ID]bool{}
	for _, e := range ee {
		eeIDs[e.ID] = true
	}
	assert.Len(t, eeIDs, len(ids))
	for _, id := range ids {
		assert.Contains(t, eeIDs, id, "doesn't contain id: %d", id)
	}
}

func TestQAdd(t *T) {
	queue := testutil.RandStr()
	contents := testutil.RandStr()
	id, err := testPeel.QAdd(QAddCommand{
		Client:   randClient(),
		Queue:    queue,
		Expire:   time.Now().Add(10 * time.Second),
		Contents: contents,
	})
	require.Nil(t, err)
	assert.NotZero(t, id)
	assertEventSet(t, queueAvailable(queue), id)

	e, err := testPeel.c.GetEvent(id)
	require.Nil(t, err)
	assert.Equal(t, contents, e.Contents)
}

// score is optional
func requireAddToES(t *T, es core.EventSet, e core.Event, score core.TS) {
	qa := core.QueryActions{
		EventSetBase: es.Base,
		QueryActions: []core.QueryAction{
			{
				QuerySelector: &core.QuerySelector{
					Events: []core.Event{e},
				},
			},
			{
				QueryAddTo: &core.QueryAddTo{
					EventSets: []core.EventSet{es},
					Score:     score,
				},
			},
		},
	}
	_, err := testPeel.c.Query(qa)
	require.Nil(t, err)
}

func newTestQueue(t *T, numEvents int) (string, []core.Event) {
	queue := testutil.RandStr()
	var ee []core.Event
	for i := 0; i < numEvents; i++ {
		id, err := testPeel.QAdd(QAddCommand{
			Client:   randClient(),
			Queue:    queue,
			Expire:   time.Now().Add(10 * time.Minute),
			Contents: testutil.RandStr(),
		})
		require.Nil(t, err)
		e, err := testPeel.c.GetEvent(id)
		require.Nil(t, err)
		ee = append(ee, e)
	}
	return queue, ee
}

func randEmptyEvent(t *T, expired bool) core.Event {
	var id core.ID
	if expired {
		id = core.ID(core.NewTS(time.Now().Add(-10 * time.Second)))
	} else {
		var err error
		id, err = testPeel.c.NewID(core.NewTS(time.Now().Add(10 * time.Second)))
		require.Nil(t, err)
	}
	return core.Event{ID: id}
}

func TestQGet(t *T) {
	queue, ee := newTestQueue(t, 6)
	cgroup := testutil.RandStr()

	for i, e := range ee {
		t.Logf("ee[%d]: %d (0x%x)", i, e.ID, e.ID)
	}

	esInProgID := queueInProgressByID(queue, cgroup)
	esInProgAck := queueInProgressByAck(queue, cgroup)
	esRedo := queueRedo(queue, cgroup)
	esDone := queueDone(queue, cgroup)

	// Test that a "blank" queue gives us its first event
	cmd := QGetCommand{
		Queue:         queue,
		ConsumerGroup: cgroup,
		AckDeadline:   time.Now().Add(1 * time.Second),
	}
	e, err := testPeel.QGet(cmd)
	require.Nil(t, err)
	assert.Equal(t, ee[0], e)
	assertEventSet(t, esInProgID, ee[0].ID)
	assertEventSet(t, esInProgAck, ee[0].ID)

	// Test that a queue with empty done but an inProg returns one after inProg
	e, err = testPeel.QGet(cmd)
	require.Nil(t, err)
	assert.Equal(t, ee[1], e)
	assertEventSet(t, esInProgID, ee[0].ID, ee[1].ID)
	assertEventSet(t, esInProgAck, ee[0].ID, ee[1].ID)

	// Test that empty expire goes straight to done
	cmd.AckDeadline = time.Time{}
	e, err = testPeel.QGet(cmd)
	require.Nil(t, err)
	assert.Equal(t, ee[2], e)
	assertEventSet(t, esInProgID, ee[0].ID, ee[1].ID)
	assertEventSet(t, esInProgAck, ee[0].ID, ee[1].ID)
	assertEventSet(t, esDone, ee[2].ID)

	// Test that a queue with an event in done ahead of all events in inProg
	// returns the next one correctly
	e, err = testPeel.QGet(cmd)
	require.Nil(t, err)
	assert.Equal(t, ee[3], e)
	assertEventSet(t, esInProgID, ee[0].ID, ee[1].ID)
	assertEventSet(t, esInProgAck, ee[0].ID, ee[1].ID)
	assertEventSet(t, esDone, ee[2].ID, ee[3].ID)

	// Artifically add two events to redo, make sure they come out in that order
	// immediately
	requireAddToES(t, esRedo, ee[4], 0)
	requireAddToES(t, esRedo, ee[5], 0)
	assertEventSet(t, esRedo, ee[4].ID, ee[5].ID)

	e, err = testPeel.QGet(cmd)
	require.Nil(t, err)
	assert.Equal(t, ee[4], e)
	assertEventSet(t, esInProgID, ee[0].ID, ee[1].ID)
	assertEventSet(t, esInProgAck, ee[0].ID, ee[1].ID)
	assertEventSet(t, esDone, ee[2].ID, ee[3].ID, ee[4].ID)
	assertEventSet(t, esRedo, ee[5].ID)

	e, err = testPeel.QGet(cmd)
	require.Nil(t, err)
	assert.Equal(t, ee[5], e)
	assertEventSet(t, esInProgID, ee[0].ID, ee[1].ID)
	assertEventSet(t, esInProgAck, ee[0].ID, ee[1].ID)
	assertEventSet(t, esDone, ee[2].ID, ee[3].ID, ee[4].ID, ee[5].ID)
	assertEventSet(t, esRedo) // assert empty

	// At this point the queue has no available events, make sure empty event is
	// returned
	e, err = testPeel.QGet(cmd)
	require.Nil(t, err)
	assert.Equal(t, core.Event{}, e)
	assertEventSet(t, esInProgID, ee[0].ID, ee[1].ID)
	assertEventSet(t, esInProgAck, ee[0].ID, ee[1].ID)
	assertEventSet(t, esDone, ee[2].ID, ee[3].ID, ee[4].ID, ee[5].ID)
	assertEventSet(t, esRedo) // assert empty
}

func TestQGetBlocking(t *T) {
	queue, ee := newTestQueue(t, 1)
	cgroup := testutil.RandStr()

	cmd := QGetCommand{
		Queue:         queue,
		ConsumerGroup: cgroup,
		BlockUntil:    time.Now().Add(1 * time.Second),
	}

	assertBlockFor := func(d time.Duration) core.Event {
		ch := make(chan core.Event)
		go func() {
			e, err := testPeel.QGet(cmd)
			require.Nil(t, err)
			ch <- e
		}()

		if d > 0 {
			select {
			case <-time.After(d):
			case <-ch:
				assert.Fail(t, "didn't block long enough")
			}
		}
		select {
		case <-time.After(100 * time.Millisecond):
			assert.Fail(t, "blocked too long")
		case e := <-ch:
			return e
		}
		return core.Event{}
	}

	e := assertBlockFor(0)
	assert.Equal(t, ee[0], e)

	cmd.BlockUntil = time.Now().Add(1 * time.Second)
	e = assertBlockFor(1 * time.Second)
	assert.Equal(t, core.Event{}, e)

	var e2 core.Event
	cmd.BlockUntil = time.Now().Add(1 * time.Second)
	go func() {
		time.Sleep(500 * time.Millisecond)
		contents := testutil.RandStr()
		id, err := testPeel.QAdd(QAddCommand{
			Queue:    queue,
			Expire:   time.Now().Add(10 * time.Minute),
			Contents: contents,
		})
		require.Nil(t, err)
		e2 = core.Event{ID: id, Contents: contents}
	}()
	e = assertBlockFor(500 * time.Millisecond)
	assert.Equal(t, e2, e)
}

func TestQAck(t *T) {
	queue, ee := newTestQueue(t, 2)
	cgroup := testutil.RandStr()
	esInProgID := queueInProgressByID(queue, cgroup)
	esInProgAck := queueInProgressByAck(queue, cgroup)
	esDone := queueDone(queue, cgroup)

	now := time.Now()
	requireAddToES(t, esInProgID, ee[0], 0)
	requireAddToES(t, esInProgAck, ee[0], core.NewTS(now.Add(10*time.Millisecond)))

	cmd := QAckCommand{
		Client:        randClient(),
		Queue:         queue,
		ConsumerGroup: cgroup,
		Event:         ee[0],
	}
	acked, err := testPeel.QAck(cmd)
	require.Nil(t, err)
	assert.True(t, acked)
	assertEventSet(t, esInProgID)
	assertEventSet(t, esInProgAck)
	assertEventSet(t, esDone, ee[0].ID)

	acked, err = testPeel.QAck(cmd)
	require.Nil(t, err)
	assert.False(t, acked)
	assertEventSet(t, esInProgID)
	assertEventSet(t, esInProgAck)
	assertEventSet(t, esDone, ee[0].ID)

	requireAddToES(t, esInProgID, ee[1], 0)
	requireAddToES(t, esInProgAck, ee[1], core.NewTS(now.Add(-10*time.Millisecond)))

	cmd.Event = ee[1]
	acked, err = testPeel.QAck(cmd)
	require.Nil(t, err)
	assert.False(t, acked)
	assertEventSet(t, esInProgID, ee[1].ID)
	assertEventSet(t, esInProgAck, ee[1].ID)
	assertEventSet(t, esDone, ee[0].ID)
}

func TestClean(t *T) {
	queue := testutil.RandStr()
	cgroup := testutil.RandStr()
	esInProgID := queueInProgressByID(queue, cgroup)
	esInProgAck := queueInProgressByAck(queue, cgroup)
	esDone := queueDone(queue, cgroup)
	esRedo := queueRedo(queue, cgroup)
	now := time.Now()

	// in progress, has neither expired nor missed its deadline
	ee0 := randEmptyEvent(t, false)
	requireAddToES(t, esInProgID, ee0, 0)
	requireAddToES(t, esInProgAck, ee0, core.NewTS(now.Add(10*time.Millisecond)))

	// in progress, missed its deadline
	ee1 := randEmptyEvent(t, false)
	requireAddToES(t, esInProgID, ee1, 0)
	requireAddToES(t, esInProgAck, ee1, core.NewTS(now.Add(-10*time.Millisecond)))

	// in progress, expired
	ee2 := randEmptyEvent(t, true)
	requireAddToES(t, esInProgID, ee2, 0)
	requireAddToES(t, esInProgAck, ee2, core.NewTS(now.Add(-10*time.Millisecond)))

	// in redo, not expired
	ee3 := randEmptyEvent(t, false)
	requireAddToES(t, esRedo, ee3, 0)

	// in redo, expired
	ee4 := randEmptyEvent(t, true)
	requireAddToES(t, esRedo, ee4, 0)

	// in done, not expired
	ee5 := randEmptyEvent(t, false)
	requireAddToES(t, esDone, ee5, 0)

	// in done, expired
	ee6 := randEmptyEvent(t, true)
	requireAddToES(t, esDone, ee6, 0)

	require.Nil(t, testPeel.Clean(queue, cgroup))
	assertEventSet(t, esInProgID, ee0.ID)
	assertEventSet(t, esInProgAck, ee0.ID)
	assertEventSet(t, esDone, ee5.ID)
	assertEventSet(t, esRedo, ee1.ID, ee3.ID)
}

func TestCleanAvailable(t *T) {
	queue := testutil.RandStr()
	esAvail := queueAvailable(queue)

	ee0 := randEmptyEvent(t, false)
	requireAddToES(t, esAvail, ee0, 0)
	ee1 := randEmptyEvent(t, true)
	requireAddToES(t, esAvail, ee1, 0)
	ee2 := randEmptyEvent(t, false)
	requireAddToES(t, esAvail, ee2, 0)
	ee3 := randEmptyEvent(t, true)
	requireAddToES(t, esAvail, ee3, 0)

	require.Nil(t, testPeel.CleanAvailable(queue))
	assertEventSet(t, esAvail, ee0.ID, ee2.ID)
}

func TestQStatus(t *T) {
	queue, ee := newTestQueue(t, 6)
	cgroup := testutil.RandStr()
	esInProgID := queueInProgressByID(queue, cgroup)
	esDone := queueDone(queue, cgroup)
	esRedo := queueRedo(queue, cgroup)

	requireAddToES(t, esInProgID, ee[0], 0)
	requireAddToES(t, esDone, ee[1], 0)
	requireAddToES(t, esDone, ee[2], 0)
	requireAddToES(t, esRedo, ee[3], 0)

	qs, err := testPeel.QStatus(QStatusCommand{
		Queue:         queue,
		ConsumerGroup: cgroup,
	})
	require.Nil(t, err)

	expected := QueueStats{
		Total:      6,
		InProgress: 1,
		Done:       2,
		Redo:       1,
		Available:  2,
	}

	assert.Equal(t, expected, qs)
}
