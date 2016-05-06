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
		assert.Contains(t, eeIDs, id)
	}
}

func TestQAdd(t *T) {
	queue := testutil.RandStr()
	contents := testutil.RandStr()
	id, err := testPeel.QAdd(QAddCommand{
		Client:   randClient(),
		Queue:    queue,
		Expire:   10 * time.Second,
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
			Expire:   10 * time.Second,
			Contents: testutil.RandStr(),
		})
		require.Nil(t, err)
		e, err := testPeel.c.GetEvent(id)
		require.Nil(t, err)
		ee = append(ee, e)
	}
	return queue, ee
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
		Client:        randClient(),
		Queue:         queue,
		ConsumerGroup: cgroup,
		Expire:        1 * time.Second,
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
	cmd.Expire = 0
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
