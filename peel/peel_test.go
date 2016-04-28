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
