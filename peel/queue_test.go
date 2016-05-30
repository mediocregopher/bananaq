package peel

import (
	. "testing"

	"github.com/levenlabs/golib/testutil"
	"github.com/mediocregopher/bananaq/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAllQueuesCGroups(t *T) {
	p := newTestPeel()

	requireAddToKey := func(t *T, k core.Key) {
		qa := core.QueryActions{
			KeyBase: k.Base,
			QueryActions: []core.QueryAction{
				{
					QuerySelector: &core.QuerySelector{
						IDs: []core.ID{randID(t, false)},
					},
				},
				{
					QueryAddTo: &core.QueryAddTo{
						Keys: []core.Key{k},
					},
				},
			},
		}
		_, err := p.c.Query(qa)
		require.Nil(t, err)
	}

	q1 := testutil.RandStr()
	q2 := testutil.RandStr()
	q3 := testutil.RandStr()
	cg1 := testutil.RandStr()
	cg2 := testutil.RandStr()
	cg3 := testutil.RandStr()

	ewAvail1, err := queueAvailable(q1)
	require.Nil(t, err)
	ewAvail2, err := queueAvailable(q2)
	require.Nil(t, err)
	ewAvail3, err := queueAvailable(q3)
	require.Nil(t, err)

	ew11, err := queueInProgress(q1, cg1)
	require.Nil(t, err)
	ew12, err := queueRedo(q1, cg2)
	require.Nil(t, err)
	ew23, err := queueInProgress(q2, cg3)
	require.Nil(t, err)

	requireAddToKey(t, ewAvail1.byArb)
	requireAddToKey(t, ewAvail2.byArb)
	requireAddToKey(t, ewAvail3.byArb)
	requireAddToKey(t, ew11.byArb)
	requireAddToKey(t, ew12.byArb)
	requireAddToKey(t, ew23.byArb)

	m, err := p.AllQueuesConsumerGroups()
	require.Nil(t, err)
	assert.Len(t, m, 3)
	assert.Contains(t, m[q1], cg1)
	assert.Contains(t, m[q1], cg2)
	assert.Contains(t, m[q2], cg3)
	assert.Empty(t, m[q3])
}
