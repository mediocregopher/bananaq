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
						Events: []core.Event{randEmptyEvent(t, false)},
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

	requireAddToKey(t, queueAvailableByID(q1))
	requireAddToKey(t, queueAvailableByID(q2))
	requireAddToKey(t, queueAvailableByID(q3))
	requireAddToKey(t, queueInUseByExpire(q1, cg1))
	requireAddToKey(t, queueInUseByExpire(q1, cg2))
	requireAddToKey(t, queueInUseByExpire(q2, cg3))

	m, err := p.AllQueuesConsumerGroups()
	require.Nil(t, err)
	assert.Len(t, m, 3)
	assert.Contains(t, m[q1], cg1)
	assert.Contains(t, m[q1], cg2)
	assert.Contains(t, m[q2], cg3)
	assert.Empty(t, m[q3])
}
