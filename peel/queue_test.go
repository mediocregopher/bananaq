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

	keyAvailID1, err := queueAvailableByID(q1)
	require.Nil(t, err)
	keyAvailID2, err := queueAvailableByID(q2)
	require.Nil(t, err)
	keyAvailID3, err := queueAvailableByID(q3)
	require.Nil(t, err)

	keyInUse11, err := queueInUseByExpire(q1, cg1)
	require.Nil(t, err)
	keyInUse12, err := queueInUseByExpire(q1, cg2)
	require.Nil(t, err)
	keyInUse23, err := queueInUseByExpire(q2, cg3)
	require.Nil(t, err)

	requireAddToKey(t, keyAvailID1)
	requireAddToKey(t, keyAvailID2)
	requireAddToKey(t, keyAvailID3)
	requireAddToKey(t, keyInUse11)
	requireAddToKey(t, keyInUse12)
	requireAddToKey(t, keyInUse23)

	m, err := p.AllQueuesConsumerGroups()
	require.Nil(t, err)
	assert.Len(t, m, 3)
	assert.Contains(t, m[q1], cg1)
	assert.Contains(t, m[q1], cg2)
	assert.Contains(t, m[q2], cg3)
	assert.Empty(t, m[q3])
}
