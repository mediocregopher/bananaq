package core

import (
	. "testing"
	"time"

	"github.com/levenlabs/golib/testutil"
	"github.com/mediocregopher/radix.v2/pool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testCore *Core

func init() {
	p, err := pool.New("tcp", "127.0.0.1:6379", 1)
	if err != nil {
		panic(err)
	}

	o := &Opts{RedisPrefix: testutil.RandStr()}
	if testCore, err = New(p, o); err != nil {
		panic(err)
	}
	go func() { panic(testCore.Run()) }()
}

func TestMonoTS(t *T) {
	for i := 0; i < 100; i++ {
		now := time.Now()
		nowTS := NewTS(now)
		mTS, err := testCore.MonoTS(nowTS)
		assert.Nil(t, err)
		assert.Equal(t, nowTS, mTS)

		// make sure at least the second precision matches, the actual times
		// won't match exactly because we truncate to microseconds
		assert.Equal(t, now.Unix(), mTS.Time().Unix())

		mTS2, err := testCore.MonoTS(nowTS)
		assert.Nil(t, err)
		assert.True(t, mTS2 > mTS, "mTS2:%d !> mTS:%d ", mTS2, mTS)

		nowTS = NewTS(time.Now())
		mTS3, err := testCore.MonoTS(nowTS)
		assert.Nil(t, err)
		assert.True(t, mTS3 > mTS2, "mTS3:%d !> mTS2:%d ", mTS3, mTS2)
	}
}

func requireNewID(t *T) ID {
	ts, err := testCore.MonoTS(NewTS(time.Now()))
	require.Nil(t, err)
	return ID{
		T:      ts,
		Expire: NewTS(ts.Time().Add(1 * time.Minute)),
	}
}

func randKey(base string) Key {
	return Key{
		Base: base,
		Subs: []string{testutil.RandStr()},
	}
}

func populatedKey(t *T, base string, ii ...ID) Key {
	k := randKey(base)
	_, err := testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{IDs: ii},
			},
			{
				QueryAddTo: &QueryAddTo{
					Keys: []Key{k},
				},
			},
		},
	})
	require.Nil(t, err, "%s", err)
	return k
}

func randPopulatedKey(t *T, base string, size int) (Key, []ID) {
	ii := make([]ID, size)
	for i := range ii {
		ii[i] = requireNewID(t)
	}
	k := populatedKey(t, base, ii...)
	return k, ii
}

func assertKey(t *T, k Key, ii ...ID) {
	res, err := testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: k,
					QueryRangeSelect: &QueryRangeSelect{
						QueryScoreRange: QueryScoreRange{
							Min: 0,
							Max: 0,
						},
					},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, ii, res.IDs)
}

// Assert the contents of a set as well as its scores
func assertKeyRaw(t *T, k Key, ixm map[ID]int64) {
	arr, err := testCore.Cmd("ZRANGE", k.String(testCore.o.RedisPrefix), 0, -1, "WITHSCORES").Array()
	require.Nil(t, err)

	m := map[ID]int64{}
	for i := 0; i < len(arr); i += 2 {
		ib, err := arr[i].Bytes()
		require.Nil(t, err)
		score, err := arr[i+1].Int64()
		require.Nil(t, err)

		var id ID
		_, err = id.UnmarshalMsg(ib)
		require.Nil(t, err)

		m[id] = score
	}
	assert.Equal(t, ixm, m)
}

func TestGetSetEvent(t *T) {
	contents := testutil.RandStr()
	now := time.Now()
	expire := time.Now().Add(500 * time.Millisecond)

	e, err := testCore.NewEvent(NewTS(now), NewTS(expire), contents)
	require.Nil(t, err)

	assert.Nil(t, testCore.SetEvent(e, 500*time.Millisecond))
	e2, err := testCore.GetEvent(e.ID)
	assert.Nil(t, err)
	assert.Equal(t, e, e2)

	time.Sleep(1*time.Second + 100*time.Millisecond)
	_, err = testCore.GetEvent(e.ID)
	assert.Equal(t, ErrNotFound, err)
}

func TestKeyString(t *T) {
	kk := []Key{
		{Base: testutil.RandStr(), Subs: nil},
		{Base: testutil.RandStr(), Subs: []string{testutil.RandStr()}},
		{Base: testutil.RandStr(), Subs: []string{testutil.RandStr(), testutil.RandStr()}},
	}

	for _, k := range kk {
		str := k.String(testCore.o.RedisPrefix)
		assert.Equal(t, k, KeyFromString(str), "key:%q", str)
	}
}

func TestQueryBasicAddRemove(t *T) {
	base := testutil.RandStr()
	k, ii := randPopulatedKey(t, base, 3)
	assertKey(t, k, ii...)

	_, err := testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: k,
					IDs: []ID{
						ii[1],
					},
				},
			},
			{
				RemoveFrom: []Key{k},
			},
		},
	})
	require.Nil(t, err)
	assertKey(t, k, ii[0], ii[2])
}

func TestQueryAddScores(t *T) {
	base := testutil.RandStr()
	k1 := randKey(base)
	k2 := randKey(base)
	k3 := randKey(base)
	ii := []ID{
		requireNewID(t),
		requireNewID(t),
		requireNewID(t),
		requireNewID(t),
	}
	ii[1].Expire = NewTS(time.Now().Add(-1 * time.Second))
	ii[3].Expire = NewTS(time.Now().Add(-1 * time.Second))

	res, err := testCore.Query(QueryActions{
		KeyBase: base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{IDs: ii},
			},
			{
				QueryAddTo: &QueryAddTo{
					Keys: []Key{k1},
				},
			},
			{
				QueryAddTo: &QueryAddTo{
					Keys:          []Key{k2},
					ExpireAsScore: true,
				},
			},
			{
				QueryAddTo: &QueryAddTo{
					Keys:  []Key{k3},
					Score: 5,
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, ii, res.IDs)
	assertKeyRaw(t, k1, map[ID]int64{
		ii[0]: int64(ii[0].T),
		ii[1]: int64(ii[1].T),
		ii[2]: int64(ii[2].T),
		ii[3]: int64(ii[3].T),
	})
	assertKeyRaw(t, k2, map[ID]int64{
		ii[0]: int64(ii[0].Expire),
		ii[1]: int64(ii[1].Expire),
		ii[2]: int64(ii[2].Expire),
		ii[3]: int64(ii[3].Expire),
	})
	assertKeyRaw(t, k3, map[ID]int64{
		ii[0]: 5,
		ii[1]: 5,
		ii[2]: 5,
		ii[3]: 5,
	})
}

func TestQueryRemoveByScore(t *T) {
	// This test is not very strict, most of the funtionality here comes form
	// QueryScoreRange, which is tested in TestQueryRangeSelect extensively
	base := testutil.RandStr()
	k, ii := randPopulatedKey(t, base, 4)
	res, err := testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QueryRemoveByScore: &QueryRemoveByScore{
					QueryScoreRange: QueryScoreRange{
						Max:     ii[2].T,
						MaxExcl: true,
					},
					Keys: []Key{k},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Empty(t, res.IDs)
	assertKey(t, k, ii[2], ii[3])
}

func TestQueryRangeSelect(t *T) {
	base := testutil.RandStr()
	k, ii := randPopulatedKey(t, base, 4)

	res, err := testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: k,
					QueryRangeSelect: &QueryRangeSelect{
						QueryScoreRange: QueryScoreRange{
							Min: ii[1].T,
							Max: ii[2].T,
						},
					},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, []ID{ii[1], ii[2]}, res.IDs)

	res, err = testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: k,
					QueryRangeSelect: &QueryRangeSelect{
						QueryScoreRange: QueryScoreRange{
							Min:     ii[0].T,
							MinExcl: true,
							Max:     ii[3].T,
							MaxExcl: true,
						},
					},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, []ID{ii[1], ii[2]}, res.IDs)

	res, err = testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: k,
					QueryRangeSelect: &QueryRangeSelect{
						QueryScoreRange: QueryScoreRange{
							Min: ii[0].T,
							Max: ii[3].T,
						},
						Offset: 1,
						Limit:  2,
					},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, []ID{ii[1], ii[2]}, res.IDs)

	res, err = testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: k,
					QueryRangeSelect: &QueryRangeSelect{
						QueryScoreRange: QueryScoreRange{
							Min: TS(ii[1].T),
							Max: TS(ii[2].T),
						},
						Reverse: true,
						Limit:   1,
					},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, []ID{ii[2]}, res.IDs)

	minK := populatedKey(t, base, ii[0], ii[1])
	maxK := populatedKey(t, base, ii[2], ii[3])
	res, err = testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key:              minK,
					QueryRangeSelect: &QueryRangeSelect{},
				},
			},
			{
				QuerySelector: &QuerySelector{
					Key: k,
					QueryRangeSelect: &QueryRangeSelect{
						QueryScoreRange: QueryScoreRange{
							MinFromInput: true,
						},
					},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, []ID{ii[1], ii[2], ii[3]}, res.IDs)

	res, err = testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key:              maxK,
					QueryRangeSelect: &QueryRangeSelect{},
				},
			},
			{
				QuerySelector: &QuerySelector{
					Key: k,
					QueryRangeSelect: &QueryRangeSelect{
						QueryScoreRange: QueryScoreRange{
							MaxFromInput: true,
						},
					},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, []ID{ii[0], ii[1], ii[2]}, res.IDs)
}

func TestQueryIDScoreSelect(t *T) {
	base := testutil.RandStr()
	k, ii := randPopulatedKey(t, base, 1)

	res, err := testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: k,
					QueryIDScoreSelect: &QueryIDScoreSelect{
						ID: ii[0],
					},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, []ID{ii[0]}, res.IDs)

	res, err = testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: k,
					QueryIDScoreSelect: &QueryIDScoreSelect{
						ID:    ii[0],
						Equal: ii[0].T,
					},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, []ID{ii[0]}, res.IDs)

	res, err = testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: k,
					QueryIDScoreSelect: &QueryIDScoreSelect{
						ID:  ii[0],
						Min: ii[0].T + 10,
					},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Empty(t, res.IDs)
}

// Tests PosRangeSelect
func TestQueryPosRangeSelect(t *T) {
	base := testutil.RandStr()
	k, ii := randPopulatedKey(t, base, 4)

	res, err := testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key:            k,
					PosRangeSelect: []int64{1, -2},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, []ID{ii[1], ii[2]}, res.IDs)
}

// Tests that expired IDs don't get returned when filtered. Also tests Invert
func TestQueryFiltering(t *T) {
	k := randKey(testutil.RandStr())
	ii := []ID{
		requireNewID(t),
		requireNewID(t),
		requireNewID(t),
		requireNewID(t),
	}
	ii[1].Expire = NewTS(time.Now().Add(-1 * time.Second))
	ii[3].Expire = NewTS(time.Now().Add(-1 * time.Second))

	res, err := testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: k,
					IDs: ii,
				},
			},
			{
				QueryFilter: &QueryFilter{
					Expired: true,
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, []ID{ii[0], ii[2]}, res.IDs)

	res, err = testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: k,
					IDs: ii,
				},
			},
			{
				QueryFilter: &QueryFilter{
					Expired: true,
					Invert:  true,
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, []ID{ii[1], ii[3]}, res.IDs)
}

func TestQueryIDs(t *T) {
	base := testutil.RandStr()
	k := randKey(base)

	ii := []ID{
		requireNewID(t),
		requireNewID(t),
		requireNewID(t),
	}

	res, err := testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: k,
					IDs: ii,
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, ii, res.IDs)
}

// Tests that an empty query result set doesn't fuck with decoding
func TestQueryEmpty(t *T) {
	base := testutil.RandStr()
	k := randKey(base)
	res, err := testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: k,
					IDs: []ID{},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, []ID{}, res.IDs)
}

func TestQueryUnion(t *T) {
	base := testutil.RandStr()
	k := randKey(base)

	iiA := []ID{
		requireNewID(t),
		requireNewID(t),
	}
	iiB := []ID{
		iiA[0],
		requireNewID(t),
	}
	iiU := []ID{
		iiA[0],
		iiA[1],
		iiB[1],
	}
	assert.True(t, iiU[0].T < iiU[1].T)
	assert.True(t, iiU[1].T < iiU[2].T)

	// First test that previous output is overwritten without Union
	res, err := testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: k,
					IDs: iiA,
				},
			},
			{
				QuerySelector: &QuerySelector{
					Key: k,
					IDs: iiB,
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, iiB, res.IDs)

	res, err = testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: k,
					IDs: iiA,
				},
			},
			{
				QuerySelector: &QuerySelector{
					Key: k,
					IDs: iiB,
				},
				Union: true,
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, iiU, res.IDs)
}

func TestQueryBreak(t *T) {
	base := testutil.RandStr()
	k := randKey(base)
	iiA := []ID{requireNewID(t)}
	iiB := []ID{requireNewID(t)}

	res, err := testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: k,
					IDs: iiA,
				},
			},
			{
				Break: true,
			},
			{
				QuerySelector: &QuerySelector{
					Key: k,
					IDs: iiB,
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, iiA, res.IDs)

	res, err = testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: k,
					IDs: iiA,
				},
			},
			{
				Break:            true,
				QueryConditional: QueryConditional{IfNoInput: true},
			},
			{
				QuerySelector: &QuerySelector{
					Key: k,
					IDs: iiB,
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, iiB, res.IDs)
}

func TestQueryConditionals(t *T) {
	base := testutil.RandStr()
	keyFull, _ := randPopulatedKey(t, base, 5)
	keyEmpty := randKey(base)
	id := requireNewID(t)

	res, err := testCore.Query(QueryActions{
		KeyBase: base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					IDs: []ID{id},
				},
				QueryConditional: QueryConditional{
					IfEmpty: &keyEmpty,
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, id, res.IDs[0])

	res, err = testCore.Query(QueryActions{
		KeyBase: base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					IDs: []ID{id},
				},
				QueryConditional: QueryConditional{
					IfEmpty: &keyFull,
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Empty(t, res.IDs)

	res, err = testCore.Query(QueryActions{
		KeyBase: base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					IDs: []ID{id},
				},
				QueryConditional: QueryConditional{
					And: []QueryConditional{
						{
							IfEmpty: &keyEmpty,
						},
						{
							IfNotEmpty: &keyFull,
						},
					},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, id, res.IDs[0])

	res, err = testCore.Query(QueryActions{
		KeyBase: base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					IDs: []ID{id},
				},
				QueryConditional: QueryConditional{
					And: []QueryConditional{
						{
							IfEmpty: &keyEmpty,
						},
						{
							IfEmpty: &keyFull,
						},
					},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Empty(t, res.IDs)
}

func TestQueryCount(t *T) {
	base := testutil.RandStr()
	k1, ii1 := randPopulatedKey(t, base, 5)
	k2, _ := randPopulatedKey(t, base, 1)
	qsr := QueryScoreRange{}

	assertCounts := func(a, b uint64) {
		res, err := testCore.Query(QueryActions{
			KeyBase: base,
			QueryActions: []QueryAction{
				{
					QueryCount: &QueryCount{Key: k1, QueryScoreRange: qsr},
				},
				{
					QueryCount: &QueryCount{Key: k2, QueryScoreRange: qsr},
				},
			},
		})
		require.Nil(t, err)
		assert.Equal(t, []uint64{a, b}, res.Counts)
	}

	assertCounts(5, 1)

	qsr.Min = ii1[0].T
	assertCounts(5, 1)

	qsr.MinExcl = true
	assertCounts(4, 1)

	qsr.Max = ii1[4].T
	assertCounts(4, 0)

	qsr.MaxExcl = true
	assertCounts(3, 0)

	res, err := testCore.Query(QueryActions{
		KeyBase: base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: k1,
					QueryRangeSelect: &QueryRangeSelect{
						QueryScoreRange: qsr,
					},
				},
			},
			{
				CountInput: true,
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, 3, len(res.IDs))
	assert.Equal(t, uint64(3), res.Counts[0])
}

func TestKeyScan(t *T) {
	base1 := testutil.RandStr()
	base2 := testutil.RandStr()
	k11, _ := randPopulatedKey(t, base1, 1)
	k12, _ := randPopulatedKey(t, base1, 1)
	k21, _ := randPopulatedKey(t, base2, 1)
	k22, _ := randPopulatedKey(t, base2, 1)

	assertScan := func(pattern Key, kk ...Key) {
		found, err := testCore.KeyScan(pattern)
		require.Nil(t, err)
		for _, k := range found {
			assert.Contains(t, kk, k, "k.String():%q", k.String(testCore.o.RedisPrefix))
		}
	}

	assertScan(k11, k11)
	assertScan(Key{Base: base1, Subs: []string{"*"}}, k11, k12)
	assertScan(Key{Base: base2, Subs: []string{"*"}}, k21, k22)
	assertScan(Key{Base: "*"}, k11, k12, k21, k22)
}

func TestSingleGetSet(t *T) {
	key := randKey(testutil.RandStr())
	id := requireNewID(t)

	// Setting returns the ID
	res, err := testCore.Query(QueryActions{
		KeyBase: key.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					IDs: []ID{id},
				},
			},
			{
				QuerySingleSet: &QuerySingleSet{
					Key: key,
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, id, res.IDs[0])

	// Getting a set ID returns it
	res, err = testCore.Query(QueryActions{
		KeyBase: key.Base,
		QueryActions: []QueryAction{
			{
				SingleGet: &key,
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, id, res.IDs[0])

	// Getting a set ID after the expire doesn't return it
	res, err = testCore.Query(QueryActions{
		Now:     (id.Expire + 1).Time(),
		KeyBase: key.Base,
		QueryActions: []QueryAction{
			{
				SingleGet: &key,
			},
		},
	})
	require.Nil(t, err)
	assert.Empty(t, res.IDs)

	// Trying to put an older ID with IfNewer results in no change
	id2 := id
	id2.T -= 5
	res, err = testCore.Query(QueryActions{
		KeyBase: key.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					IDs: []ID{id2},
				},
			},
			{
				QuerySingleSet: &QuerySingleSet{
					Key:     key,
					IfNewer: true,
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, id2, res.IDs[0])

	res, err = testCore.Query(QueryActions{
		KeyBase: key.Base,
		QueryActions: []QueryAction{
			{
				SingleGet: &key,
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, id, res.IDs[0])
}
