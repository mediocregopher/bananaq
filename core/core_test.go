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

	if testCore, err = New(p, nil); err != nil {
		panic(err)
	}
	go func() { panic(testCore.Run()) }()

	// We set idKey to be random because this package's tests assume they are
	// the only thing calling NewID. But if tests for other sub-packages in
	// bananaq are also running they will be calling it too.
	idKey = idKey + ":" + testutil.RandStr()
}

func TestNewID(t *T) {
	for i := 0; i < 100; i++ {
		now := time.Now()
		nowTS := NewTS(now)
		id, err := testCore.NewID(nowTS)
		assert.Nil(t, err)
		assert.Equal(t, ID(nowTS), id)

		// make sure at least the second precision matches, the actual times
		// won't match exactly because we truncate to microseconds
		assert.Equal(t, now.Unix(), TS(id).Time().Unix())

		id2, err := testCore.NewID(nowTS)
		assert.Nil(t, err)
		assert.True(t, id2 > id, "id2:%d !> id:%d ", id2, id)

		nowTS = NewTS(time.Now())
		id3, err := testCore.NewID(nowTS)
		assert.Nil(t, err)
		assert.True(t, id3 > id2, "id3:%d !> id2:%d ", id3, id2)
	}
}

func requireNewID(t *T) ID {
	id, err := testCore.NewID(NewTS(time.Now()))
	require.Nil(t, err)
	return id
}

func requireNewEvent(t *T) Event {
	ts := NewTS(time.Now().Add(1 * time.Minute))
	e, err := testCore.NewEvent(ts, testutil.RandStr())
	require.Nil(t, err)
	return e
}

func requireNewEmptyEvent(t *T) Event {
	ts := NewTS(time.Now().Add(1 * time.Minute))
	e, err := testCore.NewEvent(ts, "")
	require.Nil(t, err)
	return e
}

func randKey(base string) Key {
	return Key{
		Base: base,
		Subs: []string{testutil.RandStr()},
	}
}

func populatedKey(t *T, base string, ee ...Event) Key {
	k := randKey(base)
	_, err := testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{Events: ee},
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

func randPopulatedKey(t *T, base string, size int) (Key, []Event) {
	ee := make([]Event, size)
	for i := range ee {
		ee[i] = requireNewEmptyEvent(t)
	}
	k := populatedKey(t, base, ee...)
	return k, ee
}

func assertKey(t *T, k Key, ee ...Event) {
	ee2, err := testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: k,
					QueryEventRangeSelect: &QueryEventRangeSelect{
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
	assert.Equal(t, ee, ee2)
}

// Assert the contents of an eventset as well as their scores
func assertKeyRaw(t *T, k Key, exm map[Event]int64) {
	arr, err := testCore.Cmd("ZRANGE", k.String(), 0, -1, "WITHSCORES").Array()
	require.Nil(t, err)

	m := map[Event]int64{}
	for i := 0; i < len(arr); i += 2 {
		eb, err := arr[i].Bytes()
		require.Nil(t, err)
		score, err := arr[i+1].Int64()
		require.Nil(t, err)

		var e Event
		_, err = e.UnmarshalMsg(eb)
		require.Nil(t, err)

		m[e] = score
	}
	assert.Equal(t, exm, m)
}

func TestGetSetEvent(t *T) {
	contents := testutil.RandStr()
	expire := time.Now().Add(500 * time.Millisecond)

	e, err := testCore.NewEvent(NewTS(expire), contents)
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
		str := k.String()
		assert.Equal(t, k, KeyFromString(str), "key:%q", str)
	}
}

// Tests AddTo/RemoveFrom, as well as infinite Min/Max in QueryEventRangeSelect,
// and Events in QuerySelect
func TestQueryBasicAddRemove(t *T) {
	base := testutil.RandStr()
	k, ee := randPopulatedKey(t, base, 3)
	assertKey(t, k, ee...)

	_, err := testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: k,
					Events: []Event{
						ee[1],
					},
				},
			},
			{
				RemoveFrom: []Key{k},
			},
		},
	})
	require.Nil(t, err)
	assertKey(t, k, ee[0], ee[2])
}

func TestQueryAddScores(t *T) {
	base := testutil.RandStr()
	k1 := randKey(base)
	k2 := randKey(base)
	k3 := randKey(base)
	ee := []Event{
		requireNewEmptyEvent(t),
		requireNewEmptyEvent(t),
		requireNewEmptyEvent(t),
		requireNewEmptyEvent(t),
	}
	ee[1].Expire = NewTS(time.Now().Add(-1 * time.Second))
	ee[3].Expire = NewTS(time.Now().Add(-1 * time.Second))

	ee2, err := testCore.Query(QueryActions{
		KeyBase: base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{Events: ee},
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
	assert.Equal(t, ee, ee2)
	assertKeyRaw(t, k1, map[Event]int64{
		ee[0]: int64(ee[0].ID),
		ee[1]: int64(ee[1].ID),
		ee[2]: int64(ee[2].ID),
		ee[3]: int64(ee[3].ID),
	})
	assertKeyRaw(t, k2, map[Event]int64{
		ee[0]: int64(ee[0].Expire),
		ee[1]: int64(ee[1].Expire),
		ee[2]: int64(ee[2].Expire),
		ee[3]: int64(ee[3].Expire),
	})
	assertKeyRaw(t, k3, map[Event]int64{
		ee[0]: 5,
		ee[1]: 5,
		ee[2]: 5,
		ee[3]: 5,
	})
}

func TestQueryRemoveByScore(t *T) {
	// This test is not very strict, most of the funtionality here comes form
	// QueryScoreRange, which is tested in TestQueryRangeSelect extensively
	base := testutil.RandStr()
	k, ee := randPopulatedKey(t, base, 4)
	ee2, err := testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QueryRemoveByScore: &QueryRemoveByScore{
					QueryScoreRange: QueryScoreRange{
						Max:     TS(ee[2].ID),
						MaxExcl: true,
					},
					Keys: []Key{k},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Empty(t, ee2)
	assertKey(t, k, ee[2], ee[3])
}

// Tests normal Min/Max, MinExcl and MaxExcl, Limit/Offset, and
// MinFromInput/MaxFromInput in QueryEventRangeSelect
func TestQueryRangeSelect(t *T) {
	base := testutil.RandStr()
	k, ee := randPopulatedKey(t, base, 4)

	ee2, err := testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: k,
					QueryEventRangeSelect: &QueryEventRangeSelect{
						QueryScoreRange: QueryScoreRange{
							Min: TS(ee[1].ID),
							Max: TS(ee[2].ID),
						},
					},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, []Event{ee[1], ee[2]}, ee2)

	ee2, err = testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: k,
					QueryEventRangeSelect: &QueryEventRangeSelect{
						QueryScoreRange: QueryScoreRange{
							Min:     TS(ee[0].ID),
							MinExcl: true,
							Max:     TS(ee[3].ID),
							MaxExcl: true,
						},
					},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, []Event{ee[1], ee[2]}, ee2)

	ee2, err = testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: k,
					QueryEventRangeSelect: &QueryEventRangeSelect{
						QueryScoreRange: QueryScoreRange{
							Min: TS(ee[0].ID),
							Max: TS(ee[3].ID),
						},
						Offset: 1,
						Limit:  2,
					},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, []Event{ee[1], ee[2]}, ee2)

	ee2, err = testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: k,
					QueryEventRangeSelect: &QueryEventRangeSelect{
						QueryScoreRange: QueryScoreRange{
							Min: TS(ee[1].ID),
							Max: TS(ee[2].ID),
						},
						Reverse: true,
						Limit:   1,
					},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, []Event{ee[2]}, ee2)

	minK := populatedKey(t, base, ee[0], ee[1])
	maxK := populatedKey(t, base, ee[2], ee[3])
	ee2, err = testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: minK,
					QueryEventRangeSelect: &QueryEventRangeSelect{},
				},
			},
			{
				QuerySelector: &QuerySelector{
					Key: k,
					QueryEventRangeSelect: &QueryEventRangeSelect{
						QueryScoreRange: QueryScoreRange{
							MinFromInput: true,
						},
					},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, []Event{ee[1], ee[2], ee[3]}, ee2)

	ee2, err = testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: maxK,
					QueryEventRangeSelect: &QueryEventRangeSelect{},
				},
			},
			{
				QuerySelector: &QuerySelector{
					Key: k,
					QueryEventRangeSelect: &QueryEventRangeSelect{
						QueryScoreRange: QueryScoreRange{
							MaxFromInput: true,
						},
					},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, []Event{ee[0], ee[1], ee[2]}, ee2)
}

func TestQueryEventScoreSelect(t *T) {
	base := testutil.RandStr()
	k, ee := randPopulatedKey(t, base, 1)

	ee2, err := testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: k,
					QueryEventScoreSelect: &QueryEventScoreSelect{
						Event: ee[0],
					},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, []Event{ee[0]}, ee2)

	ee2, err = testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: k,
					QueryEventScoreSelect: &QueryEventScoreSelect{
						Event: ee[0],
						Equal: TS(ee[0].ID),
					},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, []Event{ee[0]}, ee2)

	ee2, err = testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key: k,
					QueryEventScoreSelect: &QueryEventScoreSelect{
						Event: ee[0],
						Min:   TS(ee[0].ID) + 10,
					},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Empty(t, ee2)
}

// Tests PosRangeSelect
func TestQueryPosRangeSelect(t *T) {
	base := testutil.RandStr()
	k, ee := randPopulatedKey(t, base, 4)

	ee2, err := testCore.Query(QueryActions{
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
	assert.Equal(t, []Event{ee[1], ee[2]}, ee2)
}

// Tests that expired events don't get returned when filtered. Also tests Invert
func TestQueryFiltering(t *T) {
	k := randKey(testutil.RandStr())
	ee := []Event{
		requireNewEmptyEvent(t),
		requireNewEmptyEvent(t),
		requireNewEmptyEvent(t),
		requireNewEmptyEvent(t),
	}
	ee[1].Expire = NewTS(time.Now().Add(-1 * time.Second))
	ee[3].Expire = NewTS(time.Now().Add(-1 * time.Second))

	ee2, err := testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key:    k,
					Events: ee,
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
	assert.Equal(t, []Event{ee[0], ee[2]}, ee2)

	ee2, err = testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key:    k,
					Events: ee,
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
	assert.Equal(t, []Event{ee[1], ee[3]}, ee2)
}

// Tests Events in QuerySelect
func TestQueryEvents(t *T) {
	base := testutil.RandStr()
	k := randKey(base)

	ee := []Event{
		requireNewEmptyEvent(t),
		requireNewEmptyEvent(t),
		requireNewEmptyEvent(t),
	}

	ee2, err := testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key:    k,
					Events: ee,
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, ee, ee2)
}

// Tests that an empty query result set doesn't fuck with decoding
func TestQueryEmpty(t *T) {
	base := testutil.RandStr()
	k := randKey(base)
	ee, err := testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key:    k,
					Events: []Event{},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, []Event{}, ee)
}

func TestQueryUnion(t *T) {
	base := testutil.RandStr()
	k := randKey(base)

	eeA := []Event{
		requireNewEmptyEvent(t),
		requireNewEmptyEvent(t),
	}
	eeB := []Event{
		eeA[0],
		requireNewEmptyEvent(t),
	}
	eeU := []Event{
		eeA[0],
		eeA[1],
		eeB[1],
	}
	assert.True(t, eeU[0].ID < eeU[1].ID)
	assert.True(t, eeU[1].ID < eeU[2].ID)

	// First test that previous output is overwritten without Union
	ee2, err := testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key:    k,
					Events: eeA,
				},
			},
			{
				QuerySelector: &QuerySelector{
					Key:    k,
					Events: eeB,
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, eeB, ee2)

	ee2, err = testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key:    k,
					Events: eeA,
				},
			},
			{
				QuerySelector: &QuerySelector{
					Key:    k,
					Events: eeB,
					Union:  true,
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, eeU, ee2)
}

func TestQueryBreak(t *T) {
	base := testutil.RandStr()
	k := randKey(base)
	eeA := []Event{requireNewEmptyEvent(t)}
	eeB := []Event{requireNewEmptyEvent(t)}

	ee2, err := testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key:    k,
					Events: eeA,
				},
			},
			{
				Break: true,
			},
			{
				QuerySelector: &QuerySelector{
					Key:    k,
					Events: eeB,
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, eeA, ee2)

	ee2, err = testCore.Query(QueryActions{
		KeyBase: k.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Key:    k,
					Events: eeA,
				},
			},
			{
				Break:            true,
				QueryConditional: QueryConditional{IfNoInput: true},
			},
			{
				QuerySelector: &QuerySelector{
					Key:    k,
					Events: eeB,
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, eeB, ee2)
}

func TestQueryConditionals(t *T) {
	base := testutil.RandStr()
	kFull, _ := randPopulatedKey(t, base, 5)
	kEmpty := randKey(base)
	e := requireNewEmptyEvent(t)

	ee, err := testCore.Query(QueryActions{
		KeyBase: base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Events: []Event{e},
				},
				QueryConditional: QueryConditional{
					IfEmpty: &kEmpty,
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, e, ee[0])

	ee, err = testCore.Query(QueryActions{
		KeyBase: base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Events: []Event{e},
				},
				QueryConditional: QueryConditional{
					IfEmpty: &kFull,
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Empty(t, ee)

	ee, err = testCore.Query(QueryActions{
		KeyBase: base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Events: []Event{e},
				},
				QueryConditional: QueryConditional{
					And: []QueryConditional{
						{
							IfEmpty: &kEmpty,
						},
						{
							IfNotEmpty: &kFull,
						},
					},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, e, ee[0])

	ee, err = testCore.Query(QueryActions{
		KeyBase: base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Events: []Event{e},
				},
				QueryConditional: QueryConditional{
					And: []QueryConditional{
						{
							IfEmpty: &kEmpty,
						},
						{
							IfEmpty: &kFull,
						},
					},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Empty(t, ee)
}

func TestSetCounts(t *T) {
	base := testutil.RandStr()
	k1, _ := randPopulatedKey(t, base, 5)
	k2, _ := randPopulatedKey(t, base, 1)

	counts, err := testCore.SetCounts(randKey(base), k1, randKey(base), k2)
	require.Nil(t, err)
	assert.Equal(t, []uint64{0, 5, 0, 1}, counts)
}
