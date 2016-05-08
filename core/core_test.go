package core

import (
	. "testing"
	"time"

	"github.com/levenlabs/golib/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testCore Core

func init() {
	var err error
	if testCore, err = New("127.0.0.1:6379", 1); err != nil {
		panic(err)
	}
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
	e, err := testCore.NewEvent(time.Now().Add(1*time.Minute), testutil.RandStr())
	require.Nil(t, err)
	return e
}

func requireNewEmptyEvent(t *T) Event {
	e, err := testCore.NewEvent(time.Now().Add(1*time.Minute), "")
	require.Nil(t, err)
	return e
}

func randEventSet(base string) EventSet {
	return EventSet{
		Base: base,
		Subs: []string{testutil.RandStr()},
	}
}

func populatedEventSet(t *T, base string, ee ...Event) EventSet {
	es := randEventSet(base)
	_, err := testCore.Query(QueryActions{
		EventSetBase: es.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{Events: ee},
			},
			{
				QueryAddTo: &QueryAddTo{
					EventSets: []EventSet{es},
				},
			},
		},
	})
	require.Nil(t, err, "%s", err)
	return es
}

func randPopulatedEventSet(t *T, base string, size int) (EventSet, []Event) {
	ee := make([]Event, size)
	for i := range ee {
		ee[i] = requireNewEmptyEvent(t)
	}
	es := populatedEventSet(t, base, ee...)
	return es, ee
}

func assertEventSet(t *T, es EventSet, ee ...Event) {
	ee2, err := testCore.Query(QueryActions{
		EventSetBase: es.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					EventSet: es,
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

func TestGetSetEvent(t *T) {
	contents := testutil.RandStr()
	expire := time.Now().Add(500 * time.Millisecond)

	e, err := testCore.NewEvent(expire, contents)
	require.Nil(t, err)

	assert.Nil(t, testCore.SetEvent(e, 500*time.Millisecond))
	e2, err := testCore.GetEvent(e.ID)
	assert.Nil(t, err)
	assert.Equal(t, e, e2)

	time.Sleep(1*time.Second + 100*time.Millisecond)
	_, err = testCore.GetEvent(e.ID)
	assert.Equal(t, ErrNotFound, err)
}

func TestEventSetKeys(t *T) {
	ess := []EventSet{
		{Base: testutil.RandStr(), Subs: nil},
		{Base: testutil.RandStr(), Subs: []string{testutil.RandStr()}},
		{Base: testutil.RandStr(), Subs: []string{testutil.RandStr(), testutil.RandStr()}},
	}

	for _, es := range ess {
		key := es.key()
		assert.Equal(t, es, eventSetFromKey(key), "key:%q", key)
	}
}

// Tests AddTo/RemoveFrom, as well as infinite Min/Max in QueryEventRangeSelect,
// and Events in QuerySelect
func TestQueryBasicAddRemove(t *T) {
	base := testutil.RandStr()
	es, ee := randPopulatedEventSet(t, base, 3)
	assertEventSet(t, es, ee...)

	_, err := testCore.Query(QueryActions{
		EventSetBase: es.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					EventSet: es,
					Events: []Event{
						ee[1],
					},
				},
			},
			{
				RemoveFrom: []EventSet{es},
			},
		},
	})
	require.Nil(t, err)
	assertEventSet(t, es, ee[0], ee[2])
}

func TestQueryRemoveByScore(t *T) {
	// This test is not very strict, most of the funtionality here comes form
	// QueryScoreRange, which is tested in TestQueryRangeSelect extensively
	base := testutil.RandStr()
	es, ee := randPopulatedEventSet(t, base, 4)
	ee2, err := testCore.Query(QueryActions{
		EventSetBase: es.Base,
		QueryActions: []QueryAction{
			{
				QueryRemoveByScore: &QueryRemoveByScore{
					QueryScoreRange: QueryScoreRange{
						Max:     TS(ee[2].ID),
						MaxExcl: true,
					},
					EventSets: []EventSet{es},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Empty(t, ee2)
	assertEventSet(t, es, ee[2], ee[3])
}

// Tests normal Min/Max, MinExcl and MaxExcl, Limit/Offset, and
// MinFromInput/MaxFromInput in QueryEventRangeSelect
func TestQueryRangeSelect(t *T) {
	base := testutil.RandStr()
	es, ee := randPopulatedEventSet(t, base, 4)

	ee2, err := testCore.Query(QueryActions{
		EventSetBase: es.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					EventSet: es,
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
		EventSetBase: es.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					EventSet: es,
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
		EventSetBase: es.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					EventSet: es,
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
		EventSetBase: es.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					EventSet: es,
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

	minES := populatedEventSet(t, base, ee[0], ee[1])
	maxES := populatedEventSet(t, base, ee[2], ee[3])
	ee2, err = testCore.Query(QueryActions{
		EventSetBase: es.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					EventSet:              minES,
					QueryEventRangeSelect: &QueryEventRangeSelect{},
				},
			},
			{
				QuerySelector: &QuerySelector{
					EventSet: es,
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
		EventSetBase: es.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					EventSet:              maxES,
					QueryEventRangeSelect: &QueryEventRangeSelect{},
				},
			},
			{
				QuerySelector: &QuerySelector{
					EventSet: es,
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
	es, ee := randPopulatedEventSet(t, base, 1)

	ee2, err := testCore.Query(QueryActions{
		EventSetBase: es.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					EventSet: es,
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
		EventSetBase: es.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					EventSet: es,
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
		EventSetBase: es.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					EventSet: es,
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
	es, ee := randPopulatedEventSet(t, base, 4)

	ee2, err := testCore.Query(QueryActions{
		EventSetBase: es.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					EventSet:       es,
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
	es := randEventSet(testutil.RandStr())
	ee := []Event{
		requireNewEmptyEvent(t),
		requireNewEmptyEvent(t),
		requireNewEmptyEvent(t),
		requireNewEmptyEvent(t),
	}
	ee[1].ID = ID(NewTS(time.Now().Add(-1 * time.Second)))
	ee[3].ID = ID(NewTS(time.Now().Add(-1 * time.Second)))

	ee2, err := testCore.Query(QueryActions{
		EventSetBase: es.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					EventSet: es,
					Events:   ee,
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
		EventSetBase: es.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					EventSet: es,
					Events:   ee,
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
	es := randEventSet(base)

	ee := []Event{
		requireNewEmptyEvent(t),
		requireNewEmptyEvent(t),
		requireNewEmptyEvent(t),
	}

	ee2, err := testCore.Query(QueryActions{
		EventSetBase: es.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					EventSet: es,
					Events:   ee,
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
	es := randEventSet(base)
	ee, err := testCore.Query(QueryActions{
		EventSetBase: es.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					EventSet: es,
					Events:   []Event{},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, []Event{}, ee)
}

func TestQueryUnion(t *T) {
	base := testutil.RandStr()
	es := randEventSet(base)

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
		EventSetBase: es.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					EventSet: es,
					Events:   eeA,
				},
			},
			{
				QuerySelector: &QuerySelector{
					EventSet: es,
					Events:   eeB,
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, eeB, ee2)

	ee2, err = testCore.Query(QueryActions{
		EventSetBase: es.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					EventSet: es,
					Events:   eeA,
				},
			},
			{
				QuerySelector: &QuerySelector{
					EventSet: es,
					Events:   eeB,
					Union:    true,
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, eeU, ee2)
}

func TestQueryBreak(t *T) {
	base := testutil.RandStr()
	es := randEventSet(base)
	eeA := []Event{requireNewEmptyEvent(t)}
	eeB := []Event{requireNewEmptyEvent(t)}

	ee2, err := testCore.Query(QueryActions{
		EventSetBase: es.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					EventSet: es,
					Events:   eeA,
				},
			},
			{
				Break: true,
			},
			{
				QuerySelector: &QuerySelector{
					EventSet: es,
					Events:   eeB,
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, eeA, ee2)

	ee2, err = testCore.Query(QueryActions{
		EventSetBase: es.Base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					EventSet: es,
					Events:   eeA,
				},
			},
			{
				Break:            true,
				QueryConditional: QueryConditional{IfNoInput: true},
			},
			{
				QuerySelector: &QuerySelector{
					EventSet: es,
					Events:   eeB,
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, eeB, ee2)
}

func TestQueryConditionals(t *T) {
	base := testutil.RandStr()
	esFull, _ := randPopulatedEventSet(t, base, 5)
	esEmpty := randEventSet(base)
	e := requireNewEmptyEvent(t)

	ee, err := testCore.Query(QueryActions{
		EventSetBase: base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Events: []Event{e},
				},
				QueryConditional: QueryConditional{
					IfEmpty: &esEmpty,
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, e, ee[0])

	ee, err = testCore.Query(QueryActions{
		EventSetBase: base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Events: []Event{e},
				},
				QueryConditional: QueryConditional{
					IfEmpty: &esFull,
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Empty(t, ee)

	ee, err = testCore.Query(QueryActions{
		EventSetBase: base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Events: []Event{e},
				},
				QueryConditional: QueryConditional{
					And: []QueryConditional{
						{
							IfEmpty: &esEmpty,
						},
						{
							IfNotEmpty: &esFull,
						},
					},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, e, ee[0])

	ee, err = testCore.Query(QueryActions{
		EventSetBase: base,
		QueryActions: []QueryAction{
			{
				QuerySelector: &QuerySelector{
					Events: []Event{e},
				},
				QueryConditional: QueryConditional{
					And: []QueryConditional{
						{
							IfEmpty: &esEmpty,
						},
						{
							IfEmpty: &esFull,
						},
					},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Empty(t, ee)
}
