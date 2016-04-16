package core

import (
	. "testing"

	"github.com/levenlabs/golib/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertEventSet(t *T, es EventSet, ee ...Event) {
	ee2, err := Query(QueryAction{
		QuerySelector: QuerySelector{
			EventSet: es,
			QueryEventRangeSelect: &QueryEventRangeSelect{
				Min: 0,
				Max: 0,
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, ee, ee2)
}

func populatedEventSet(t *T, base string, ee ...Event) EventSet {
	es := randEventSet(base)
	_, err := Query(QueryAction{
		QuerySelector: QuerySelector{
			EventSet: es,
			Events:   ee,
		},
		AddTo: []EventSet{es},
	})
	require.Nil(t, err)
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

// Tests AddTo/RemoveFrom, as well as infinite Min/Max in QueryEventRangeSelect,
// and Events in QuerySelect
func TestQueryBasicAddRemove(t *T) {
	base := testutil.RandStr()
	es, ee := randPopulatedEventSet(t, base, 3)
	assertEventSet(t, es, ee...)

	_, err := Query(QueryAction{
		QuerySelector: QuerySelector{
			EventSet: es,
			Events: []Event{
				ee[1],
			},
		},
		RemoveFrom: []EventSet{es},
	})
	require.Nil(t, err)
	assertEventSet(t, es, ee[0], ee[2])
}

// Tests normal Min/Max, MinExcl and MaxExcl, Limit/Offset, and
// MinQuerySelector/MaxQuerySelector in QueryEventRangeSelect
func TestQueryRangeSelect(t *T) {
	base := testutil.RandStr()
	es, ee := randPopulatedEventSet(t, base, 4)

	ee2, err := Query(QueryAction{
		QuerySelector: QuerySelector{
			EventSet: es,
			QueryEventRangeSelect: &QueryEventRangeSelect{
				Min: ee[1].ID,
				Max: ee[2].ID,
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, []Event{ee[1], ee[2]}, ee2)

	ee2, err = Query(QueryAction{
		QuerySelector: QuerySelector{
			EventSet: es,
			QueryEventRangeSelect: &QueryEventRangeSelect{
				Min:     ee[0].ID,
				MinExcl: true,
				Max:     ee[3].ID,
				MaxExcl: true,
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, []Event{ee[1], ee[2]}, ee2)

	ee2, err = Query(QueryAction{
		QuerySelector: QuerySelector{
			EventSet: es,
			QueryEventRangeSelect: &QueryEventRangeSelect{
				Min:    ee[0].ID,
				Max:    ee[3].ID,
				Offset: 1,
				Limit:  2,
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, []Event{ee[1], ee[2]}, ee2)

	minES := populatedEventSet(t, base, ee[0], ee[1])
	maxES := populatedEventSet(t, base, ee[2], ee[3])
	ee2, err = Query(QueryAction{
		QuerySelector: QuerySelector{
			EventSet: es,
			QueryEventRangeSelect: &QueryEventRangeSelect{
				MinQuerySelector: &QuerySelector{
					EventSet:              minES,
					QueryEventRangeSelect: &QueryEventRangeSelect{},
				},
				MaxQuerySelector: &QuerySelector{
					EventSet:              maxES,
					QueryEventRangeSelect: &QueryEventRangeSelect{},
				},
			},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, []Event{ee[1], ee[2]}, ee2)
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

	ee2, err := Query(QueryAction{
		QuerySelector: QuerySelector{
			EventSet: es,
			Events:   ee,
		},
	})
	require.Nil(t, err)
	assert.Equal(t, ee, ee2)
}

// Tests that an empty query result set doesn't fuck with decoding
func TestQueryEmpty(t *T) {
	base := testutil.RandStr()
	es := randEventSet(base)
	ee, err := Query(QueryAction{
		QuerySelector: QuerySelector{
			EventSet: es,
			Events:   []Event{},
		},
	})
	require.Nil(t, err)
	assert.Equal(t, []Event{}, ee)
}
