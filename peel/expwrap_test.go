package peel

import (
	. "testing"
	"time"

	"github.com/levenlabs/golib/testutil"
	"github.com/mediocregopher/bananaq/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func randExWrap() exWrap {
	k := core.Key{Base: testutil.RandStr(), Subs: []string{testutil.RandStr()}}
	return newExWrap(k)
}

func requireExWrapDo(t *T, now core.TS, ex exWrap, aa ...core.QueryAction) {
	_, err := testPeel.c.Query(core.QueryActions{
		KeyBase:      ex.byArb.Base,
		QueryActions: aa,
		Now:          now,
	})
	require.Nil(t, err)
}

func assertExWrapOut(t *T, now core.TS, ex exWrap, out []core.ID, aa ...core.QueryAction) {
	res, err := testPeel.c.Query(core.QueryActions{
		KeyBase:      ex.byArb.Base,
		QueryActions: aa,
		Now:          now,
	})
	require.Nil(t, err)
	assert.Equal(t, out, res.IDs)
}

func assertExWrapCounts(t *T, now core.TS, ex exWrap, counts []uint64, aa ...core.QueryAction) {
	res, err := testPeel.c.Query(core.QueryActions{
		KeyBase:      ex.byArb.Base,
		QueryActions: aa,
		Now:          now,
	})
	require.Nil(t, err)
	assert.Equal(t, counts, res.Counts)
}

func TestExWrapAddClean(t *T) {
	ex := randExWrap()
	id1 := randID(t, false)
	id2 := randID(t, false)
	id3 := randID(t, true)
	now := core.NewTS(time.Now())

	// Unfortunately core always returns in ID order, so it's not really
	// possibly to test that the scores are being stored correctly from here. We
	// just have to trust core and the later tests
	requireExWrapDo(t, now, ex, ex.add(id1, id1.T)...)
	requireExWrapDo(t, now, ex, ex.add(id2, id2.T)...)
	requireExWrapDo(t, now, ex, ex.add(id3, id3.T)...)
	assertKey(t, ex.byArb, id1, id2, id3)
	assertKey(t, ex.byExp, id1, id2, id3)

	requireExWrapDo(t, now, ex, ex.removeExpired(now)...)
	assertKey(t, ex.byArb, id1, id2)
	assertKey(t, ex.byExp, id1, id2)
}

func TestExWrapFirstAfterAllBefore(t *T) {
	ex := randExWrap()
	id1 := randID(t, false)
	id2 := randID(t, false)
	id3 := randID(t, false)
	now := core.NewTS(time.Now())
	requireExWrapDo(t, now, ex, ex.add(id1, id1.T)...)
	requireExWrapDo(t, now, ex, ex.add(id2, id2.T)...)
	requireExWrapDo(t, now, ex, ex.add(id3, id3.T)...)

	assertExWrapOut(t, now, ex, []core.ID{id1, id2, id3}, ex.after(0, 0))
	assertExWrapOut(t, now, ex, []core.ID{id1}, ex.after(0, 1))
	assertExWrapOut(t, now, ex, []core.ID{id2, id3}, ex.after(id1.T, 2))
	assertExWrapOut(t, now, ex, []core.ID{id2}, ex.after(0, 1), ex.afterInput(1))

	assertExWrapOut(t, now, ex, []core.ID{id1, id2, id3}, ex.before(id3.T+1, 0))
	assertExWrapOut(t, now, ex, []core.ID{id3}, ex.before(id3.T+1, 1))
	assertExWrapOut(t, now, ex, []core.ID{id1, id2}, ex.before(id3.T, 0))
	assertExWrapOut(t, now, ex, []core.ID{id1}, ex.before(id2.T, 0))
	assertExWrapOut(t, now, ex, []core.ID{}, ex.before(id1.T, 0))
}

func TestExWrapCount(t *T) {
	ex := randExWrap()
	id1 := randID(t, false)
	id2 := randID(t, false)
	id3 := randID(t, false)
	now := core.NewTS(time.Now())
	requireExWrapDo(t, now, ex, ex.add(id1, id1.T)...)
	requireExWrapDo(t, now, ex, ex.add(id2, id2.T)...)
	requireExWrapDo(t, now, ex, ex.add(id3, id3.T)...)
	assertExWrapCounts(t, now, ex, []uint64{3}, ex.countNotExpired(now))
	assertExWrapCounts(t, now, ex, []uint64{2}, ex.after(0, 1), ex.countAfterInput())

	id4 := randID(t, true)
	id5 := randID(t, false)
	requireExWrapDo(t, now, ex, ex.add(id4, id4.T)...)
	requireExWrapDo(t, now, ex, ex.add(id5, id5.T)...)
	assertExWrapCounts(t, now, ex, []uint64{4}, ex.countNotExpired(now))
	assertExWrapCounts(t, now, ex, []uint64{4}, ex.after(0, 1), ex.countAfterInput())
}
