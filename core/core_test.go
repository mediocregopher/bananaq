package core

import (
	. "testing"
	"time"

	"github.com/levenlabs/golib/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	Init("127.0.0.1:6379", 1)
}

func TestNewID(t *T) {
	for i := 0; i < 100; i++ {
		now := time.Now()
		nowI := newIDBase(now)
		id, err := newID(nowI)
		assert.Nil(t, err)
		assert.Equal(t, ID(nowI), id)

		// make sure at least the second precision matches, the actual times
		// won't match exactly because we truncate to microseconds
		assert.Equal(t, now.Unix(), id.Time().Unix())

		id2, err := newID(nowI)
		assert.Nil(t, err)
		assert.True(t, id2 > id, "id2:%s !> id:%s ", id2, id)

		nowI = newIDBase(time.Now())
		id3, err := newID(nowI)
		assert.Nil(t, err)
		assert.True(t, id3 > id2, "id3:%s !> id2:%s ", id3, id2)
	}
}

func requireNewEvent(t *T) Event {
	e, err := NewEvent(time.Now().Add(1*time.Minute), testutil.RandStr())
	require.Nil(t, err)
	return e
}

func randEventSet(base string) EventSet {
	return EventSet{
		Base: base,
		Subs: []string{testutil.RandStr()},
	}
}

func TestGetSetEvent(t *T) {
	contents := testutil.RandStr()
	// This is a terrible hack that we have to do because we're adding 30 to the
	// expire internally
	expire := time.Now().Add(-29 * time.Second)

	e, err := NewEvent(expire, contents)
	require.Nil(t, err)

	assert.Nil(t, SetEvent(e))
	e2, err := GetEvent(e.ID)
	assert.Nil(t, err)
	assert.Equal(t, e, e2)

	time.Sleep(1*time.Second + 100*time.Millisecond)
	_, err = GetEvent(e.ID)
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

func assertSetEvents(t *T, es EventSet, ee ...Event) {
	ee2, err := GetIDRange(es, 0, -1)
	require.Nil(t, err)

	expected := map[ID]bool{}
	for i := range ee {
		expected[ee[i].ID] = true
	}

	found := map[ID]bool{}
	for i := range ee2 {
		found[ee2[i].ID] = true
	}

	assert.Equal(t, expected, found)
}

func TestAddRemove(t *T) {
	base := testutil.RandStr()
	es1, es2, es3 := randEventSet(base), randEventSet(base), randEventSet(base)
	e1, e2, e3 := requireNewEvent(t), requireNewEvent(t), requireNewEvent(t)

	require.Nil(t, AddRemove(e1, []EventSet{es1, es2}, nil))
	require.Nil(t, AddRemove(e2, []EventSet{es2, es3}, nil))
	require.Nil(t, AddRemove(e3, []EventSet{es1, es3}, nil))
	assertSetEvents(t, es1, e1, e3)
	assertSetEvents(t, es2, e1, e2)
	assertSetEvents(t, es3, e2, e3)

	require.Nil(t, AddRemove(e1, []EventSet{es3}, []EventSet{es1, es2}))
	assertSetEvents(t, es1, e3)
	assertSetEvents(t, es2, e2)
	assertSetEvents(t, es3, e1, e2, e3)

	require.Nil(t, AddRemove(e2, nil, []EventSet{es2, es3}))
	assertSetEvents(t, es1, e3)
	assertSetEvents(t, es2)
	assertSetEvents(t, es3, e1, e3)
}
