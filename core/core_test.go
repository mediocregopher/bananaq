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

func TestKeys(t *T) {
	queueName := testutil.RandStr()
	groupName := testutil.RandStr()

	key := queueKey(queueName)
	assert.Equal(t, queueName, extractQueueName(key))
	key = queueKey(queueName, "foo")
	assert.Equal(t, queueName, extractQueueName(key))

	key = groupKey(queueName, groupName)
	assert.Equal(t, queueName, extractQueueName(key))
	assert.Equal(t, groupName, extractGroupName(key))
	key = groupKey(queueName, groupName, "foo")
	assert.Equal(t, queueName, extractQueueName(key))
	assert.Equal(t, groupName, extractGroupName(key))
}

func requireNewEvent(t *T) Event {
	e, err := NewEvent(time.Now().Add(1*time.Minute), testutil.RandStr())
	require.Nil(t, err)
	return e
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
