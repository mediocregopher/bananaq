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

func requireNewID(t *T) ID {
	id, err := NewID()
	require.Nil(t, err)
	return id
}

func TestGetSetEvent(t *T) {
	id := requireNewID(t)
	contents := testutil.RandStr()

	assert.Nil(t, SetEvent(id, contents, 1))
	contents2, err := GetEvent(id)
	assert.Nil(t, err)
	assert.Equal(t, contents, contents2)

	time.Sleep(1*time.Second + 100*time.Millisecond)
	_, err = GetEvent(id)
	assert.Equal(t, ErrNotFound, err)
}
