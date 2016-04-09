package core

import (
	. "testing"

	"github.com/levenlabs/golib/timeutil"
	"github.com/stretchr/testify/assert"
)

func init() {
	Init("127.0.0.1:6379", 1)
}

func TestNewID(t *T) {
	now := timeutil.TimestampNow().Float64()
	id, err := newID(now)
	assert.Nil(t, err)
	assert.Equal(t, ID(now), id)

	id2, err := newID(now)
	assert.Nil(t, err)
	assert.True(t, id2 > id)

	now = timeutil.TimestampNow().Float64()
	id3, err := newID(now)
	assert.Nil(t, err)
	assert.True(t, id3 > id2)
}
