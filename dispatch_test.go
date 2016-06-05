package main

import (
	. "testing"
	"time"

	"github.com/levenlabs/golib/timeutil"
	"github.com/stretchr/testify/assert"
)

func TestTimeFromStr(t *T) {
	now := time.Now()
	nowts := timeutil.Timestamp{Time: now}

	ts, err := timeFromStr(now, "30")
	assert.Nil(t, err)
	assert.True(t, now.Add(30*time.Second).Equal(ts))

	next := timeutil.TimestampFromFloat64(nowts.Float64() + 30).String()
	ts, err = timeFromStr(now, "@"+next)
	assert.Nil(t, err)
	assert.True(t, now.Add(29*time.Second).Before(ts))
	assert.True(t, now.Add(31*time.Second).After(ts))
}
