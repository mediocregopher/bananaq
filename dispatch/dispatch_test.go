package dispatch

import (
	. "testing"
	"time"

	"github.com/levenlabs/go-llog"
	"github.com/levenlabs/golib/testutil"
	"github.com/mediocregopher/bananaq/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	llog.SetLevel(llog.DebugLevel)
	core.Init("127.0.0.1:6379", 1)
	Init(1)
}

func testCmd(cmd string, args ...string) interface{} {
	return Dispatch(Command{
		ClientID: "test",
		Command:  cmd,
		Args:     args,
	})
}

func assertCmd(t *T, expected interface{}, cmd string, args ...string) {
	assert.Equal(t, expected, testCmd(cmd, args...))
}

func eventSetElems(t *T, es core.EventSet) []core.Event {
	qa := core.QueryAction{
		QuerySelector: core.QuerySelector{
			EventSet:              es,
			QueryEventRangeSelect: &core.QueryEventRangeSelect{},
		},
	}
	ee, err := core.Query(qa)
	require.Nil(t, err)
	return ee
}

func assertEventSet(t *T, es core.EventSet, ids ...core.ID) {
	ee := eventSetElems(t, es)
	eeIDs := map[core.ID]bool{}
	for _, e := range ee {
		eeIDs[e.ID] = true
	}
	assert.Len(t, eeIDs, len(ids))
	for _, id := range ids {
		assert.Contains(t, eeIDs, id)
	}
}

func TestPing(t *T) {
	assertCmd(t, "PONG", "PING")
}

func TestQAdd(t *T) {
	queue := testutil.RandStr()
	contents := testutil.RandStr()
	id := testCmd("QADD", queue, "10", contents).(core.ID)
	assert.NotEmpty(t, id)
	assertEventSet(t, queueAvailable(queue), id)

	e, err := core.GetEvent(id)
	require.Nil(t, err)
	assert.Equal(t, contents, e.Contents)
}

func TestQAddNoBlock(t *T) {
	queue := testutil.RandStr()
	contents := testutil.RandStr()
	assert.Equal(t, "OK", testCmd("QADD", queue, "10", contents, "NOBLOCK"))

	time.Sleep(100 * time.Millisecond)
	ee := eventSetElems(t, queueAvailable(queue))
	assert.NotEmpty(t, ee)
}
