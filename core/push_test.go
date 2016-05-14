package core

import (
	. "testing"
	"time"

	"github.com/levenlabs/golib/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventSetWait(t *T) {
	// Make sure notifying on EventSets which don't have waiters is fine
	base := testutil.RandStr()
	es1 := randEventSet(base)
	es2 := randEventSet(base)
	testCore.EventSetNotify(es1)
	testCore.EventSetNotify(es2)

	// This test shouldn't take too long
	go func() {
		time.Sleep(5 * time.Second)
		panic("test took too long")
	}()

	ch1 := make(chan bool, 1)
	go func() {
		<-testCore.EventSetWait(es1, nil)
		ch1 <- true
	}()

	ch2 := make(chan bool, 1)
	go func() {
		<-testCore.EventSetWait(es1, nil)
		ch2 <- true
	}()

	ch3 := make(chan bool, 1)
	go func() {
		<-testCore.EventSetWait(es2, nil)
		ch3 <- true
	}()

	ch4 := make(chan bool, 1)
	ch4stop := make(chan struct{})
	go func() {
		<-testCore.EventSetWait(randEventSet(base), ch4stop)
		ch4 <- true
	}()

	time.Sleep(100 * time.Millisecond)

	assertBlocking := func(ch chan bool) {
		select {
		case <-ch:
			assert.Fail(t, "channel should be blocking")
		default:
		}
	}

	assertNotBlocking := func(ch chan bool) {
		select {
		case b := <-ch:
			assert.True(t, b)
		case <-time.After(100 * time.Millisecond):
			assert.Fail(t, "channel should not be blocking")
		}
	}

	assertBlocking(ch1)
	assertBlocking(ch2)
	assertBlocking(ch3)
	assertBlocking(ch4)

	testCore.EventSetNotify(es1)
	assertNotBlocking(ch1)
	assertNotBlocking(ch2)
	assertBlocking(ch3)
	assertBlocking(ch4)

	testCore.EventSetNotify(es2)
	assertNotBlocking(ch3)
	assertBlocking(ch4)

	close(ch4stop)
	assertNotBlocking(ch4)
}

func TestStoreWaiters(t *T) {
	id1 := testutil.RandStr()
	id2 := testutil.RandStr()
	es := randEventSet(testutil.RandStr())

	now := NewTS(time.Now())
	assertCount := func(c int) {
		cc, err := testCore.EventSetCountWaiters(es, now)
		require.Nil(t, err)
		assert.Equal(t, c, cc)
	}
	assertCount(0)

	now = NewTS(time.Now())
	err := testCore.EventSetStoreWaiter(es, id1, now, NewTS(time.Now().Add(1*time.Second)))
	require.Nil(t, err)
	err = testCore.EventSetStoreWaiter(es, id2, now, NewTS(time.Now().Add(5*time.Second)))
	require.Nil(t, err)
	assertCount(2)

	now = NewTS(time.Now().Add(2 * time.Second))
	assertCount(1)

	now = NewTS(time.Now().Add(10 * time.Second))
	assertCount(0)
}
