package peel

import (
	. "testing"
	"time"

	"github.com/levenlabs/golib/testutil"
	"github.com/mediocregopher/bananaq/core"
	"github.com/mediocregopher/radix.v2/pool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestPeel() Peel {
	p, err := pool.New("tcp", "127.0.0.1:6379", 1)
	if err != nil {
		panic(err)
	}

	o := &core.Opts{
		RedisPrefix: testutil.RandStr(),
	}
	peel, err := New(p, o)
	if err != nil {
		panic(err)
	}
	go func() { panic(peel.Run()) }()
	return peel
}

var testPeel = newTestPeel()

func assertKey(t *T, k core.Key, ii ...core.ID) {
	qa := core.QueryActions{
		KeyBase: k.Base,
		QueryActions: []core.QueryAction{
			{
				QuerySelector: &core.QuerySelector{
					Key:              k,
					QueryRangeSelect: &core.QueryRangeSelect{},
				},
			},
		},
	}
	res, err := testPeel.c.Query(qa)
	require.Nil(t, err)

	if len(ii) == 0 {
		assert.Empty(t, res.IDs)
	} else {
		assert.Equal(t, ii, res.IDs)
	}
}

func assertSingleKey(t *T, k core.Key, id core.ID) {
	qa := core.QueryActions{
		KeyBase: k.Base,
		QueryActions: []core.QueryAction{
			{
				SingleGet: &k,
			},
		},
	}
	res, err := testPeel.c.Query(qa)
	require.Nil(t, err)
	require.NotEmpty(t, res.IDs)
	assert.Equal(t, id, res.IDs[0])
}

func TestQAdd(t *T) {
	queue := testutil.RandStr()
	contents := testutil.RandStr()
	id, err := testPeel.QAdd(QAddCommand{
		Queue:    queue,
		Expire:   time.Now().Add(10 * time.Second),
		Contents: contents,
	})
	require.Nil(t, err)
	assert.NotZero(t, id)

	ewAvail, err := queueAvailable(queue)
	require.Nil(t, err)
	assertKey(t, ewAvail.byArb, id)
	assertKey(t, ewAvail.byExp, id)

	e, err := testPeel.c.GetEvent(id)
	require.Nil(t, err)
	assert.Equal(t, contents, e.Contents)
}

// score is optional
func requireAddToKey(t *T, k core.Key, id core.ID, score core.TS) {
	qa := core.QueryActions{
		KeyBase: k.Base,
		QueryActions: []core.QueryAction{
			{
				QuerySelector: &core.QuerySelector{
					IDs: []core.ID{id},
				},
			},
			{
				QueryAddTo: &core.QueryAddTo{
					Keys:  []core.Key{k},
					Score: score,
				},
			},
		},
	}
	_, err := testPeel.c.Query(qa)
	require.Nil(t, err)
}

func requireSetSingleKey(t *T, k core.Key, id core.ID) {
	qa := core.QueryActions{
		KeyBase: k.Base,
		QueryActions: []core.QueryAction{
			{
				QuerySelector: &core.QuerySelector{
					IDs: []core.ID{id},
				},
			},
			{
				QuerySingleSet: &core.QuerySingleSet{
					Key: k,
				},
			},
		},
	}
	_, err := testPeel.c.Query(qa)
	require.Nil(t, err)
}

func newTestQueue(t *T, numIDs int) (string, []core.ID) {
	queue := testutil.RandStr()
	var ii []core.ID
	for i := 0; i < numIDs; i++ {
		id, err := testPeel.QAdd(QAddCommand{
			Queue:    queue,
			Expire:   time.Now().Add(10 * time.Minute),
			Contents: testutil.RandStr(),
		})
		require.Nil(t, err)
		ii = append(ii, id)
	}
	return queue, ii
}

func randID(t *T, expired bool) core.ID {
	now := time.Now()
	nowTS := core.NewTS(now)

	var expireTS core.TS
	if expired {
		expireTS = core.NewTS(now.Add(-10 * time.Second))
	} else {
		expireTS = core.NewTS(now.Add(10 * time.Second))
	}
	e, err := testPeel.c.NewEvent(nowTS, expireTS, "")
	require.Nil(t, err)
	return e.ID
}

func TestQGet(t *T) {
	queue, ii := newTestQueue(t, 6)
	cgroup := testutil.RandStr()

	for i, id := range ii {
		t.Logf("ii[%d]: %d (%#v)", i, id, id)
	}

	keys, err := queueCGroupKeys(queue, cgroup)
	require.Nil(t, err)
	keyInProgAck := keys[0]
	keyRedo := keys[1]
	keyPtr := keys[2]
	keyInUse := keys[3]

	// Test that a "blank" queue gives us its first event
	cmd := QGetCommand{
		Queue:         queue,
		ConsumerGroup: cgroup,
		AckDeadline:   time.Now().Add(1 * time.Second),
	}
	e, err := testPeel.QGet(cmd)
	require.Nil(t, err)
	assert.Equal(t, ii[0], e.ID)
	assertKey(t, keyInProgAck, ii[0])
	assertKey(t, keyInUse, ii[0])

	// Test that a queue with empty done but an inProg returns one after inProg
	e, err = testPeel.QGet(cmd)
	require.Nil(t, err)
	assert.Equal(t, ii[1], e.ID)
	assertKey(t, keyInProgAck, ii[0], ii[1])
	assertKey(t, keyInUse, ii[0], ii[1])

	// Test that empty expire goes straight to done
	cmd.AckDeadline = time.Time{}
	e, err = testPeel.QGet(cmd)
	require.Nil(t, err)
	assert.Equal(t, ii[2], e.ID)
	assertKey(t, keyInProgAck, ii[0], ii[1])
	assertSingleKey(t, keyPtr, ii[2])

	// Test that a queue with an event in done ahead of all events in inProg
	// returns the next one correctly
	e, err = testPeel.QGet(cmd)
	require.Nil(t, err)
	assert.Equal(t, ii[3], e.ID)
	assertKey(t, keyInProgAck, ii[0], ii[1])
	assertSingleKey(t, keyPtr, ii[3])

	// Artificially add two events to redo, make sure they come out in that
	// order immediately
	requireAddToKey(t, keyRedo, ii[4], 0)
	requireAddToKey(t, keyRedo, ii[5], 0)
	assertKey(t, keyRedo, ii[4], ii[5])

	e, err = testPeel.QGet(cmd)
	require.Nil(t, err)
	assert.Equal(t, ii[4], e.ID)
	assertKey(t, keyInProgAck, ii[0], ii[1])
	assertSingleKey(t, keyPtr, ii[4])
	assertKey(t, keyRedo, ii[5])

	e, err = testPeel.QGet(cmd)
	require.Nil(t, err)
	assert.Equal(t, ii[5], e.ID)
	assertKey(t, keyInProgAck, ii[0], ii[1])
	assertSingleKey(t, keyPtr, ii[5])
	assertKey(t, keyRedo) // assert empty

	// At this point the queue has no available events, make sure empty event is
	// returned
	e, err = testPeel.QGet(cmd)
	require.Nil(t, err)
	assert.Equal(t, core.Event{}, e)
	assertKey(t, keyInProgAck, ii[0], ii[1])
	assertSingleKey(t, keyPtr, ii[5])
	assertKey(t, keyRedo) // assert empty

	// Now we're gonna do something mean, and insert an event with an expire
	// which is before the most recent expire in done
	contents := testutil.RandStr()
	expire := ii[5].Expire.Time().Add(-5 * time.Second)
	id, err := testPeel.QAdd(QAddCommand{
		Queue:    queue,
		Expire:   expire,
		Contents: contents,
	})
	require.Nil(t, err)
	e, err = testPeel.QGet(cmd)
	require.Nil(t, err)
	assert.Equal(t, core.Event{ID: id, Contents: contents}, e)
	assertKey(t, keyInProgAck, ii[0], ii[1])
	assertSingleKey(t, keyPtr, id)
	assertKey(t, keyRedo) // assert empty

	// Insert an event that's already expired in redo, and make sure it doesn't
	// come back
	// TODO uncomment
	//exRedo := randID(t, true)
	//requireAddToKey(t, keyRedo, exRedo, 0)
	//e, err = testPeel.QGet(cmd)
	//require.Nil(t, err)
	//assert.Equal(t, core.Event{}, e)
	//assertKey(t, keyInProgAck, ii[0], ii[1])
	//assertSingleKey(t, keyPtr, id)
	//assertKey(t, keyRedo, exRedo)
}

func TestQGetBlocking(t *T) {
	queue, ii := newTestQueue(t, 1)
	cgroup := testutil.RandStr()

	cmd := QGetCommand{
		Queue:         queue,
		ConsumerGroup: cgroup,
		BlockUntil:    time.Now().Add(1 * time.Second),
	}

	assertBlockFor := func(d time.Duration) core.Event {
		ch := make(chan core.Event)
		go func() {
			e, err := testPeel.QGet(cmd)
			require.Nil(t, err)
			ch <- e
		}()

		if d > 0 {
			select {
			case <-time.After(d):
			case <-ch:
				assert.Fail(t, "didn't block long enough")
			}
		}
		select {
		case <-time.After(100 * time.Millisecond):
			assert.Fail(t, "blocked too long")
		case e := <-ch:
			return e
		}
		return core.Event{}
	}

	e := assertBlockFor(0)
	assert.Equal(t, ii[0], e.ID)

	cmd.BlockUntil = time.Now().Add(1*time.Second + 10*time.Millisecond)
	e = assertBlockFor(1 * time.Second)
	assert.Equal(t, core.Event{}, e)

	cmd.BlockUntil = time.Now().Add(1 * time.Second)
	e2ch := make(chan core.Event)
	go func() {
		time.Sleep(500 * time.Millisecond)
		contents := testutil.RandStr()
		expire := core.NewTS(time.Now().Add(10 * time.Minute))
		id, err := testPeel.QAdd(QAddCommand{
			Queue:    queue,
			Expire:   expire.Time(),
			Contents: contents,
		})
		require.Nil(t, err)
		e2ch <- core.Event{ID: id, Contents: contents}
	}()
	e = assertBlockFor(500 * time.Millisecond)
	e2 := <-e2ch
	assert.Equal(t, e2, e)
}

func TestQAck(t *T) {
	queue, ii := newTestQueue(t, 2)
	cgroup := testutil.RandStr()

	keys, err := queueCGroupKeys(queue, cgroup)
	require.Nil(t, err)
	keyInProgAck := keys[0]
	keyPtr := keys[2]

	ackDeadline := core.NewTS(ii[0].T.Time().Add(10 * time.Millisecond))
	requireAddToKey(t, keyInProgAck, ii[0], ackDeadline)

	cmd := QAckCommand{
		Queue:         queue,
		ConsumerGroup: cgroup,
		EventID:       ii[0],
	}
	acked, err := testPeel.QAck(cmd)
	require.Nil(t, err)
	assert.True(t, acked)
	assertKey(t, keyInProgAck)
	assertSingleKey(t, keyPtr, ii[0])

	acked, err = testPeel.QAck(cmd)
	require.Nil(t, err)
	assert.False(t, acked)
	assertKey(t, keyInProgAck)
	assertSingleKey(t, keyPtr, ii[0])

	ackDeadline = core.NewTS(ii[1].T.Time().Add(-10 * time.Millisecond))
	requireAddToKey(t, keyInProgAck, ii[1], ackDeadline)

	cmd.EventID = ii[1]
	acked, err = testPeel.QAck(cmd)
	require.Nil(t, err)
	assert.False(t, acked)
	assertKey(t, keyInProgAck, ii[1])
	assertSingleKey(t, keyPtr, ii[0])
}

func TestClean(t *T) {
	queue := testutil.RandStr()
	cgroup := testutil.RandStr()

	keys, err := queueCGroupKeys(queue, cgroup)
	require.Nil(t, err)
	keyInProgAck := keys[0]
	keyRedo := keys[1]
	keyInUse := keys[3]

	now := time.Now()

	// in progress, has neither expired nor missed its deadline
	ii0 := randID(t, false)
	requireAddToKey(t, keyInProgAck, ii0, core.NewTS(now.Add(1*time.Second)))
	requireAddToKey(t, keyInUse, ii0, ii0.Expire)

	// in progress, missed its deadline
	ii1 := randID(t, false)
	requireAddToKey(t, keyInProgAck, ii1, core.NewTS(now.Add(-10*time.Millisecond)))
	requireAddToKey(t, keyInUse, ii1, ii1.Expire)

	// in progress, expired
	ii2 := randID(t, true)
	requireAddToKey(t, keyInProgAck, ii2, core.NewTS(now.Add(-10*time.Millisecond)))
	requireAddToKey(t, keyInUse, ii2, ii2.Expire)

	// in redo, not expired
	ii3 := randID(t, false)
	requireAddToKey(t, keyRedo, ii3, 0)
	requireAddToKey(t, keyInUse, ii3, ii3.Expire)

	// in redo, expired
	ii4 := randID(t, true)
	requireAddToKey(t, keyRedo, ii4, 0)
	requireAddToKey(t, keyInUse, ii4, ii4.Expire)

	// in use (so really just done), not expired
	ii5 := randID(t, false)
	requireAddToKey(t, keyInUse, ii5, ii5.Expire)

	// in use (so really just done), expired
	ii6 := randID(t, true)
	requireAddToKey(t, keyInUse, ii6, ii6.Expire)

	require.Nil(t, testPeel.Clean(queue, cgroup))
	assertKey(t, keyInProgAck, ii0)
	assertKey(t, keyRedo, ii1, ii3)
	assertKey(t, keyInUse, ii0, ii1, ii3, ii5)
}

func TestCleanAvailable(t *T) {
	queue := testutil.RandStr()

	ewAvail, err := queueAvailable(queue)
	require.Nil(t, err)

	ii0 := randID(t, false)
	requireAddToKey(t, ewAvail.byArb, ii0, 0)
	requireAddToKey(t, ewAvail.byExp, ii0, ii0.Expire)
	ii1 := randID(t, true)
	requireAddToKey(t, ewAvail.byArb, ii1, 0)
	requireAddToKey(t, ewAvail.byExp, ii1, ii1.Expire)
	ii2 := randID(t, false)
	requireAddToKey(t, ewAvail.byArb, ii2, 0)
	requireAddToKey(t, ewAvail.byExp, ii2, ii2.Expire)
	ii3 := randID(t, true)
	requireAddToKey(t, ewAvail.byArb, ii3, 0)
	requireAddToKey(t, ewAvail.byExp, ii3, ii3.Expire)

	require.Nil(t, testPeel.CleanAvailable(queue))
	assertKey(t, ewAvail.byArb, ii0, ii2)
}

func TestQStatus(t *T) {
	queue, ii := newTestQueue(t, 6)
	cg1 := testutil.RandStr()
	cg2 := testutil.RandStr()

	keys, err := queueCGroupKeys(queue, cg1)
	require.Nil(t, err)
	keyInProgAck := keys[0]
	keyRedo := keys[1]
	keyPtr := keys[2]

	requireAddToKey(t, keyInProgAck, ii[0], core.NewTS(time.Now().Add(1*time.Minute)))
	requireAddToKey(t, keyInProgAck, ii[1], core.NewTS(time.Now().Add(1*time.Minute)))
	requireSetSingleKey(t, keyPtr, ii[2])
	requireAddToKey(t, keyRedo, ii[2], 0)

	cmd := QStatusCommand{
		QueuesConsumerGroups: map[string][]string{
			queue: []string{cg1, cg2},
		},
	}
	qsm, err := testPeel.QStatus(cmd)
	require.Nil(t, err)

	expected := map[string]QueueStats{
		queue: QueueStats{
			Total: 6,
			ConsumerGroupStats: map[string]ConsumerGroupStats{
				cg1: ConsumerGroupStats{
					InProgress: 2,
					Redo:       1,
					Available:  3,
				},
				cg2: ConsumerGroupStats{
					Available: 6,
				},
			},
		},
	}
	assert.Equal(t, expected, qsm)

	lines, err := testPeel.QInfo(cmd)
	require.Nil(t, err)
	// We don't need to try and assert what the lines are, but just print them
	// out so I might notice if they get wonky somehow
	for _, line := range lines {
		t.Log(line)
	}
}
