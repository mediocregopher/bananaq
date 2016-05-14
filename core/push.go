package core

import (
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/mediocregopher/radix.v2/util"
	"github.com/mediocregopher/wublub"
)

// TODO I'm not super happy about this API. It's kind of gross. Really EventSet
// should be given a new name, probably just ZSet, and then things like Query
// can treat it like an EventSet, but things like this could treat it like a
// WaiterSet.

// EventSetWait returns a channel which will be closed when the given EventSet
// is notified by some other process. stopCh can be closed to stop waiting and
// immediately close the returned channel
func (c *Core) EventSetWait(es EventSet, stopCh <-chan struct{}) <-chan struct{} {
	retCh := make(chan struct{})

	go func() {
		readCh := make(chan wublub.Publish, 1)
		k := es.key()
		c.w.Subscribe(readCh, k)
		select {
		case <-readCh:
		case <-stopCh:
		}
		close(retCh)
		c.w.Unsubscribe(readCh, k)
	}()

	return retCh
}

// EventSetNotify will notify all processes currently waiting on the given
// EventSet using EventSetWait
func (c *Core) EventSetNotify(es EventSet) {
	c.w.Publish(wublub.Publish{Channel: es.key(), Message: "notify"})
}

func (c *Core) esWaitersCmd(now TS, cmd string, es EventSet, args ...interface{}) *redis.Resp {
	es.Subs = append(es.Subs, "waiters")
	k := es.key()

	lua := `
		redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
		local argv = {}
		for i = 2,#ARGV do table.insert(argv, ARGV[i]) end
		return redis.call(unpack(argv))
	`

	return util.LuaEval(c.Cmder, lua, 1, k, now, cmd, k, args)
}

// EventSetStoreWaiter stores that the ID should be included with the rest of
// the waiters in the set for this EventSet. The id will stop being included in
// the set after until.
func (c *Core) EventSetStoreWaiter(es EventSet, id string, now, until TS) error {
	return c.esWaitersCmd(now, "ZADD", es, until, id).Err
}

// EventSetCountWaiters returns the number of waiters currently stored for the
// given EventSet
func (c *Core) EventSetCountWaiters(es EventSet, now TS) (int, error) {
	return c.esWaitersCmd(now, "ZCARD", es).Int()
}
