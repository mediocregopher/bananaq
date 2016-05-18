package core

import (
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/mediocregopher/radix.v2/util"
	"github.com/mediocregopher/wublub"
)

// TODO I'm not super happy about this API. It's kind of gross. Really Key
// should be given a new name, probably just ZSet, and then things like Query
// can treat it like an Key, but things like this could treat it like a
// WaiterSet.

// KeyWait returns a channel which will be closed when the given Key
// is notified by some other process. stopCh can be closed to stop waiting and
// immediately close the returned channel
func (c *Core) KeyWait(k Key, stopCh <-chan struct{}) <-chan struct{} {
	retCh := make(chan struct{})

	go func() {
		readCh := make(chan wublub.Publish, 1)
		c.w.Subscribe(readCh, k.String())
		select {
		case <-readCh:
		case <-stopCh:
		}
		close(retCh)
		c.w.Unsubscribe(readCh, k.String())
	}()

	return retCh
}

// KeyNotify will notify all processes currently waiting on the given
// Key using KeyWait
func (c *Core) KeyNotify(k Key) {
	c.w.Publish(wublub.Publish{Channel: k.String(), Message: "notify"})
}

func (c *Core) esWaitersCmd(now TS, cmd string, k Key, args ...interface{}) *redis.Resp {
	k.Subs = append(k.Subs, "waiters")

	lua := `
		redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", ARGV[1])
		local argv = {}
		for i = 2,#ARGV do table.insert(argv, ARGV[i]) end
		return redis.call(unpack(argv))
	`

	return util.LuaEval(c.Cmder, lua, 1, k, now, cmd, k.String(), args)
}

// KeyStoreWaiter stores that the ID should be included with the rest of
// the waiters in the set for this Key. The id will stop being included in
// the set after until.
func (c *Core) KeyStoreWaiter(k Key, id string, now, until TS) error {
	return c.esWaitersCmd(now, "ZADD", k, until, id).Err
}

// KeyCountWaiters returns the number of waiters currently stored for the
// given Key
func (c *Core) KeyCountWaiters(k Key, now TS) (int, error) {
	return c.esWaitersCmd(now, "ZCARD", k).Int()
}
