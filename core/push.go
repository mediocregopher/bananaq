package core

import "github.com/mediocregopher/wublub"

// KeyWait returns a channel which will be closed when the given Key
// is notified by some other process. stopCh can be closed to stop waiting and
// immediately close the returned channel
func (c *Core) KeyWait(k Key, stopCh <-chan struct{}) <-chan struct{} {
	retCh := make(chan struct{})

	go func() {
		readCh := make(chan wublub.Publish, 1)
		c.w.Subscribe(readCh, k.String(c.RedisPrefix))
		select {
		case <-readCh:
		case <-stopCh:
		}
		close(retCh)
		c.w.Unsubscribe(readCh, k.String(c.RedisPrefix))
	}()

	return retCh
}

// KeyNotify will notify all processes currently waiting on the given
// Key using KeyWait
func (c *Core) KeyNotify(k Key) {
	c.w.Publish(wublub.Publish{Channel: k.String(c.RedisPrefix), Message: "notify"})
}
