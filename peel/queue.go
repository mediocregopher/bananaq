package peel

import (
	"fmt"
	"strings"

	"github.com/mediocregopher/bananaq/core"
)

// marshal and unmarshall are used to encode/decode the data in a Key so it's
// safe for use in core. What they actually do is not important

func queueKeyMarshal(k core.Key) (core.Key, error) {
	validateKeyPt := func(s string) error {
		if strings.Contains(s, ":") {
			return fmt.Errorf("key part %q contains invalid character ':'", s)
		}
		return nil
	}

	if err := validateKeyPt(k.Base); err != nil {
		return core.Key{}, err
	}

	km := core.Key{
		Base: k.Base,
		Subs: make([]string, len(k.Subs)),
	}
	for i, sub := range k.Subs {
		if err := validateKeyPt(sub); err != nil {
			return core.Key{}, err
		}
		km.Subs[i] = sub
	}
	return km, nil
}

func queueKeyUnmarshal(k core.Key) (core.Key, error) {
	return k, nil
}

// Keeps track of events which are available to be retrieved by any particular
// consumer group, with scores corresponding to the event's id.
func queueAvailableByID(queue string) (core.Key, error) {
	return queueKeyMarshal(core.Key{Base: queue, Subs: []string{"available", "id"}})
}

// Keeps track of events which are available to be retrieved by any particular
// consumer group, with scores corresponding to the event's expire time.
func queueAvailableByExpire(queue string) (core.Key, error) {
	return queueKeyMarshal(core.Key{Base: queue, Subs: []string{"available", "expire"}})
}

func queueAvailableKeys(queue string) (core.Key, core.Key, error) {
	keyAvailID, err := queueAvailableByID(queue)
	if err != nil {
		return core.Key{}, core.Key{}, err
	}
	keyAvailEx, err := queueAvailableByExpire(queue)
	if err != nil {
		return core.Key{}, core.Key{}, err
	}
	return keyAvailID, keyAvailEx, nil
}

////////////////////////////////////////////////////////////////////////////////

// Keeps track of events that are currently in progress, with scores
// corresponding to the event's ack deadline. Used to timeout in progress events
// and put them in redo
func queueInProgressByAck(queue, cgroup string) (core.Key, error) {
	return queueKeyMarshal(core.Key{Base: queue, Subs: []string{cgroup, "inprogress", "ack"}})
}

// Keeps track of events which were previously attempted to be processed but
// failed. Score is the event's id
func queueRedo(queue, cgroup string) (core.Key, error) {
	return queueKeyMarshal(core.Key{Base: queue, Subs: []string{cgroup, "redo"}})
}

// Single key, used to keep track of newest event retrieved from avail by the
// cgroup
func queuePointer(queue, cgroup string) (core.Key, error) {
	return queueKeyMarshal(core.Key{Base: queue, Subs: []string{cgroup, "done"}})
}

// Keeps track of all events currently in use by a cgroup which haven't expired.
// Used in order to clean expired events out of a group
func queueInUseByExpire(queue, cgroup string) (core.Key, error) {
	return queueKeyMarshal(core.Key{Base: queue, Subs: []string{cgroup, "inuse"}})
}

func queueCGroupKeys(queue, cgroup string) ([4]core.Key, error) {
	var ret [4]core.Key
	var err error

	next := func(prev error, fn func(string, string) (core.Key, error)) (core.Key, error) {
		if prev != nil {
			return core.Key{}, prev
		}
		return fn(queue, cgroup)
	}

	ret[0], err = next(err, queueInProgressByAck)
	ret[1], err = next(err, queueRedo)
	ret[2], err = next(err, queuePointer)
	ret[3], err = next(err, queueInUseByExpire)
	return ret, err
}

////////////////////////////////////////////////////////////////////////////////

// AllQueuesConsumerGroups returns a map whose keys are all the currently known
// queues, and the keys for the sub-maps are the known consumer groups for each
// queue.
func (p Peel) AllQueuesConsumerGroups() (map[string]map[string]struct{}, error) {
	kk, err := p.c.KeyScan(core.Key{Base: "*", Subs: []string{"*"}})
	if err != nil {
		return nil, err
	}

	m := map[string]map[string]struct{}{}
	for _, k := range kk {
		if k, err = queueKeyUnmarshal(k); err != nil {
			return nil, err
		}
		if m[k.Base] == nil {
			m[k.Base] = map[string]struct{}{}
		}
		if k.Subs[0] == "available" {
			continue
		}
		m[k.Base][k.Subs[0]] = struct{}{}
	}
	return m, nil
}
