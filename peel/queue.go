package peel

import (
	"encoding/hex"

	"github.com/mediocregopher/bananaq/core"
)

// TODO this hex encoding thing makes debugging a lot harder, would be nice to
// not have to do it

func queueKeyMarshal(k core.Key) core.Key {
	return k
	km := core.Key{
		Base: hex.EncodeToString([]byte(k.Base)),
		Subs: make([]string, len(k.Subs)),
	}
	for i, sub := range k.Subs {
		km.Subs[i] = hex.EncodeToString([]byte(sub))
	}
	return km
}

func queueKeyUnmarshal(k core.Key) core.Key {
	return k
	b, _ := hex.DecodeString(k.Base)
	subs := make([]string, len(k.Subs))
	for i, sub := range k.Subs {
		subum, _ := hex.DecodeString(sub)
		subs[i] = string(subum)
	}
	return core.Key{Base: string(b), Subs: subs}
}

// Keeps track of events which are available to be retrieved by any particular
// consumer group, with scores corresponding to the event's id.
func queueAvailableByID(queue string) core.Key {
	return queueKeyMarshal(core.Key{Base: queue, Subs: []string{"available", "id"}})
}

// Keeps track of events which are available to be retrieved by any particular
// consumer group, with scores corresponding to the event's expire time.
func queueAvailableByExpire(queue string) core.Key {
	return queueKeyMarshal(core.Key{Base: queue, Subs: []string{"available", "expire"}})
}

// Keeps track of events that are currently in progress, with scores
// corresponding to the event's ack deadline. Used to timeout in progress events
// and put them in redo
func queueInProgressByAck(queue, cgroup string) core.Key {
	return queueKeyMarshal(core.Key{Base: queue, Subs: []string{cgroup, "inprogress", "ack"}})
}

// Keeps track of events which were previously attempted to be processed but
// failed. Score is the event's id
func queueRedo(queue, cgroup string) core.Key {
	return queueKeyMarshal(core.Key{Base: queue, Subs: []string{cgroup, "redo"}})
}

// Single key, used to keep track of newest event retrieved from avail by the
// cgroup
func queuePointer(queue, cgroup string) core.Key {
	return queueKeyMarshal(core.Key{Base: queue, Subs: []string{cgroup, "done"}})
}

// Keeps track of all events currently in use by a cgroup which haven't expired.
// Used in order to clean expired events out of a group
func queueInUseByExpire(queue, cgroup string) core.Key {
	return queueKeyMarshal(core.Key{Base: queue, Subs: []string{cgroup, "inuse"}})
}

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
		k = queueKeyUnmarshal(k)
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
