package peel

import (
	"encoding/hex"

	"github.com/mediocregopher/bananaq/core"
)

func queueNameClean(queue, cgroup string) (string, string) {
	return hex.EncodeToString([]byte(queue)), hex.EncodeToString([]byte(cgroup))
}

// Keeps track of events which are available to be retrieved by any particular
// consumer group, with scores corresponding to the event's id.
func queueAvailableByID(queue string) core.Key {
	queue, _ = queueNameClean(queue, "")
	return core.Key{Base: queue, Subs: []string{"available", "id"}}
}

// Keeps track of events which are available to be retrieved by any particular
// consumer group, with scores corresponding to the event's expire time.
func queueAvailableByExpire(queue string) core.Key {
	queue, _ = queueNameClean(queue, "")
	return core.Key{Base: queue, Subs: []string{"available", "expire"}}
}

// Keeps track of events that are currently in progress, with scores
// corresponding to the event's id. Used to retrieve the next available event in
// a queue
func queueInProgressByID(queue, cgroup string) core.Key {
	queue, cgroup = queueNameClean(queue, cgroup)
	return core.Key{Base: queue, Subs: []string{cgroup, "inprogress", "id"}}
}

// Keeps track of events that are currently in progress, with scores
// corresponding to the event's ack deadline. Used to timeout in progress events
// and put them in redo
func queueInProgressByAck(queue, cgroup string) core.Key {
	queue, cgroup = queueNameClean(queue, cgroup)
	return core.Key{Base: queue, Subs: []string{cgroup, "inprogress", "ack"}}
}

// Keeps track of events which were previously attempted to be processed but
// failed. Score is the event's id
func queueRedo(queue, cgroup string) core.Key {
	queue, cgroup = queueNameClean(queue, cgroup)
	return core.Key{Base: queue, Subs: []string{cgroup, "redo"}}
}

// Keeps track of events which have been successfully processed. Score is the
// event's id
func queueDone(queue, cgroup string) core.Key {
	queue, cgroup = queueNameClean(queue, cgroup)
	return core.Key{Base: queue, Subs: []string{cgroup, "done"}}
}

// Keeps track of all events currently in use by a cgroup which haven't expired.
// Used in order to clean expired events out of a group
func queueInUseByExpire(queue, cgroup string) core.Key {
	queue, cgroup = queueNameClean(queue, cgroup)
	return core.Key{Base: queue, Subs: []string{cgroup, "inuse"}}
}
