package peel

import "github.com/mediocregopher/bananaq/core"

// TODO need to make sure everything all fields here are alphanumeric, or maybe
// change the separator in redis to be \0 or something

// Keeps track of events which are available to be retrieved by any particular
// consumer group, with scores corresponding to the event's id.
func queueAvailableByID(queue string) core.Key {
	return core.Key{Base: queue, Subs: []string{"available", "id"}}
}

// Keeps track of events which are available to be retrieved by any particular
// consumer group, with scores corresponding to the event's expire time.
func queueAvailableByExpire(queue string) core.Key {
	return core.Key{Base: queue, Subs: []string{"available", "expire"}}
}

// Keeps track of events that are currently in progress, with scores
// corresponding to the event's id. Used to retrieve the next available event in
// a queue
func queueInProgressByID(queue, cgroup string) core.Key {
	return core.Key{Base: queue, Subs: []string{cgroup, "inprogress", "id"}}
}

// Keeps track of events that are currently in progress, with scores
// corresponding to the event's ack deadline. Used to timeout in progress events
// and put them in redo
func queueInProgressByAck(queue, cgroup string) core.Key {
	return core.Key{Base: queue, Subs: []string{cgroup, "inprogress", "ack"}}
}

// Keeps track of events which were previously attempted to be processed but
// failed. Score is the event's id
func queueRedo(queue, cgroup string) core.Key {
	return core.Key{Base: queue, Subs: []string{cgroup, "redo"}}
}

// Keeps track of events which have been successfully processed. Score is the
// event's id
func queueDone(queue, cgroup string) core.Key {
	return core.Key{Base: queue, Subs: []string{cgroup, "done"}}
}

// Keeps track of all events currently in use by a cgroup which haven't expired.
// Used in order to clean expired events out of a group
func queueInUseByExpire(queue, cgroup string) core.Key {
	return core.Key{Base: queue, Subs: []string{cgroup, "inuse"}}
}
