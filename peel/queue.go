package peel

import "github.com/mediocregopher/bananaq/core"

// TODO need to make sure everything all fields here are alphanumeric, or maybe
// change the separator in redis to be \0 or something

// Keeps track of events which are available to be retrieved by any particular
// consumer group
func queueAvailable(queue string) core.EventSet {
	return core.EventSet{Base: queue, Subs: []string{"available"}}
}

// Keeps track of events that are currently in progress, with scores
// corresponding to the event's id. Used to retrieve the next available event in
// a queue
func queueInProgressByID(queue, cgroup string) core.EventSet {
	return core.EventSet{Base: queue, Subs: []string{cgroup, "inprogress", "id"}}
}

// Keeps track of events that are currently in progress, with scores
// corresponding to the event's ack deadline. Used to timeout in progress events
// and put them in redo
func queueInProgressByAck(queue, cgroup string) core.EventSet {
	return core.EventSet{Base: queue, Subs: []string{cgroup, "inprogress", "ack"}}
}

// Keeps track of events which were previously attempted to be processed but
// failed. Score is the event's id
func queueRedo(queue, cgroup string) core.EventSet {
	return core.EventSet{Base: queue, Subs: []string{cgroup, "redo"}}
}

// Keeps track of events which have been successfully processed. Score is the
// event's id
func queueDone(queue, cgroup string) core.EventSet {
	return core.EventSet{Base: queue, Subs: []string{cgroup, "done"}}
}
