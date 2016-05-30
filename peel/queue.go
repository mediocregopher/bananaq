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
func queueAvailable(queue string) (exWrap, error) {
	k, err := queueKeyMarshal(core.Key{Base: queue, Subs: []string{"available"}})
	if err != nil {
		return exWrap{}, err
	}
	return newExWrap(k), nil
}

////////////////////////////////////////////////////////////////////////////////

// Keeps track of events that are currently in progress, with scores
// corresponding to the event's ack deadline. Used to timeout in progress events
// and put them in redo
func queueInProgress(queue, cgroup string) (exWrap, error) {
	k, err := queueKeyMarshal(core.Key{Base: queue, Subs: []string{cgroup, "inprogress"}})
	if err != nil {
		return exWrap{}, err
	}
	return newExWrap(k), nil
}

// Keeps track of events which were previously attempted to be processed but
// failed. Score is the event's id
func queueRedo(queue, cgroup string) (exWrap, error) {
	k, err := queueKeyMarshal(core.Key{Base: queue, Subs: []string{cgroup, "redo"}})
	if err != nil {
		return exWrap{}, err
	}
	return newExWrap(k), nil
}

// Single key, used to keep track of newest event retrieved from avail by the
// cgroup
func queuePointer(queue, cgroup string) (core.Key, error) {
	return queueKeyMarshal(core.Key{Base: queue, Subs: []string{cgroup, "ptr"}})
}

func queueCGroupKeys(queue, cgroup string) (exWrap, exWrap, core.Key, error) {
	ewInProg, err := queueInProgress(queue, cgroup)
	if err != nil {
		return exWrap{}, exWrap{}, core.Key{}, err
	}

	ewRedo, err := queueRedo(queue, cgroup)
	if err != nil {
		return exWrap{}, exWrap{}, core.Key{}, err
	}

	keyPtr, err := queuePointer(queue, cgroup)
	if err != nil {
		return exWrap{}, exWrap{}, core.Key{}, err
	}

	return ewInProg, ewRedo, keyPtr, nil
}

////////////////////////////////////////////////////////////////////////////////

// AllQueuesConsumerGroups returns a map whose keys are all the currently known
// queues, and the values are a list of known consumer groups for each queue. A
// queue may have no known consumer groups, but the slice will never be nil.
func (p Peel) AllQueuesConsumerGroups() (map[string][]string, error) {
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

	outm := map[string][]string{}
	for q, cgm := range m {
		outm[q] = make([]string, 0, len(cgm))
		for cg := range cgm {
			outm[q] = append(outm[q], cg)
		}
	}

	return outm, nil
}
