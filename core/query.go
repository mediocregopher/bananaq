package core

import (
	"time"

	"github.com/mediocregopher/radix.v2/util"
)

//go:generate msgp -io=false
//go:generate varembed -pkg core -in query.lua -out query_lua.go -varname queryLua

// wherein I develop what amounts to a DSL for working with multiple EventSet's
// contents transactionally

// QueryEventRangeSelect is used to select all Events within the given range
// from an EventSet. If MinExcl is true, Min itself will be excluded from the
// return if it's in the set (and similarly for MaxExcl/Max). If Min or Max are
// 0 that indicates -infinity or +infinity, respectively
type QueryEventRangeSelect struct {
	Min              ID
	Max              ID
	MinExcl, MaxExcl bool

	// May be set instead of Min. The newest ID from the result set of
	// MinQuerySelector will be used as the Min. If the result set of
	// MinQuerySelector is empty, Min is 0.
	MinQuerySelector *QuerySelector

	// May be set instead of Max. The oldest ID from the result set of
	// MaxQuerySelector will be used as the Max. If the result set of
	// MaxQuerySelector is empty, Max is 0.
	MaxQuerySelector *QuerySelector

	// Optional modifiers. If Offset is nonzero, Limit must be nonzero too (it
	// can be -1 to indicate no limit)
	Limit, Offset int64
}

// QuerySelector describes a set of criteria for selecting a set of Events from
// an EventSet. EventSet is a required field, only one field apart from it
// should be set in this selector (except when indicated on the field comment
// that the field goes with another field)
//
// A QuerySelector always has a "resulting set", i.e. the set of Events which
// match the selector on the EventSet. The resulting set may be empty. A
// QuerySelector never modifies anything, it simply retrieves a set of Events
// (sans their Contents). By default, The resulting set is automatically
// filtered to only contain Events which haven't yet expired
type QuerySelector struct {
	EventSet

	// See QueryEventRangeSelect doc string
	*QueryEventRangeSelect

	// Select Events by their position with the EventSet, using two element
	// slice. 0 is the oldest id, 1 is the second oldest, etc... -1 is the
	// youngest, -2 the second youngest, etc...
	PosRangeSelect []int64

	// Performs all the given selectors, and picks the one whose resulting set
	// has the newest Event in it. That resulting set becomes the resulting set
	// for this selector. If all resulting sets are empty, this selector's
	// resulting set will be empty
	Newest []QuerySelector

	// Performs the given selectors one by one, and the first resulting set
	// which isn't empty becomes the resulting set of this selector
	FirstNotEmpty []QuerySelector

	// Doesn't actually do a query, the resulting set from this selector will
	// simply be these events.
	Events []Event

	// If true, only keep the Events in this result set which have expired. This
	// should be set alongside another field in this struct.
	FilterNotExpired bool
}

// QueryActions describes what to actually do with the results of a
// QuerySelector. The QuerySelector field must be filled in, as well as one or
// more of the other fields.
type QueryAction struct {
	QuerySelector

	AddTo      []EventSet
	RemoveFrom []EventSet
}

func Query(qa QueryAction) ([]Event, error) {
	var b []byte
	var err error
	withMarshaled(&qa, func(qab []byte) {
		b, err = util.LuaEval(c, string(queryLua), 1, qa.EventSet.key(), NewTS(time.Now()), qab).Bytes()
	})
	if err != nil {
		return nil, err
	}

	var ee Events
	if _, err := ee.UnmarshalMsg(b); err != nil {
		return nil, err
	}
	if ee.Events == nil {
		ee.Events = []Event{}
	}

	return ee.Events, nil
}
