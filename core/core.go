// Package core is responsible for actually communicating with redis and
// providing an abstraction for the data stored in it
//
// This package is not stable! At present it is only intended to be used by
// other components in bananaq
package core

//go:generate msgp -io=false
//go:generate varembed -pkg core -in query.lua -out query_lua.go -varname queryLua

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/levenlabs/golib/radixutil"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/mediocregopher/radix.v2/util"
	"github.com/tinylib/msgp/msgp"
)

var (
	bpool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 10240)
		},
	}
)

// Cleverly marshal multiple things into a single byte buffer. All the []bytes
// that get put in bb and passed to the callback are actually pointers into the
// same buffer, which gets expanded and put back in the pool.
func withMarshaled(fn func([][]byte), mm ...msgp.Marshaler) {
	b := bpool.Get().([]byte)
	bb := make([][]byte, len(mm))
	var err error
	for i := range mm {
		// at this point len(b) is always 0
		b, err = mm[i].MarshalMsg(b)
		if err != nil {
			panic(err)
		}
		bb[i] = b
		b = b[len(b):]
	}
	fn(bb)
	bpool.Put(b[:0])
}

// Various errors this package may return
var (
	ErrNotFound = errors.New("not found")
)

// Core contains all the information needed to interact with the underlying
// redis instances for bananaq. It can be initialized manually or using New. All
// methods on Core are thread-safe.
type Core struct {
	util.Cmder `msg:"-"`
}

// New initializes a Core struct using the given redis address and pool size.
// The redis address can be a standalone node or a node in a cluster.
func New(redisAddr string, poolSize int) (Core, error) {
	c, err := radixutil.DialMaybeCluster("tcp", redisAddr, poolSize)
	return Core{c}, err
}

const idKey = "id"

// TS identifies a single point in time as an integer number of microseconds
type TS uint64

// NewTS returns a TS corresponding to the given Time (though it may be
// truncated in precision by some small amount).
func NewTS(t time.Time) TS {
	// We unfortunately are stuck using microseconds because redis' builtin lua
	// doesn't handle integers with more than 52-bit precision (supposedly, even
	// though microseconds are more than 52 bits,  but the tests pass so ship
	// it). There is a newer lua which does properly handle this, but redis
	// doesn't use it yet
	return TS(t.UnixNano() / 1e3)
}

// Time returns the Time object this TS corresponds to
func (ts TS) Time() time.Time {
	return time.Unix(0, int64(ts)*1e3)
}

// ID identifies a single event across the entire cluster, and is unique for all
// time
type ID TS

// NewID returns a new, unique ID which can be used for a new event
func (c Core) NewID() (ID, error) {
	return c.newID(NewTS(time.Now()))
}

// We split this out to make testing easier
func (c Core) newID(now TS) (ID, error) {
	lua := `
		local key = KEYS[1]
		local now_raw = ARGV[1]
		local now = cmsgpack.unpack(now_raw)
		local last_raw = redis.call("GET", key)

		if not last_raw then
			redis.call("SET", key, now_raw)
			return now_raw
		end

		local last = cmsgpack.unpack(last_raw)
		if last < now then
			redis.call("SET", key, now_raw)
			return now_raw
		end

		-- Add a microsecond and use that
		last = last + 1
		last_raw = cmsgpack.pack(last)
		redis.call("SET", key, last_raw)
		return last_raw
	`

	var ib []byte
	var err error
	withMarshaled(func(bb [][]byte) {
		nowb := bb[0]
		ib, err = util.LuaEval(c.Cmder, lua, 1, idKey, nowb).Bytes()
	}, now)

	var id ID
	_, err = id.UnmarshalMsg(ib)
	return id, err
}

// Event describes all the information related to a single event. An event is
// immutable, nothing in this struct will ever change
type Event struct {
	ID       ID
	Expire   TS
	Contents string
}

// NewEvent initializes an event struct with the given information, as well as
// creating an ID for the event
func (c Core) NewEvent(expire time.Time, contents string) (Event, error) {
	id, err := c.NewID()
	if err != nil {
		return Event{}, err
	}

	return Event{
		ID:       id,
		Expire:   NewTS(expire),
		Contents: contents,
	}, nil
}

func (e Event) key() string {
	return fmt.Sprintf("event:%d", e.ID)
}

// SetEvent sets the event with the given id to have the given contents. The
// event will expire based on the Expire field in it (which will be truncated to
// an integer) added with the given buffer
func (c Core) SetEvent(e Event, expireBuffer time.Duration) error {
	pexpire := e.Expire.Time().Add(expireBuffer).UnixNano() / 1e6 // to milliseconds
	lua := `
		local key = KEYS[1]
		local pexpire = ARGV[1]
		local val = ARGV[2]
		redis.call("SET", key, val)
		redis.call("PEXPIREAT", key, pexpire)
	`

	var err error
	withMarshaled(func(bb [][]byte) {
		eb := bb[0]
		err = util.LuaEval(c.Cmder, lua, 1, e.key(), pexpire, eb).Err
	}, &e)
	return err
}

// GetEvent returns the event identified by the given id, or ErrNotFound if it's
// expired or never existed
func (c Core) GetEvent(id ID) (Event, error) {
	r := c.Cmd("GET", Event{ID: id}.key())
	if r.IsType(redis.Nil) {
		return Event{}, ErrNotFound
	}

	eb, err := r.Bytes()
	if err != nil {
		return Event{}, err
	}

	var e Event
	_, err = e.UnmarshalMsg(eb)
	return e, err
}

// Events is a wrapper around a slice of Event structs, useful for MessagePack
// encoding/decoding
type Events struct {
	Events []Event
}

// EventSet describes identifying information for a set of events being stored.
// EventSets with the same Base will be stored together and can be interacted
// with transactionally. Subs is used a further set of identifiers for the
// EventSet. EventSets don't actually store the Contents of an Event, but they
// do contain all other fields of an Event.
//
// All strings in EventSet should be alphanumeric, and none should be empty
type EventSet struct {
	Base string
	Subs []string
}

// If this changes remember to change it in Query as well
func (es EventSet) key() string {
	if len(es.Subs) > 0 {
		return fmt.Sprintf("eventset:{%s}:%s", es.Base, strings.Join(es.Subs, ":"))
	}
	return fmt.Sprintf("eventset:{%s}", es.Base)
}

func eventSetFromKey(key string) EventSet {
	var es EventSet
	i, j := strings.Index(key, "{"), strings.Index(key, "}")
	es.Base = key[i+1 : j]
	if len(key) > j+1 {
		es.Subs = strings.Split(key[j+2:], ":")
	}
	return es
}

////////////////////////////////////////////////////////////////////////////////

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

	// May be set instead of Min. The newest ID from the input to this
	// QueryAction will be used as the Min. If the input is empty, Min will be 0
	MinFromInput bool

	// May be set instead of Max. The oldest ID from the input to this
	// QueryAction will be used as the Max. If the input is empty, Max will be 0
	MaxFromInput bool

	// Optional modifiers. If Offset is nonzero, Limit must be nonzero too (it
	// can be -1 to indicate no limit)
	Limit, Offset int64
}

// QuerySelector describes a set of criteria for selecting a set of Events from
// an EventSet. EventSet is a required field, only one field apart from it
// should be set in this selector (unless otherwise noted)
type QuerySelector struct {
	EventSet

	// See QueryEventRangeSelect doc string
	*QueryEventRangeSelect

	// Select Events by their position with the EventSet, using two element
	// slice. 0 is the oldest id, 1 is the second oldest, etc... -1 is the
	// youngest, -2 the second youngest, etc...
	PosRangeSelect []int64

	// Doesn't actually do a query, the output from this selector will simply be
	// these events.
	Events []Event

	// Must be set alongside another field in this Selector. Indicates that
	// instead of discarding the output of the previous QueryAction, its output
	// and the output from this selector should be merged as a union. That union
	// then becomes the output of this QuerySelector.
	Union bool
}

// QueryFilter will apply a filter to its input, only outputting the Events
// which don't match the filter. Only one filter field should be set per
// QueryAction
type QueryFilter struct {
	// If set, only Events which have not expired will be allowed through
	Expired bool

	// May be set alongside any other filter field. Will invert the filter, so
	// that whatever Events would have been allowed through will not be, and
	// vice-versa
	Invert bool
}

// QueryConditional is a field on QueryAction which can affect what the
// QueryAction does. More than one field may be set on this to have multiple
// conditionals.
type QueryConditional struct {
	// Only do the QueryAction if there is an empty input. Otherwise do no
	// action and pass the non-empty input through
	IfNoInput bool

	// Only do the QueryAction if there is non-empty input. Otherwise pass the
	// empty input through
	IfInput bool
}

// QueryAction describes a single action to take on a set of events. Every
// action has an input and an output, which are both always sorted
// chronologically. Only one single field, apart from QueryConditional, should
// be set on a QueryAction.
type QueryAction struct {
	// Selects a set of Events using various types of logic. See the doc on
	// QuerySelector for more. By default, a QuerySelector causes this
	// QueryAction to simply discard its input and output the QuerySelector's
	// output.
	*QuerySelector

	// Adds the input Events to the given EventSets
	AddTo []EventSet

	// Removes the input Events from the given EventSets
	RemoveFrom []EventSet

	// Filters Events out of the input. See its doc string for more info
	*QueryFilter

	// Stops the pipeline of QueryActions and returns the previous output
	Break bool

	// May be set alongside any of the other fields on this struct. See its doc
	// string for more info
	QueryConditional
}

// QueryActions are a set of actions to take sequentially. A set of QueryActions
// effectively amounts to a pipeline of Events. Each QueryAction has the
// potential to output some set of Events, which become the input for the next
// QueryAction. The initial set of events is empty, so the first QueryAction
// should always have a QuerySelector to start things off. The final
// QueryAction's output will be the output set of Events from the Query method.
type QueryActions struct {
	// This must match the Base field on all EventSets being used in this
	// pipeline
	EventSetBase string
	QueryActions []QueryAction
}

// Query performs the given QueryActions pipeline. Whatever the final output
// from the pipeline is is returned.
func (c Core) Query(qas QueryActions) ([]Event, error) {
	var b []byte
	var err error
	withMarshaled(func(bb [][]byte) {
		nowb := bb[0]
		qasb := bb[1]
		key := EventSet{Base: qas.EventSetBase}.key()
		b, err = util.LuaEval(c.Cmder, string(queryLua), 1, key, nowb, qasb).Bytes()
	}, NewTS(time.Now()), &qas)
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
