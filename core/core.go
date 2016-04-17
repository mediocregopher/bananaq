// Package core is responsible for actually communicating with redis and
// providing an abstraction for the data stored in it
package core

//go:generate msgp -io=false
//go:generate varembed -pkg core -in query.lua -out query_lua.go -varname queryLua

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/levenlabs/go-llog"
	"github.com/levenlabs/golib/radixutil"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/mediocregopher/radix.v2/util"
	"github.com/tinylib/msgp/msgp"
)

var (
	c     util.Cmder
	pkgKV = llog.KV{"pkg": "core"}

	bpool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 10240)
		},
	}
)

func withMarshaled(m msgp.Marshaler, fn func([]byte)) {
	b := bpool.Get().([]byte)
	defer bpool.Put(b)
	bb, err := m.MarshalMsg(b)
	if err != nil {
		panic(err)
	}
	fn(bb)
}

// Various errors this package may return
var (
	ErrNotFound = errors.New("not found")
)

// Init initializes the core global variables and gets the party started
func Init(redisAddr string, poolSize int) {
	pkgKV["redisAddr"] = redisAddr
	pkgKV["poolSize"] = poolSize
	llog.Info("initializing core", pkgKV, llog.KV{"havingFun": true})

	var err error
	c, err = radixutil.DialMaybeCluster("tcp", redisAddr, poolSize)
	if err != nil {
		llog.Fatal("could not connect to redis", pkgKV, llog.KV{"err": err})
	}
}

const idKey = "id"

// TS identifies a single point in time as an integer number of microseconds
type TS uint64

// NewTS returns a TS corresponding to the given Time (though it may be
// truncated in precision by some small amount).
func NewTS(t time.Time) TS {
	// We unfortunately are stuck using microseconds because lua gets kind of
	// weird at bigger numbers in converting to/from strings
	// TODO we could use msgp instead when interacting with redis
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
func NewID() (ID, error) {
	return newID(NewTS(time.Now()))
}

// We split this out to make testing easier
func newID(now TS) (ID, error) {
	lua := `
		local key = KEYS[1]
		local nowStr = ARGV[1]
		local lastStr = redis.call("GET", key)

		local now  = tonumber(nowStr, 10)
		local last = tonumber(lastStr, 10)

		if not lastStr or not last then
			redis.call("SET", key, nowStr)
			return nowStr
		end

		if last < now then
			redis.call("SET", key, nowStr)
			return nowStr
		end

		-- Add a microsecond and use that
		last = last + 1
		lastStr = string.format("%.0f", last)
		redis.call("SET", key, lastStr)
		return lastStr
	`

	iStr, err := util.LuaEval(c, lua, 1, idKey, now).Str()
	if err != nil {
		return 0, err
	}

	i, err := strconv.ParseInt(iStr, 10, 64)
	return ID(i), err
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
func NewEvent(expire time.Time, contents string) (Event, error) {
	id, err := NewID()
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

// TODO don't use JSON

// MarshalBinary implements the encoding.BinaryMarshaler interface
func (e Event) MarshalBinary() ([]byte, error) {
	return json.Marshal(e)
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface
func (e *Event) UnmarshalBinary(b []byte) error {
	return json.Unmarshal(b, e)
}

// SetEvent sets the event with the given id to have the given contents. The
// event will expire after the given timeout (which will be truncated to an
// integer)
func SetEvent(e Event) error {
	pexpire := e.Expire.Time().UnixNano() / 1e6 // to milliseconds
	// We add 30 seconds to the expire so that the event key is still around for
	// a little while after it's been removed from any queues. This is kind of
	// ghetto
	// TODO maybe don't do this here somehow, it complects this package with the
	// actual bananaq application. It also makes testing kind of annoying
	pexpire += 30000
	lua := `
		local key = KEYS[1]
		local pexpire = ARGV[1]
		local val = ARGV[2]
		redis.call("SET", key, val)
		redis.call("PEXPIREAT", key, pexpire)
	`

	// TODO use something better than json. flatbuffers?
	b, err := e.MarshalBinary()
	if err != nil {
		return err
	}

	return util.LuaEval(c, lua, 1, e.key(), pexpire, b).Err
}

// GetEvent returns the event identified by the given id, or ErrNotFound if it's
// expired or never existed
func GetEvent(id ID) (Event, error) {
	r := c.Cmd("GET", Event{ID: id}.key())
	if r.IsType(redis.Nil) {
		return Event{}, ErrNotFound
	}

	b, err := r.Bytes()
	if err != nil {
		return Event{}, err
	}

	var e Event
	err = e.UnmarshalBinary(b)
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
	} else {
		return fmt.Sprintf("eventset:{%s}", es.Base)
	}
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
