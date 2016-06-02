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
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mediocregopher/radix.v2/cluster"
	"github.com/mediocregopher/radix.v2/pool"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/mediocregopher/radix.v2/util"
	"github.com/mediocregopher/wublub"
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

// Opts are extra configuration fields which may be set on Core
type Opts struct {
	// Default 10. Number of threads which may publish simultaneously.
	NumPublishers int

	// Default "bananaq". String to prefix all redis keys with
	RedisPrefix string
}

// Core contains all the information needed to interact with the underlying
// redis instances for bananaq. All methods on Core are thread-safe, except Run
// which should only be run by a single goroutine at any time.
type Core struct {
	w *wublub.Wublub

	// Required. Must either be a pool or a cluster instance.
	util.Cmder `msg:"-"`

	// Optional. If given, any errors encountered by Run will be written to it.
	// Should be buffered by at least 1
	ErrCh chan error

	// Optional. If given, may be later closed by any process to stop a Run
	StopCh chan struct{}

	// Extra options you usually don't have to worry about
	Opts
}

// Run performs all the background work needed to support Core. It spawns a
// background go-routine which does the actual work, and which can be stopped
// with the StopCh. If the background goroutine encounters an error then the
// error will be written to ErrCh (if set) and then the goroutine will stop.
// After that Run may be called again to retry.
func (c *Core) Run() error {
	if c.NumPublishers == 0 {
		c.NumPublishers = 10
	}
	if c.RedisPrefix == "" {
		c.RedisPrefix = "bananaq"
	}

	var df pool.DialFunc
	if cl, ok := c.Cmder.(*cluster.Cluster); ok {
		rand := rand.New(rand.NewSource(time.Now().UnixNano()))
		df = func(_, _ string) (*redis.Client, error) {
			istr := strconv.Itoa(rand.Int())
			return cl.GetForKey(istr)
		}
	} else {
		cp := c.Cmder.(*pool.Pool)
		df = func(_, _ string) (*redis.Client, error) {
			return cp.Get()
		}
	}

	p, err := pool.NewCustom("", "", c.NumPublishers, df)
	if err != nil {
		return err
	}

	c.w = wublub.New(wublub.Opts{Pool: p})
	cwErrCh := make(chan error, 1)
	go func() { cwErrCh <- c.w.Run() }()

	go func() {
		select {
		case err := <-cwErrCh:
			if c.ErrCh != nil {
				c.ErrCh <- err
			}
		case <-c.StopCh:
		}

		c.w.Close()
		p.Empty()
	}()

	return nil
}

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

// String returns the string form of a TS
func (ts TS) String() string {
	return strconv.FormatUint(uint64(ts), 10)
}

// MonoTS returns a new, unique TS corresponding to the given timestamp All
// returns are monotonically increasing across the entire cluster. Consequently,
// the TS returned might differ in the time it represents from the given TS by a
// very small amount (or a big amount, if the given time is way in the past).
func (c Core) MonoTS(t TS) (TS, error) {
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

	idKey := c.RedisPrefix + ":monots"

	var ib []byte
	var err error
	withMarshaled(func(bb [][]byte) {
		nowb := bb[0]
		ib, err = util.LuaEval(c.Cmder, lua, 1, idKey, nowb).Bytes()
	}, t)

	var t2 TS
	_, err = t2.UnmarshalMsg(ib)
	return t2, err
}

// ID identifies a single event across the entire cluster, and is unique for all
// time. It also represents the point in time at which an event will expire
type ID struct {
	T      TS
	Expire TS
}

// String returns the string form of the ID
func (id ID) String() string {
	return fmt.Sprintf("%d_%d", id.T, id.Expire)
}

// IDFromString takes a string previously returned by an ID's String method and
// returns the corresponding ID for it
func IDFromString(idstr string) (ID, error) {
	p := strings.SplitN(idstr, "_", 2)
	if len(p) != 2 {
		return ID{}, errors.New("invalid ID string")
	}
	tI, err := strconv.ParseUint(p[0], 10, 64)
	if err != nil {
		return ID{}, err
	}
	expireI, err := strconv.ParseUint(p[1], 10, 64)
	if err != nil {
		return ID{}, err
	}
	return ID{T: TS(tI), Expire: TS(expireI)}, nil
}

// Event describes all the information related to a single event. An event is
// immutable, nothing in this struct will ever change
type Event struct {
	ID       ID
	Contents string
}

// NewEvent initializes an event struct with the given information, as well as
// creating an ID for the event

// NewEvent creates an event struct with the given information. The returned
// Event will have the given contents, and its ID will a unique identifier based
// on the passed in now and expire.
func (c *Core) NewEvent(now, expire TS, contents string) (Event, error) {
	nowMono, err := c.MonoTS(now)
	if err != nil {
		return Event{}, err
	}
	id := ID{nowMono, expire}

	return Event{
		ID:       id,
		Contents: contents,
	}, nil
}

func pexpireAt(t TS, buffer time.Duration) int64 {
	return t.Time().Add(buffer).UnixNano() / 1e6 // to millisecond
}

// SetEvent sets the event with the given id to have the given contents. The
// event will expire based on the ID field in it (which will be truncated to an
// integer) added with the given buffer
func (c *Core) SetEvent(e Event, expireBuffer time.Duration) error {
	k := Key{Base: e.ID.String(), Subs: []string{"event"}}
	pex := pexpireAt(e.ID.Expire, expireBuffer)
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
		err = util.LuaEval(c.Cmder, lua, 1, k.String(c.RedisPrefix), pex, eb).Err
	}, &e)
	return err
}

// GetEvent returns the event identified by the given ID, or ErrNotFound if it's
// expired or never existed
func (c *Core) GetEvent(id ID) (Event, error) {
	k := Key{Base: id.String(), Subs: []string{"event"}}
	r := c.Cmd("GET", k.String(c.RedisPrefix))
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

// Key describes a location some data can be stored in in redis. Keys with the
// same Base will be stored together and can be interacted with transactionally.
// Subs is used a further set of identifiers for the Key.
//
// The only disallowed character in the strings making up key is ':'
type Key struct {
	Base string
	Subs []string
}

// String returns the string form of the Key, as it will appear in redis
func (k Key) String(prefix string) string {
	// If this changes remember to change it in Query as well
	if len(k.Subs) > 0 {
		return fmt.Sprintf("%s:k:{%s}:%s", prefix, k.Base, strings.Join(k.Subs, ":"))
	}
	return fmt.Sprintf("%s:k:{%s}", prefix, k.Base)
}

// Copy returns a deep copy of the Key
func (k Key) Copy() Key {
	subs := make([]string, len(k.Subs))
	copy(subs, k.Subs)
	k.Subs = subs
	return k
}

// KeyFromString takes the string form of a Key and returns the associated
// String. Will probably panic if the given string is not valid, so check
// yourself before you wreck yourself.
func KeyFromString(key string) Key {
	var k Key
	i, j := strings.Index(key, "{"), strings.Index(key, "}")
	k.Base = key[i+1 : j]
	if len(key) > j+1 {
		k.Subs = strings.Split(key[j+2:], ":")
	}
	return k
}

////////////////////////////////////////////////////////////////////////////////

// wherein I develop what amounts to a DSL for working with multiple Key's
// contents transactionally

// QueryScoreRange is used by multiple selectors to describe a range of elements
// by their scores. If MinExcl is true, Min itself will be excluded from the
// return if it's in the set (and similarly for MaxExcl/Max). If Min or Max are
// 0 that indicates -infinity or +infinity, respectively
type QueryScoreRange struct {
	Min              TS
	Max              TS
	MinExcl, MaxExcl bool

	// May be set instead of Min. The newest ID from the input to this
	// QueryAction will be used as the Min. If the input is empty, Min will be 0
	MinFromInput bool

	// May be set instead of Max. The oldest ID from the input to this
	// QueryAction will be used as the Max. If the input is empty, Max will be 0
	MaxFromInput bool
}

// QueryRangeSelect is used to select all elements within the given range from a
// Key.
type QueryRangeSelect struct {
	QueryScoreRange

	// Optional modifiers. If Offset is nonzero, Limit must be nonzero too (it
	// can be -1 to indicate no limit)
	Limit, Offset int64

	// If true, reverses the order that IDs in the set are processed and
	// returned. The meanings of Min/Max stay the same though.
	Reverse bool
}

// QueryIDScoreSelect pulls the given ID from a Key. If the ID is not
// in the Key there is no output. If the ID is in the Key but its score does
// not match whatever conditions are set by Min/Max/Equal there is no output.
// Otherwise the output is the single ID.
type QueryIDScoreSelect struct {
	ID    ID
	Min   TS
	Max   TS
	Equal TS
}

// QuerySelector describes a set of criteria for selecting a set of IDs from a
// Key. Key is a required field, only one field apart from it should be set in
// this selector (unless otherwise noted)
type QuerySelector struct {
	Key

	// See QueryRangeSelect doc string
	*QueryRangeSelect

	// See QueryIDScoreSelect doc string
	*QueryIDScoreSelect

	// Select IDs by their position within the Key, using a two element
	// slice. 0 is the oldest id, 1 is the second oldest, etc... -1 is the
	// youngest, -2 the second youngest, etc...
	PosRangeSelect []int64

	// Doesn't actually do a query, the output from this selector will simply be
	// these IDs.
	IDs []ID
}

// QueryCount will count the number of elements within the Key which fall into
// the given QueryScoreRange. The input to this action is passed straight into
// the output. The count is appended to the Counts field in the QueryRes
type QueryCount struct {
	Key
	QueryScoreRange
}

// QueryFilter will apply a filter to its input, only outputting the IDs
// which don't match the filter. Only one filter field should be set per
// QueryAction
type QueryFilter struct {
	// If set, only IDs which have not expired will be allowed through
	Expired bool

	// May be set alongside any other filter field. Will invert the filter, so
	// that whatever IDs would have been allowed through will not be, and
	// vice-versa
	Invert bool
}

// QueryConditional is a field on QueryAction which can affect what the
// QueryAction does. More than one field may be set on this to have multiple
// conditionals.
type QueryConditional struct {
	// Only do the QueryAction if all of the conditionals in this list return
	// true
	And []QueryConditional

	// Only do the QueryAction if there is an empty input. Otherwise do no
	// action and pass the non-empty input through
	IfNoInput bool

	// Only do the QueryAction if there is non-empty input. Otherwise pass the
	// empty input through
	IfInput bool

	// Only do the QueryAction if the given Key has no data in it
	IfEmpty *Key

	// Only do the QueryAction if the given Key has data in it
	IfNotEmpty *Key
}

// QueryAddTo adds its input IDs to the given Keys. If ExpireAsScore is set to
// true, then each ID's expire time will be used as its score. If Score is given
// it will be used as the score for all IDs being added, otherwise the T of
// each individual ID will be used
type QueryAddTo struct {
	Keys          []Key
	ExpireAsScore bool
	Score         TS
}

// QueryRemoveByScore is used to remove IDs from Keys based on a range of
// scores. This action does not change the input in anyway, it simply passes the
// input through as its output.
type QueryRemoveByScore struct {
	Keys []Key
	QueryScoreRange
}

// QuerySingleSet will set the given Key to the first ID in the input. If the
// input to this action has no IDs then nothing happens.  The output from this
// action will be the input.
//
// If IfNewer is set, the key will only be set if its ID is newer than the ID
// already in the Key. This does not change the output in any way
type QuerySingleSet struct {
	Key
	IfNewer bool
}

// QueryAction describes a single action to take on a set of IDs. Every action
// has an input and an output, which are both always sorted chronologically (by
// ID.T). Only one single field, apart from QueryConditional or Union, should be
// set on a QueryAction.
type QueryAction struct {
	// Selects a set of IDs using various types of logic. See the doc on
	// QuerySelector for more. By default, a QuerySelector causes this
	// QueryAction to simply discard its input and output the QuerySelector's
	// output.
	*QuerySelector

	// Counts elements in a Key. See its doc string for more info
	*QueryCount

	// If true will count the number of IDs in the input to this action, append
	// that count to the result, and pass that input through as the output.
	CountInput bool

	// Adds the input IDs to the given Keys. See its doc string for more info
	*QueryAddTo

	// Removes IDs from Keys by score. See its doc string for more info
	*QueryRemoveByScore

	// Removes the input IDs from the given Keys
	RemoveFrom []Key

	// Adds an ID to a key. See its doc string for more info
	*QuerySingleSet

	// Retrieve an ID that was set at the given Key using SingleSet. The
	// output will be a set continaing only this ID, or empty output if the
	// key isn't set
	SingleGet *Key

	// Filters Events out of the input. See its doc string for more info
	*QueryFilter

	// Deletes whatever the Key's contents are. The input to this action becomes
	// the output
	Delete *Key

	// Stops the pipeline of QueryActions and returns the previous output
	Break bool

	// May be set alongside any of the other fields on this struct. See its doc
	// string for more info
	QueryConditional

	// Must be set alongside another field in this Selector. Indicates that
	// instead of discarding the output of the previous QueryAction, its output
	// and the output from this action should be merged as a union. That union
	// then becomes the output of this action.
	Union bool
}

// QueryActions are a set of actions to take sequentially. A set of QueryActions
// effectively amounts to a pipeline of IDs. Each QueryAction has the potential
// to output some set of IDs, which become the input for the next QueryAction.
// The initial set of IDs is empty, so the first QueryAction should always have
// a QuerySelector to start things off. The final QueryAction's output will be
// the output set of IDs from the Query method, as well as the set of results
// from all Count operations which occurred during the query.
type QueryActions struct {
	// This must match the Base field on all Keys being used in this pipeline
	KeyBase      string
	QueryActions []QueryAction

	// Optional, may be passed in if there is a previous notion of "current
	// time", to maintain consistency
	Now TS `msg:"-"`
}

// QueryRes contains all the return values from a Query
type QueryRes struct {
	IDs    []ID
	Counts []uint64
}

// Query performs the given QueryActions pipeline. Whatever the final output
// from the pipeline is is returned.
func (c *Core) Query(qas QueryActions) (QueryRes, error) {
	var err error

	if qas.Now == 0 {
		qas.Now = NewTS(time.Now())
	}

	var resb []byte
	withMarshaled(func(bb [][]byte) {
		nowb := bb[0]
		qasb := bb[1]
		k := Key{Base: qas.KeyBase}.String(c.RedisPrefix)
		resb, err = util.LuaEval(c.Cmder, string(queryLua), 1, k, nowb, qasb, c.RedisPrefix).Bytes()
	}, qas.Now, &qas)
	if err != nil {
		return QueryRes{}, err
	}

	var res QueryRes
	if _, err = res.UnmarshalMsg(resb); err != nil {
		return QueryRes{}, err
	}
	if res.IDs == nil {
		res.IDs = []ID{}
	}
	return res, nil
}

// KeyScan returns all the Keys matching the given Key pattern. At least one
// field in the given Key should be a "*"
func (c *Core) KeyScan(k Key) ([]Key, error) {
	s := util.NewScanner(c.Cmder, util.ScanOpts{
		Command: "SCAN",
		Pattern: k.String(c.RedisPrefix),
	})

	var ret []Key
	for s.HasNext() {
		ret = append(ret, KeyFromString(s.Next()))
	}
	return ret, s.Err()
}
