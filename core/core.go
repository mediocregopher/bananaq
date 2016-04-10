// Package core is responsible for actually communicating with redis and
// providing an abstraction for the data stored in it
package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/levenlabs/go-llog"
	"github.com/levenlabs/golib/radixutil"
	"github.com/mediocregopher/radix.v2/redis"
	"github.com/mediocregopher/radix.v2/util"
)

var (
	c     util.Cmder
	pkgKV = llog.KV{"pkg": "core"}
)

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

// ID identifies a single event across the entire cluster, and is unique for all
// time
type ID int64

// NewID returns a new, unique ID which can be used for a new event
func NewID() (ID, error) {
	return newID(newIDBase(time.Now()))
}

func newIDBase(now time.Time) int64 {
	// We unfortunately are stuck using microseconds because lua gets kind of
	// weird at bigger numbers in converting to/from strings
	return now.UnixNano() / 1e3
}

// We split this out to make testing easier
func newID(now int64) (ID, error) {
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

// Time returns a time object which corresponds with when the id was created
func (id ID) Time() time.Time {
	return time.Unix(0, int64(id)*1e3)
}

// Event describes all the information related to a single event. An event is
// immutable, nothing in this struct will ever change
type Event struct {
	ID       ID
	Expire   time.Time
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
		Expire:   expire,
		Contents: contents,
	}, nil
}

func eventKey(id ID) string {
	return fmt.Sprintf("event:%f", id)
}

func queueKey(queueName string, parts ...string) string {
	return fmt.Sprintf("queue:{%s}:%s", queueName, strings.Join(parts, ":"))
}

func groupKey(queueName, groupName string, parts ...string) string {
	fullParts := make([]string, 0, len(parts)+1)
	fullParts = append(fullParts, groupName)
	fullParts = append(fullParts, parts...)
	return queueKey(queueName, fullParts...)
}

func extractQueueName(key string) string {
	i, j := strings.Index(key, "{"), strings.Index(key, "}")
	return key[i+1 : j]
}

func extractGroupName(key string) string {
	i := strings.Index(key, "}")
	key = key[i+2:]
	j := strings.Index(key, ":")
	if j == -1 {
		return key
	}
	return key[:j]
}

// SetEvent sets the event with the given id to have the given contents. The
// event will expire after the given timeout (which will be truncated to an
// integer)
func SetEvent(e Event) error {
	pexpire := e.Expire.UnixNano() / 1e6 // to milliseconds
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
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}

	return util.LuaEval(c, lua, 1, eventKey(e.ID), pexpire, b).Err
}

// GetEvent returns the event identified by the given id, or ErrNotFound if it's
// expired or never existed
func GetEvent(id ID) (Event, error) {
	r := c.Cmd("GET", eventKey(id))
	if r.IsType(redis.Nil) {
		return Event{}, ErrNotFound
	}

	b, err := r.Bytes()
	if err != nil {
		return Event{}, err
	}

	var e Event
	err = json.Unmarshal(b, &e)
	return e, err
}
