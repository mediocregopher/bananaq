// Package core is responsible for actually communicating with redis and
// providing an abstraction for the data stored in it
package core

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/levenlabs/go-llog"
	"github.com/levenlabs/golib/radixutil"
	"github.com/levenlabs/golib/timeutil"
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

// Timeout identifies the amount of time something is valid for before it will
// be cleaned from whatever context it exists in. It is represented as number of
// seconds, with everything passed the decimal point representing sub-second
// precision
type Timeout float64

// Abs is like AbsTime, but returns the time as a unix timestamp, with
// everything after the decimal representing sub-second precision
func (t Timeout) Abs(starting time.Time) float64 {
	return timeutil.Timestamp{starting}.Float64() + float64(t)
}

// AbsTime returns the point in time when this timeout is over, assuming it
// starts at the given time.
func (t Timeout) AbsTime(starting time.Time) time.Time {
	return timeutil.TimestampFromFloat64(t.Abs(starting)).Time
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
func SetEvent(id ID, contents string, timeout Timeout) error {
	return c.Cmd("SET", eventKey(id), contents, "EX", int64(timeout)).Err
}

// GetEvent returns the event identified by the given id, or ErrNotFound if it's
// expired or never existed
func GetEvent(id ID) (string, error) {
	r := c.Cmd("GET", eventKey(id))
	if r.IsType(redis.Nil) {
		return "", ErrNotFound
	}
	return r.Str()
}
