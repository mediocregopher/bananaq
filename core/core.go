// Package core is responsible for actually communicating with redis and
// providing an abstraction for the data stored in it
package core

import (
	"strconv"
	"time"

	"github.com/levenlabs/go-llog"
	"github.com/levenlabs/golib/radixutil"
	"github.com/levenlabs/golib/timeutil"
	"github.com/mediocregopher/radix.v2/util"
)

var (
	c     util.Cmder
	pkgKV = llog.KV{"pkg": "core"}
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
// time. It's also a timestamp which describes when the event is no longer
// valid.
type ID float64

// Time returns a time object which corresponds with the id/timestamp
func (id ID) Time() time.Time {
	return timeutil.TimestampFromFloat64(float64(id)).Time
}

// NewID returns a new, unique ID which can be used for a new event
func NewID() (ID, error) {
	return newID(timeutil.TimestampNow().Float64())
}

// We split this out to make testing easier
func newID(now float64) (ID, error) {
	lua := `
		local key = KEYS[1]
		local nowStr = ARGV[1]
		local lastStr = redis.call("GET", key)
		
		if not lastStr then
			redis.call("SET", key, nowStr)
			return nowStr
		end

		local now  = tonumber(nowStr, 10)
		local last = tonumber(lastStr, 10)
		if not now or not last or last < now then
			redis.call("SET", key, nowStr)
			return nowStr
		end

		-- Add a microsecond and use that
		last = last + 0.000001
		lastStr = tostring(last)
		redis.call("SET", key, lastStr)
		return lastStr
	`

	fStr, err := util.LuaEval(c, lua, 1, idKey, now).Str()
	if err != nil {
		return 0, err
	}

	f, err := strconv.ParseFloat(fStr, 64)
	return ID(f), err
}
