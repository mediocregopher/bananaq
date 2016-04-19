// package dispatch contains the actual application specific logic for each
// command which can be performed.
package dispatch

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/levenlabs/go-llog"
	"github.com/mediocregopher/bananaq/core"
)

var (
	pkgKV    = llog.KV{"pkg": "dispatch"}
	qaddBGCh chan Command
)

// Init initializes the package so it can handle commands. qaddBGPoolSize is the
// number of go-routines to spin up in order to handle non-blocking qadd
// commands
func Init(qaddBGPoolSize int) {
	qaddBGCh = make(chan Command, qaddBGPoolSize)

	for i := 0; i < qaddBGPoolSize; i++ {
		go func(i int) {
			ikv := llog.KV{"i": i}
			for c := range qaddBGCh {
				kv := c.kv()
				llog.Debug("background qadd", pkgKV, ikv, kv)
				res, err := qadd(c)
				if err != nil {
					llog.Error("error doing background qadd", pkgKV, ikv, kv, llog.KV{"err": err})
				} else {
					llog.Debug("background qadd result", pkgKV, ikv, kv, llog.KV{"result": res})
				}
			}
		}(i)
	}
}

// Command describes a command to be dispatched, and the client which is
// dispatching it
type Command struct {
	ClientID string
	Command  string
	Args     []string
}

func (c Command) kv() llog.KV {
	return llog.KV{
		"clientID": c.ClientID,
		"cmd":      c.Command,
		"args":     c.Args,
	}
}

type commandFunc func(Command) (interface{}, error)

type commandInfo struct {
	f       commandFunc
	minArgs int
}

var commandMap = map[string]commandInfo{
	"PING": {ping, 0},
	"QADD": {qadd, 3},
}

// Dispatch takes in a client whose command has already been read off the
// socket, a list of arguments from that command (not including the command name
// itself), and handles that command, returning a result.
func Dispatch(c Command) interface{} {
	cmdInfo, ok := commandMap[strings.ToUpper(c.Command)]
	if !ok {
		return fmt.Errorf("ERR unknown command %q", c.Command)
	}

	if len(c.Args) < cmdInfo.minArgs {
		return fmt.Errorf("ERR missing args")
	}

	kv := c.kv()
	llog.Debug("dispatching command", pkgKV, kv)

	ret, err := cmdInfo.f(c)
	if err != nil {
		llog.Error("server side error", pkgKV, kv, llog.KV{"err": err})
		return fmt.Errorf("ERR unexpected server-side error")
	}

	llog.Debug("dispatched command result", pkgKV, kv, llog.KV{"result": ret})
	return ret
}

func ping(c Command) (interface{}, error) {
	return "PONG", nil
}

func qadd(c Command) (interface{}, error) {
	queue := c.Args[0]
	expire, err := strconv.Atoi(c.Args[1])
	if err != nil {
		return err, nil
	}
	contents := c.Args[2]

	if len(c.Args) > 3 && strings.ToLower(c.Args[3]) == "noblock" {
		c.Args = c.Args[:3] // if we don't do this, we get an infinite loop
		select {
		case qaddBGCh <- c:
		default:
			return fmt.Errorf("bg-qadd-pool full, dropping qadd"), nil
		}
		return "OK", nil
	}

	now := time.Now()
	e, err := core.NewEvent(now.Add(time.Duration(expire)*time.Second), contents)
	if err != nil {
		return nil, err
	}

	// We always store the event data itself with an extra 30 seconds until it
	// expires, just in case a consumer gets it just as its expire time hits
	if err := core.SetEvent(e, 30*time.Second); err != nil {
		return nil, err
	}

	es := queueAvailable(queue)

	qa := core.QueryAction{
		QuerySelector: core.QuerySelector{
			EventSet: es,
			Events:   []core.Event{e},
		},
		AddTo: []core.EventSet{es},
	}
	if _, err := core.Query(qa); err != nil {
		return nil, err
	}

	return e.ID, nil
}
