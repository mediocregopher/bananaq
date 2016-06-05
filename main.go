package main

import (
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/levenlabs/go-llog"
	"github.com/levenlabs/golib/radixutil"
	"github.com/mediocregopher/bananaq/peel"
	"github.com/mediocregopher/lever"
	"github.com/mediocregopher/radix.v2/redis"
)

// TODO go through and make sure "okq" is completely gone
// TODO metalint everything

var p *peel.Peel
var bgQAddCh chan peel.QAddCommand

func main() {
	l := lever.New("bananaq", nil)
	l.Add(lever.Param{
		Name:        "--listen-addr",
		Description: "Address to listen for client connections on",
		Default:     ":5777",
	})
	l.Add(lever.Param{
		Name:        "--redis-addr",
		Description: "Address redis is listening on. May be a solo redis instance or a node in a cluster",
		Default:     "127.0.0.1:6379",
	})
	l.Add(lever.Param{
		Name:        "--redis-pool-size",
		Description: "Size of the pool of idle connections to keep for redis. If a cluster is used, this many connections will be kept to each member of the cluster",
		Default:     "10",
	})
	l.Add(lever.Param{
		Name:        "--log-level",
		Description: "Log level to run with. Can be debug, info, warn, error, fatal",
		Default:     "info",
	})
	l.Add(lever.Param{
		Name:        "--bg-qadd-pool-size",
		Description: "Number of goroutines to have processing NOBLOCK QADD commands",
		Default:     "128",
	})
	l.Parse()

	listenAddr, _ := l.ParamStr("--listen-addr")
	redisAddr, _ := l.ParamStr("--redis-addr")
	redisPoolSize, _ := l.ParamInt("--redis-pool-size")
	logLevel, _ := l.ParamStr("--log-level")
	bgQAddPoolSize, _ := l.ParamInt("--bg-qadd-pool-size")

	llog.SetLevelFromString(logLevel)

	// Set up redis/peel
	{
		kv := llog.KV{
			"redisAddr":     redisAddr,
			"redisPoolSize": redisPoolSize,
		}
		llog.Info("connecting to redis", kv)
		cmder, err := radixutil.DialMaybeCluster("tcp", redisAddr, redisPoolSize)
		if err != nil {
			llog.Fatal("could not connect to redis", kv.Set("err", err))
		}

		p := peel.New(cmder, nil)
		go func() {
			for {
				err := <-p.Run(nil)
				llog.Error("error during peel runtime", kv.Set("err", err))
				time.Sleep(500 * time.Millisecond)
			}
		}()
	}

	// Start bgQAdd processes
	{
		if bgQAddPoolSize < 1 {
			llog.Fatal("--bg-qadd-pool-size must be at least 1")
		}
		kv := llog.KV{"bgQAddPoolSize": bgQAddPoolSize}
		llog.Debug("starting bgQAdd routines", kv)
		bgQAddCh = make(chan peel.QAddCommand, bgQAddPoolSize*10)
		for i := 0; i < bgQAddPoolSize; i++ {
			go func(i int) {
				kv = kv.Set("i", i)
				for qadd := range bgQAddCh {
					qkv := llog.KV{"queue": qadd.Queue}
					llog.Debug("bg qadd", kv, qkv)
					if ret, err := p.QAdd(qadd); err != nil {
						llog.Error("error doing background qadd", kv, qkv, llog.KV{"err": err})
					} else {
						llog.Debug("bg qadd ret", kv, qkv, llog.KV{"ret": ret})
					}
				}
			}(i)
		}
	}

	// Start actually listening
	{
		kv := llog.KV{"listenAddr": listenAddr}

		llog.Info("starting listen", kv)
		server, err := net.Listen("tcp", listenAddr)
		if err != nil {
			llog.Fatal("error listening", kv, llog.KV{"err": err})
		}

		go func() {
			for {
				conn, err := server.Accept()
				if conn == nil {
					llog.Error("error accepting", kv.Set("err", err))
					continue
				}
				go serveConn(conn)
			}
		}()
	}

	llog.Info("ready, set, go!")
	select {}

}

func serveConn(conn net.Conn) {
	kv := llog.KV{
		"remoteAddr": conn.RemoteAddr().String(),
	}
	rr := redis.NewRespReader(conn)

	llog.Debug("client connected", kv)

	var cmd string
	var args []string

	readCmd := func() (string, []string, error) {
		m := rr.Read()
		if m.IsType(redis.IOErr) {
			return "", nil, m.Err
		}

		parts, err := m.Array()
		if err != nil {
			return "", nil, fmt.Errorf("invalid command: %s", err)
		}
		args = args[:0]
		for i := range parts {
			val, err := parts[i].Str()
			if err != nil {
				return "", nil, fmt.Errorf("invalid command: %s", err)
			}
			if i == 0 {
				cmd = val
			} else {
				args = append(args, val)
			}
		}
		return strings.ToUpper(cmd), args, nil
	}

	writeErr := func(err error) {
		redis.NewResp(fmt.Errorf("ERR %s", err)).WriteTo(conn)
	}

	for {
		cmd, args, err := readCmd()
		if nerr, ok := err.(*net.OpError); ok && nerr.Timeout() {
			continue
		} else if err == io.EOF {
			llog.Debug("client disconnected", kv)
			conn.Close()
			return
		} else if err != nil {
			llog.Warn("client error reading command", kv.Set("err", err))
			writeErr(err)
		}

		shortArgs := make([]string, len(args))
		for i, arg := range args {
			if len(arg) > 100 {
				arg = arg[:100] + "..."
			}
			shortArgs[i] = arg
		}
		cmdKV := llog.KV{"cmd": cmd, "args": shortArgs}

		llog.Debug("client command", kv, cmdKV)

		// ret may be an error if it's a client error (e.g. invalid params)
		if ret, err := dispatch(cmd, args); err != nil {
			llog.Error("error dispatching command", kv, cmdKV, llog.KV{"err": err})
			redis.NewResp(fmt.Errorf("server-side error: %s", err)).WriteTo(conn)
			writeErr(fmt.Errorf("server-side error: %s", err))
		} else if rerr, ok := ret.(error); ok {
			llog.Warn("client error dispatching command", kv, cmdKV, llog.KV{"err": rerr})
			writeErr(rerr)
		} else {
			llog.Debug("client command response", kv, cmdKV, llog.KV{"ret": ret})
			redis.NewResp(ret).WriteTo(conn)
		}
	}
}
