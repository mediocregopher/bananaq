package main

import (
	"errors"
	"math/rand"
	"net"
	"time"

	"github.com/levenlabs/go-llog"
	"github.com/mc0/okq/config"
	"github.com/mediocregopher/bananaq/clients"
	"github.com/mediocregopher/bananaq/clients/consumers"
	"github.com/mediocregopher/bananaq/commands"
	"github.com/mediocregopher/bananaq/log"
	_ "github.com/mediocregopher/bananaq/restore"
	"github.com/mediocregopher/lever"
	"github.com/mediocregopher/radix.v2/redis"
)

var pkgKV = llog.KV{
	"pkg": "main",
}

// TODO go through and make sure "okq" is completely gone

func main() {
	l := lever.New("bananaq", nil)
	l.Add(lever.Param{
		Name:        "--listen-addr",
		Description: "Address to listen for client connections on",
		Default:     ":4777",
	})
	l.Add(lever.Param{
		Name:        "--redis-addr",
		Description: "Address redis is listening on. May be a solo redis instance or a node in a cluster",
		Default:     "127.0.0.1:6379",
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
	logLevel, _ := l.ParamStr("--log-level")
	bgQaddPoolSize, _ := l.ParamInt("--bg-qadd-pool-size")

	llog.SetLevelFromString(logLevel)

	// TODO do this locally wherever rand is actually needed
	rand.Seed(time.Now().UnixNano())

	kv := llog.KV{"listenAddr": listenAddr}

	llog.Info("starting listen", pkgKV, kv)
	server, err := net.Listen("tcp", config.ListenAddr)
	if err != nil {
		llog.Fatal("error listening", pkgKV, kv, llog.KV{"err": err})
	}

	log.L.Printf("listening on %s", config.ListenAddr)

	incomingConns := make(chan net.Conn)

	go acceptConns(server, incomingConns)

	for {
		conn := <-incomingConns
		client := clients.NewClient(conn)

		log.L.Debug(client.Sprintf("serving"))
		go serveClient(client)
	}
}

func acceptConns(listener net.Listener, incomingConns chan net.Conn) {
	for {
		conn, err := listener.Accept()
		if conn == nil {
			log.L.Printf("couldn't accept: %q", err)
			continue
		}
		incomingConns <- conn
	}
}

var invalidCmdResp = redis.NewResp(errors.New("ERR invalid command"))

func serveClient(client *clients.Client) {
	conn := client.Conn
	rr := redis.NewRespReader(conn)

outer:
	for {
		var command string
		var args []string

		m := rr.Read()
		if m.IsType(redis.IOErr) {
			log.L.Debug(client.Sprintf("client connection error %q", m.Err))
			if len(client.Queues) > 0 {
				consumers.UpdateQueues(client, []string{})
			}
			client.Close()
			return
		}

		parts, err := m.Array()
		if err != nil {
			log.L.Debug(client.Sprintf("error parsing to array: %q", err))
			continue outer
		}
		for i := range parts {
			val, err := parts[i].Str()
			if err != nil {
				log.L.Debug(client.Sprintf("invalid command part %#v: %s", parts[i], err))
				invalidCmdResp.WriteTo(conn)
				continue outer
			}
			if i == 0 {
				command = val
			} else {
				args = append(args, val)
			}
		}

		log.L.Debug(client.Sprintf("%s %#v", command, args))
		commands.Dispatch(client, command, args)
	}
}
