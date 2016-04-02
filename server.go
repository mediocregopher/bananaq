package main

import (
	"errors"
	"math/rand"
	"net"
	"time"

	"github.com/mediocregopher/bananaq/clients"
	"github.com/mediocregopher/bananaq/clients/consumers"
	"github.com/mediocregopher/bananaq/commands"
	"github.com/mediocregopher/bananaq/config"
	"github.com/mediocregopher/bananaq/log"
	_ "github.com/mediocregopher/bananaq/restore"
	"github.com/mediocregopher/radix.v2/redis"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	server, err := net.Listen("tcp", config.ListenAddr)
	if server == nil {
		log.L.Fatal(err)
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
