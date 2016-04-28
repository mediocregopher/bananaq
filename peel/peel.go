// Package peel contains the actual application specific logic for each
// command which can be performed. It's designed to be able to be used as an
// actual client for bananaq if desired.
package peel

import (
	"time"

	"github.com/mediocregopher/bananaq/core"
)

// Peel contains all the information needed to actually implement the
// application logic of bananaq. it is intended to be used both as the server
// component and as a client for external applications which want to be able to
// interact with the database directly. It can be initialized manually, or using
// the New method. All methods on Peel are thread-safe.
type Peel struct {
	c core.Core
}

// New initializes a Peel struct with a Core, using the given redis address and
// pool size. The redis address can be a standalone node or a node in a cluster.
func New(redisAddr string, poolSize int) (Peel, error) {
	c, err := core.New(redisAddr, poolSize)
	return Peel{c}, err
}

// Client describes the information attached to any given client of bananaq.
// For most commands this isn't actually necessary except for logging, but for
// some it's actually used (these will be documented as such)
type Client struct {
	ID string
}

// QAddCommand describes the parameters which can be passed into the QAdd
// command
type QAddCommand struct {
	Client
	Queue    string        // Required
	Expire   time.Duration // Required
	Contents string        // Required
}

// QAdd adds an event to a queue. The event will expire after the given
// duration. The ID of the event is returned.
func (p Peel) QAdd(c QAddCommand) (core.ID, error) {
	now := time.Now()
	e, err := p.c.NewEvent(now.Add(c.Expire), c.Contents)
	if err != nil {
		return 0, err
	}

	// We always store the event data itself with an extra 30 seconds until it
	// expires, just in case a consumer gets it just as its expire time hits
	if err := p.c.SetEvent(e, 30*time.Second); err != nil {
		return 0, err
	}

	es := queueAvailable(c.Queue)

	qa := core.QueryActions{
		EventSetBase: es.Base,
		QueryActions: []core.QueryAction{
			{
				QuerySelector: &core.QuerySelector{
					EventSet: es,
					Events:   []core.Event{e},
				},
			},
			{
				AddTo: []core.EventSet{es},
			},
		},
	}
	if _, err := p.c.Query(qa); err != nil {
		return 0, err
	}

	return e.ID, nil
}
