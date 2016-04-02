package consumers

import (
	"time"

	"github.com/mediocregopher/bananaq/db"
	"github.com/mediocregopher/bananaq/log"
)

// This piece runs separately from the main consumer thread. Its job is to
// periodically:
//
// * Run through all queues it can find in redis and truncate stale consumers
//   from the consumers list. This is necessary for the case of another okq
//   instances dying and leaving behind stale data
//
// * Update the timestamps of all the currently live consumers on this instance
//   in redis, so other okq instances don't truncate them

const (
	// StaleConsumerTimeout is the time a consumer has to update its register
	// status on a queue before it is considered stale and removed
	StaleConsumerTimeout = 30 * time.Second
)

func activeSpin() {
	tick := time.Tick(10 * time.Second)
	for range tick {
		if err := updateActiveConsumers(); err != nil {
			log.L.Printf("updating active consumers: %s", err)
		}
		if err := removeStaleConsumers(StaleConsumerTimeout); err != nil {
			log.L.Printf("removing stale consumers: %s", err)
		}
	}
}

func updateActiveConsumers() error {
	log.L.Debug("updating active consumers")
	ts := time.Now().Unix()

	// A list of args to pass into ZADD for each consumer
	consumersArgs := map[string][]interface{}{}

	// Populate consumersArgs arguments to the ZADD commands we're going to
	// need to perform
	for _, queue := range registeredQueues() {
		for _, client := range queueClients(queue) {
			args, ok := consumersArgs[queue]
			if !ok {
				args = make([]interface{}, 0, 3)
				args = append(args, db.ConsumersKey(queue))
				consumersArgs[queue] = args
			}

			consumersArgs[queue] = append(args, ts, client.ID)
		}
	}

	for _, args := range consumersArgs {
		if err := db.Inst.Cmd("ZADD", args...).Err; err != nil {
			return err
		}
	}

	return nil
}

func removeStaleConsumers(timeout time.Duration) error {
	log.L.Debug("removing stale consumers")

	wildcardKey := db.ConsumersKey("*")
	staleTS := time.Now().Add(timeout * -1).Unix()

	for key := range db.Inst.Scan(wildcardKey) {
		r := db.Inst.Cmd("ZREMRANGEBYSCORE", key, "-inf", staleTS)
		if err := r.Err; err != nil {
			return err
		}
	}

	return nil
}
