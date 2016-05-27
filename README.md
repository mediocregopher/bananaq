# bananaq

(Pronounced: "Banana queue")

# THIS IS SUPER ALPHA UNSTABLE WIP YMMV DONT TRY THIS AT HOME, DONT USE IT

Like half the docs in this README are actually from okq, so don't read through
this all and expect it to make sense.

[![Build Status](https://travis-ci.org/mediocregopher/bananaq.svg?branch=master)](https://travis-ci.org/mediocregopher/bananaq)

bananaq is conceptually similar to kafka, but with major improvements in terms
of simplicity, ease-of-use, and (the biggest improvement of all) not running on
the JVM.

Features of bananaq:

* Clients talk to bananaq using the redis protocol. So if your language has a redis
  driver you already have an bananaq driver as well.

* Binary safe.

* Supports a single redis instance or a redis cluster. So scaling and optimizing
  bananaq is as easy as scaling/optimizing redis.

* Multiple bananaq instances can run on the same redis instance/cluster without
  knowing about each other, easing deployment.

## Table of contents

* [Install](#install)
* [Configuration](#configuration)
* [Usage](#usage)
* [Commands](#commands)
  * [QLPUSH](#qlpush)
  * [QRPUSH](#qrpush)
  * [QLPEEK](#qlpeek)
  * [QRPEEK](#qrpeek)
  * [QRPOP](#qrpop)
  * [QACK](#qack)
  * [QREGISTER](#qregister)
  * [QNOTIFY](#qnotify)
  * [QFLUSH](#qflush)
  * [QSTATUS](#qstatus)
  * [QINFO](#qinfo)
* [Consumers](#consumers)

## Concepts

    * Events are pushed on to queues in fifo style.

      Each event is assigned an
      id, which is a timestamp (monotonically increasing across the cluster, so
      each event's id is unique).

        * The client cannot specify an id, negating the ability to do
          "de-duplication" of events. bananaq is a message passing service, not
          a storage service. If you want to know if something has happened
          somewhere else already, store that fact in a storage service and use
          that.

    * When an event is pushed, its timeout is also specified. After this
      timeout, the event will no longer exist in bananaq.

    * Consumers declare their "consumer group" when consuming. Consumer groups
      are arbitrary and can appear/dissappear arbitrarily.

    * An event is only handed out to at most one consumer within a group at a
      time. Once an event has been processed, it will not be seen within a group
      again.

    * An event can be set to require acknowlegement after a certain timeout, or
      it will be put back in the queue. This is done on a per-consumer group
      basis.

    * An event's status for one group does not affect its status in another.
      Therefore, an event might be "completed" by all known consumer groups, but
      that doesn't mean its data can be discarded. An event is only discarded
      once its timeout is reached.

    * Each queue is a sorted set of events. The score is the id/timestamp
      of the event, the value is the timestamp it will timeout at

    * A consumer group looks through the queue from back to front. As it
      consumes events it marks down what it's currently processing, and the
      latest that has been completely processed.

        * This has problems, imagine the following scenario:

          - Queue has [A, B, C, D]
          - Consumer group has consumers foo and bar
          - A has been completed for this group
          - foo claims B
          - bar wants to get next event.

          Obviously C is next, but how can bar know that? It would have to
          simultaneously know the queue's contents and the current actions of
          all the other consumers.

        * Idea: queue is sorted set. Consumer group keeps three sorted sets,
          done, redo, and inprogress, scores also timestamp/id, values also
          expire time

          When consumer is getting an event:
          - If redo is not empty, take first id from it, put it in inprogress.
          - If either done and inprogress are not empty, get latest from both.
            Use that to get next from queue, and add to inprogress
          - If done and inprogress are empty, get first in queue, and put it in
            inprogress.

          When consumer is acking an event:
          - If acking success, delete id from inprogress, add it to done. Delete
            everything in done with a lower score
          - If acking failure, delete id from inprogress, add it to redo

          When cleaning events:
          - From the queue delete all events with a value less than the current
            time
          - From done and redo delete all events that have a value less than the
            current time
          - From inprogress move all events that have a value less than the
            current time to redo

          This idea assumes:
          - ids are monotonically increasing
          - all four sorted sets on same redis node

    * Would be nice to partition queues implicitely. Given that redis is backing
      bananaq, a single sorted set cannot be split across nodes. Instead we have
      to make multiple sorted sets. This could be done client-side, by having
      the client's do the sharding across multiple queues themselves. But that's
      kind of lame.

      On the other hand, doing it implicitely would be somewhat difficult. If a
      consumer knows only the name of the queue, it doesn't know which shard of
      the queue actually has any events. So it must go to them one-by-one and
      ask.

        * Choosing the shard ask-order randomly each time would be one solution,
          although not a great one. We've nixed all ordering guarantees, so
          that's not a concern, but for a highly sharded queue that happens to
          have few active events in it, it'd be a lot of wasted work.

        * Having each consumer assign themselves a shard number they start the
          search from would be interesting. They would start from their number
          and work their way around the "ring". If the numbers chosen by the
          different consumers were spread out enough this would have the
          net-result of speeding up consuming. However it would also cause
          certain shards to be preferred highly, so this might not work.

        * It's not ACTUALLY necessary to split the sorted sets. Those will grow
          to be quite large, but the actual in memory use shouldn't be crazy. A
          million entries in a sorted set takes about 135MB, and they really
          never should need to get that large. If they do actually need to be
          that large, then the user can do app-side sharding as well.

          Since the sorted sets only need the actual ids in them, the events
          themselves can just be normal keys scattered around the cluster, with
          their expire set properly.

      It's also not clear how the number of shards would even be determined.

        * A global number would work. However this could be a waste for clusters
          that have a couple very active queues and many many not-so-active
          queues, depending on the sharding strategy used.

        * A number per queue would be optimal, but how to assign that number?
          Could it be assigned during insert? Then the client and consumer would
          both have to coordinate the number. I'd like to avoid having a command
          which explicitely sets it, since that's configuration data being
          modified by a server command, which I don't like.

        * In the third idea where we don't actually split the sorted sets it
          won't be necessary to pick a shard size, the redis cluster shards the
          event data automatically.

## Install

From within the bananaq project root:

    go get ./...
    go build

You'll now have an `bananaq` binary. To get started you can do `bananaq -h` to see
available options

## Configuration

bananaq can take in options on the command line, through environment variables, or
from a config file. All configuration parameters are available through any of
the three. The three methods can be mixed an matched, with environment variables
taking precedence over a config file, and command line arguments taking
precedence over environment variables.

    # See command line parameters and descriptions
    bananaq -h

    # Create a configuration file with default values pre-populated
    bananaq --example > bananaq.conf

    # Use configuration file
    bananaq --config bananaq.conf

    # Set --listen-addr argument in an environment variable (as opposed to on
    # the command line or the configuration file
    export bananaq_LISTEN_ADDR=127.0.0.1:4777
    bananaq

    # Mix all three
    export bananaq_LISTEN_ADDR=127.0.0.1:4777
    bananaq --config bananaq.conf --redis-cluster --redis-addr=127.0.0.1:6380

## Usage

By default bananaq listens on port 4777. You can connect to it using any existing
redis client, and all bananaq commands look very similar to redis commands. For
example, to peek at the next event on the `foo` queue:

```
redis-cli -p 4777
> QLPEEK foo
< 1) "1"
  2) "testevent"
```

See the next section for all available commands.

## Commands

bananaq is modeled as a left-to-right queue. Clients submit events on the left
side of the queue (with `QLPUSH`), and consumers read off the right side (with
`QRPOP`).

```
     QLPUSH                QRPOP
client -> [event event event event] -> consumer
```

It is not necessary to instantiate a queue. Simply pushing an event onto it or
creating a consumer for it implicitly creates it. Deleting a queue is also not
necessary, once a queue has no events and no consumers it no longer exists.

### QADD

> QADD queue expireSeconds contents [NOBLOCK]

Add an event to the given queue.

`queue` is any arbitrary queue name.

`expireSeconds` is the number of seconds from this moment after which the event
will be removed from the queue.

This will not return until the event has been successfully stored in redis. Set
`NOBLOCK` if you want the server to return as soon as possible, even if the
event can't be successfully added.

Returns the event's id (a string) on success. If `NOBLOCK` is sent, the string
`OK` will be returned.

Returns an error if `NOBLOCK` is set and the bananaq instance is too overloaded
to handle the event in the background. Increasing `bg-qadd-pool-size` will
increase the number of available routines which can handle unblocked push
commands.

### QGET

> QGET queue consumerGroup [EX expireSeconds]

Retrieve the next available event from the given queue for the given
consumer-group.

`queue` is any arbitrary queue name.

`consumerGroup` is any arbitrary name a consumer group this consumer is
consuming as.

`EX expireSeconds` determines how long the consumer has to [QACK](#qack) the
event before it is marked as available (so it will be consumed next) and made
available to other consumers in the group again. It may be an integer string, or
a float string for less than second precision. If not set, defaults to 30
seconds. If set to `0` the event will automatically be acknowledged.

Returns an array-reply with the ID and contents of an event in the queue, or nil
if the queue is empty.

```
> QGET foo cool-kids
< 1) "9919b6ba-298a-44ee-9127-7176e91fd7d7"
  2) "event contents to be consumed"

> QGET foo cool-kids
< (nil)
```

### QACK

> QACK queue consumerGroup eventID

Acknowledges that the given event has been successfully processed by a consumer
in `consumerGroup`, so it won't be given to any consumers in that group again.

If this is not called within some amount of time after [QGET](#qget)ing the
event off the queue then the event will be marked as available again so any
consumers in that consumer group can get it again.

Returns an integer `1` if the event was acknowledged successfully, or `0` if not
(implying the event timed out or it was acknowledged by another consumer).

### QNOTIFY

> QNOTIFY consumerGroup timeout [queue ...]

Looks for an available event for the given `consumerGroup` on any of the given
queues, returning the name of the queue. If no events are available on any of
the queues, blocks until one is available, or until `timeout` seconds have
elapsed, at which point `nil` is returned.

### QFLUSH

> QFLUSH queue

Removes all events from the given queue.

Effectively makes it as if the given queue never existed. Any events in the
process of being consumed from the given queue may still complete, but if they
do not complete succesfully (no [QACK](#qack) is received) they will not be
added back to the queue.

Returns `OK` on success

### QSTATUS

> QSTATUS [queue ...] [GROUPS group ...]

Get information about the given queues (or all active queues, if none are
given) on the system. An optional set of consumer groups names can be given as
well to only return information about those particular consumer groups.

An array structure is returned with the following layout:

* Top-level is an array, one element for each queue requested

* Each element is a two element array, the first is the queue name, the second
  is a map (array of key/value pairs)

* For the map, the keys are consumer group names, and the values are a map of
  statistics, which are yet another map (array of key/value pairs)

```
> QSTATUS foo bar
< 1) 1) "foo"
     2) 1) "consumerGroup1"
        2) 1) "total"
           2) (integer) 5
           3) "inprogress"
           4) (integer) 1
           5) "redo"
           6) (integer) 2
           7) "done"
           8) (integer) 1
           9) "available"
           10) (integer) 1
           11) "consumers"
           12) 2
        3) "consumerGroup2"
        4) 1) "total"
           2) (integer) 5
           3) "inprogress"
           4) (integer) 1
           5) "redo"
           6) (integer) 2
           7) "done"
           8) (integer) 1
           9) "available"
           10) (integer) 1
           11) "consumers"
           12) 3
  2) 1) "bar"
     2) 1) "consumerGroup1"
        2) 1) "total"
           2) (integer) 5
           3) "inprogress"
           4) (integer) 1
           5) "redo"
           6) (integer) 2
           7) "done"
           8) (integer) 1
           9) "available"
           10) (integer) 1
           11) "consumers"
           12) 4
```

The statistic maps each contain these keys/values:

* total - The total number of non-timed out events currently in that queue (will
  be the same across all consumer groups for that queue).

* inprogress - The number of events marked as in-progress (i.e. are awaiting
  being [QACK'd](#qack)) for this consumer group

* redo - The number of events which were marked as in-progress but were
  timedout, and so are currently available for consuming again by this
  consumer group.

* done - The number of events which are have been consumed and acknowledged
  successfully by consumers in this consumer group.

* available - The number of events which are available for being consumed by a
  consumer in this consumer group.

* consumers - The number of consumers currently [notified](#qnotify) on the
  queue in this consumer group.

*NOTE that there may in the future be more information returned in the
statistics maps returned by this call; do not assume that they will always be of
the given length or order.*

### QINFO

> QINFO [queue ...] [GROUPS group ...]

Get human readable information about the given queues (or all active queues,
if none are given) on the system in a human readable format.

This command effectively calls [QSTATUS](#qstatus) with the given arguments and
returns its output in a nicely formatted way. The returned value will be an
array of strings, one per queue, each formatted like so:

```
> QINFO foo bar
< 1) "foo consumerGroup1 total:5 inprogress:1 redo:2 done:1 available:1 consumers:2"
< 2) "foo consumerGroup2 total:5 inprogress:1 redo:2 done:1 available:1 consumers:2"
< 3) "bar consumerGroup  total:5 inprogress:1 redo:2 done:1 available:1 consumers:2"
```

See QSTATUS for the meaning of the different fields

*NOTE that this output is intended to be read by humans and its format may
change slightly everytime the command is called. For easily machine readable
output of the same data see the [QSTATUS](#qstatus) command*

## Consumers

Writing bananaq clients (those which only submit events) is easy: A redis driver and
a call to [QLPUSH](#qlpush) are all that are needed.

The logic for consumer clients (those which are retrieving events and processing
them) is, however, slightly more involved (although not nearly as much
as for some other queue servers).

Here is the general order of events for a consumer for the `foo`, `bar`, and
`baz` queues:

1) Call `QREGISTER foo bar baz`. The client is now considered a consumer for
   these queues.

2) Call `QNOTIFY 30`. This will block until an event is pushed onto one of the
   three queues by some other client or the 30 second timeout is reached. If the
   timeout is reached `nil` will be returned and the consumer can start this
   step over. If an event was pushed then the name of the queue it was pushed
   onto will be returned from this call.

3) Call `QRPOP <queue from last step>`. If it returns `nil` then another
   consumer nabbed the event before we could; go back to step 2. Otherwise this
   will return the eventID and its contents.

4) At this point any application specific logic for the event should be run.
   Assuming success call `QACK <queue name from previous steps> <eventID>`. Go
   back to step 2.

It's likely that you'll want to write some generic wrapper code for this in your
language, if someone else hasnt written it already.
