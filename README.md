# bananaq

(Pronounced: "Banana queue")

# THIS IS SUPER ALPHA UNSTABLE WIP YMMV DONT TRY THIS AT HOME, DONT USE IT

[![Build Status](https://travis-ci.org/mediocregopher/bananaq.svg?branch=master)](https://travis-ci.org/mediocregopher/bananaq)

bananaq is conceptually similar to kafka, but with major improvements in terms
of simplicity, ease-of-use, and (the biggest improvement of all) not running on
the JVM.

Features of bananaq:

* Clients talk to bananaq using the redis protocol. So if your language has a
  redis driver you already have an bananaq driver as well.

* Binary safe.

* Supports a single redis instance or a redis cluster. So scaling and optimizing
  bananaq is as easy as scaling/optimizing redis.

* Multiple bananaq instances can run on the same redis instance/cluster without
  knowing about each other, easing deployment.

* Go client library available which connects directly to redis instead of going
  through a server. No coordination is needed to use it, it feels exactly like a
  normal driver.

## Table of contents

* [Concepts](#concepts)
* [Install](#install)
* [Configuration](#configuration)
* [Usage](#usage)
  * [QADD](#qadd)
  * [QGET](#qget)
  * [QACK](#qack)
  * [QSTATUS](#qstatus)
  * [QINFO](#qinfo)

## Concepts

* An event is pushed onto a queue. It's ID corresponds to the timestamp it was
  pushed onto the queue at, and its expire time is specified by the client
  adding it. Once the expire time has been reached, the event is no longer
  available for any interactions.

* Events are consumed by consumers in the order they were added to a queue.

* Each consumer assigns itself to a consumer group. Two consumers within a
  consumer group share the internal pointer indicating how far in the queue has
  been read. So if another consumer in another group comes in and starts
  reading, it'll start reading the queue as if no events have ever been read
  from it (because from that group's point of view, none have).

* If all consumers of a queue use the same consumer group, then the queue acts
  effectively like a normal queue. If they all use a different consumer group,
  the queue effectively acts like a pubsub system. Bananaq also supports
  everything in-between.

* Once retrieved from a queue, and event may be ack'd to indicate that consuming
  it was successul. If it's not ack'd after some time the event will be put made
  available for the consumer group to retrieve again.

* A consumer may decide it doesn't need to ack an event. In this case the event
  is considered succesffully consumed as soon as its gotten. So a consumer can
  decide to be either at-most-once or at-least-once

## Install

    go get github.com/mediocregopher/bananaq

You'll now have a `bananaq` binary in your `$GOPATH/bin`. To get started you can
do `bananaq -h` to see available options

## Configuration

bananaq can take in options on the command line, through environment variables,
or from a config file. All configuration parameters are available through any of
the three. The three methods can be mixed and matched, with environment
variables taking precedence over a config file, and command line arguments
taking precedence over environment variables.

    # See command line parameters and descriptions
    bananaq -h

    # Create a configuration file with default values pre-populated
    bananaq --example > bananaq.conf

    # Use configuration file
    bananaq --config bananaq.conf

    # Set --listen-addr argument in an environment variable (as opposed to on
    # the command line or the configuration file
    export BANANAQ_LISTEN_ADDR=127.0.0.1:5777
    bananaq

    # Mix all three
    export bananaq_LISTEN_ADDR=127.0.0.1:5777
    bananaq --config bananaq.conf --redis-cluster --redis-addr=127.0.0.1:6380

## Usage

By default bananaq listens on port 5777. You can connect to it using any existing
redis client, and all bananaq commands look very similar to redis commands. For
example, to add an event to a queue:

```
redis-cli -p 5777
> QADD foo 1464387087 eventcontents
< "1464387077"
```

TODO maybe make anote about expire seconds and precision

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

> QGET queue consumerGroup [DEADLINE deadlineSeconds] [BLOCK blockSeconds]

Retrieve the next available event from the given queue for the given
consumer-group.

`queue` is any arbitrary queue name.

`consumerGroup` is any arbitrary name a consumer group this consumer is
consuming as.

`DEADLINE deadlineSeconds` determines how long the consumer has to [QACK](#qack) the
event before it is marked as available (so it will be consumed next) and made
available to other consumers in the group again. If not set the event will be
automatically marked as consumed so no QACK is necessary.

`BLOCK blockSeconds` may be set to indicate that the connection should block for
up to that many seconds if the queue has no available events on it, waiting for
a new event to show up.

Returns an array-reply with the ID and contents of an event in the queue, or nil
if no events are available.

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

This is only necessary if the `DEADLINE` parameter was given for the
[QGET](#qget) command used to retrieve the event. If this is not called within
that deadline the event will be made available again for other consumers in the
consumer group.

Returns an integer `1` if the event was acknowledged successfully, or `0` if not
(implying the deadline was passed or the event was acknowledged by another
consumer).

### QSTATUS

> QSTATUS [[QUEUE queue] [GROUP consumerGroup] …]

Get information about all active queues and consumer groups. `QUEUE queue` and
`GROUP consumerGroup` may both be specified zero or more times each to only
retrieve information about specific queues and consumer groups. A `QUEUE` must
be specified if a `GROUP` is specified.

An array structure is returned with the following layout:

* Top-level is a key-value array, with alternating queue names and their
  information

* Each information value is another key-value array. The final key-value pair
  will be for consumer information, which is yet another key-value array of
  consumer group -> consumer group information

* Consumer group information is yet another key-value array

```
> QSTATUS QUEUE foo QUEUE bar GROUP consumerGroup1 GROUP consumerGroup2
< 1) "foo"
< 2) 1) "total"
     2) (integer) 5
     3) "consumers"
     4) 1) "consumerGroup1"
        2) 1) "inprogress"
           2) (integer) 1
           3) "redo"
           4) (integer) 2
           5) "done"
           6) (integer) 1
           7) "available"
           8) (integer) 1
        3) "consumerGroup2"
        4) 1) "inprogress"
           2) (integer) 1
           3) "redo"
           4) (integer) 2
           5) "done"
           6) (integer) 1
           7) "available"
           8) (integer) 1
  3) "bar"
  4) 1) "total"
     2) (integer) 5
     3) "consumers"
     4) 1) "consumerGroup1"
        2) 1) "inprogress"
           2) (integer) 1
           3) "redo"
           4) (integer) 2
           5) "done"
           6) (integer) 1
           7) "available"
           8) (integer) 1
```

The statistic maps each contain these keys/values:

* total - The total number of non-timed out events currently in that queue (will
  be the same across all consumer groups for that queue).

* inprogress - The number of events marked as in-progress (i.e. are awaiting
  being [QACK'd](#qack)) for this consumer group. This includes expired events.

* redo - The number of events which were marked as in-progress but were
  timedout, and so are currently available for consuming again by this
  consumer group. This includes expired events.

* available - The number of events which are available for being consumed by a
  consumer in this consumer group. This includes expired events.

*NOTE that there may in the future be more information returned in the
statistics maps returned by this call; do not assume that they will always be of
the given length or order.*

### QINFO

> QINFO [[QUEUE queue] [GROUP consumerGroup] …]

Get human readable information about all active queues and consumer groups.
`QUEUE queue` and `GROUP consumerGroup` may both be specified zero or more times
each to only retrieve information about specific queues and consumer groups.

This command effectively calls [QSTATUS](#qstatus) with the given arguments and
returns its output in a nicely formatted way. The returned value will be an
array of strings, one per queue, each formatted like so:

```
> QINFO QUEUE foo QUEUE bar GROUP consumerGroup1 GROUP consumerGroup2
< 1) queue:"foo" total:5
< 2) consumerGroup:"consumerGroup1" avail:1 inProg:1 redo:2
< 3) consumerGroup:"consumerGroup2" avail:1 inProg:1 redo:2
< 4) queue:"bar" total:5
< 5) consumerGroup:"consumerGroup1" avail:1 inProg:1 redo:2
```

See QSTATUS for the meaning of the different fields

*NOTE that this output is intended to be read by humans and its format may
change slightly every time the command is called. For easily machine readable
output of the same data see the [QSTATUS](#qstatus) command*
