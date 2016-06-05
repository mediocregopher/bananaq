package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/levenlabs/golib/timeutil"
	"github.com/mediocregopher/bananaq/core"
	"github.com/mediocregopher/bananaq/peel"
	"github.com/mediocregopher/radix.v2/redis"
)

type dispatchFn struct {
	fn      func([]string) (interface{}, error)
	minArgs int
}

var dispatchTable = map[string]dispatchFn{
	"PING":    {ping, 0},
	"QADD":    {qadd, 3},
	"QGET":    {qget, 2},
	"QACK":    {qack, 3},
	"QSTATUS": {qstatus, 0},
	"QINFO":   {qinfo, 0},
}

func dispatch(cmd string, args []string) (interface{}, error) {
	fn, ok := dispatchTable[cmd]
	if !ok {
		return fmt.Errorf("unknown cmd %q", cmd), nil
	} else if len(args) < fn.minArgs {
		return errors.New("insufficient arguments"), nil
	}
	return fn.fn(args)
}

func timeFromStr(now time.Time, str string) (time.Time, error) {
	abs := false
	if len(str) == 0 {
		return time.Time{}, errors.New("empty expire string")
	} else if str[0] == '@' {
		abs = true
		str = str[1:]
	}

	expireF, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return time.Time{}, err
	}

	if abs {
		return timeutil.TimestampFromFloat64(expireF).Time, nil
	}
	d := time.Duration(expireF * float64(time.Second))
	return now.Add(d), nil
}

func ping(args []string) (interface{}, error) {
	return redis.NewRespSimple("PONG"), nil
}

func qadd(args []string) (interface{}, error) {
	now := time.Now()
	expire, err := timeFromStr(now, args[1])
	if err != nil {
		return err, nil
	}

	qadd := peel.QAddCommand{
		Queue:    args[0],
		Expire:   expire,
		Contents: args[2],
	}

	if len(args) > 3 && strings.ToUpper(args[3]) == "NOBLOCK" {
		select {
		case bgQAddCh <- qadd:
			return redis.NewRespSimple("OK"), nil
		default:
			return nil, errors.New("bgQAdd processes all busy and buffer is full")
		}
	}

	return p.QAdd(qadd)
}

func qget(args []string) (interface{}, error) {
	now := time.Now()

	qget := peel.QGetCommand{
		Queue:         args[0],
		ConsumerGroup: args[1],
	}
	args = args[2:]

	timeKV := func(k string) (time.Time, error) {
		if len(args) < 2 {
			return time.Time{}, nil
		}
		if strings.ToUpper(args[0]) == k {
			t, err := timeFromStr(now, args[1])
			if err != nil {
				return time.Time{}, err
			}
			args = args[2:]
			return t, err
		}
		return time.Time{}, nil
	}

	var err error
	if qget.AckDeadline, err = timeKV("DEADLINE"); err != nil {
		return err, nil
	}
	if qget.BlockUntil, err = timeKV("BLOCK"); err != nil {
		return err, nil
	}

	e, err := p.QGet(qget)
	if err != nil {
		return nil, err
	} else if (e == core.Event{}) {
		return nil, nil
	}
	return []string{e.ID.String(), e.Contents}, nil
}

func qack(args []string) (interface{}, error) {
	id, err := core.IDFromString(args[2])
	if err != nil {
		return err, nil
	}

	return p.QAck(peel.QAckCommand{
		Queue:         args[0],
		ConsumerGroup: args[1],
		EventID:       id,
	})
}

func argsToQCG(args []string) map[string][]string {
	m := map[string][]string{}
	var lastQueue string
	for {
		if len(args) < 2 {
			break
		}
		if up := strings.ToUpper(args[0]); up == "QUEUE" {
			lastQueue = args[1]
			if _, ok := m[lastQueue]; !ok {
				m[lastQueue] = nil
			}
		} else if up == "GROUP" {
			m[lastQueue] = append(m[lastQueue], args[1])
		}
		args = args[2:]
	}
	return m
}

func qstatus(args []string) (interface{}, error) {
	qsm, err := p.QStatus(peel.QStatusCommand{
		QueuesConsumerGroups: argsToQCG(args),
	})
	if err != nil {
		return nil, err
	}

	ret := []interface{}{}
	for q, qs := range qsm {
		ret = append(ret, q)
		qsret := []interface{}{"total", qs.Total}

		cgsret := []interface{}{}
		for cg, cgs := range qs.ConsumerGroupStats {
			cgret := []interface{}{
				"available", cgs.Available,
				"inprogress", cgs.InProgress,
				"redo", cgs.Redo,
			}
			cgsret = append(cgsret, cg, cgret)
		}

		qsret = append(qsret, "consumers", cgsret)
		ret = append(ret, qsret)
	}

	return ret, nil
}

func qinfo(args []string) (interface{}, error) {
	return p.QInfo(peel.QStatusCommand{
		QueuesConsumerGroups: argsToQCG(args),
	})
}
