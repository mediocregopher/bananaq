package peel

import (
	"math/rand"
	"os"
	. "testing"
	"time"

	"github.com/levenlabs/go-llog"
	"github.com/levenlabs/golib/testutil"
	"github.com/mediocregopher/bananaq/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TODO make sure this still works

// This test is looking for any inconsistencies which might appear under load
// from race-conditions and things of that nature
func TestConsumerLoad(t *T) {
	if os.Getenv("BANANAQ_LOAD_TEST") == "" {
		return
	}

	numEvents := 10000
	llog.Info("creating test queue", llog.KV{"numEvents": numEvents})
	queue, ii := newTestQueue(t, numEvents)
	cgroup := testutil.RandStr()
	llog.Info("test queue created", llog.KV{"queue": queue, "cgroup": cgroup})

	go func() {
		time.Sleep(5 * time.Minute)
		t.Fatal("test took too long")
	}()

	ch := make(chan core.ID)
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))

	go func() {
		for range time.Tick(10 * time.Millisecond) {
			require.Nil(t, testPeel.Clean(queue, cgroup))
			require.Nil(t, testPeel.CleanAvailable(queue))
		}
	}()

	consumer := func(i int) {
		for {
			var withDeadline bool
			var deadline time.Time
			if rand.Intn(2) == 0 {
				withDeadline = true
				deadline = time.Now().Add(10 * time.Millisecond)
			}

			e, err := testPeel.QGet(QGetCommand{
				Queue:         queue,
				ConsumerGroup: cgroup,
				AckDeadline:   deadline,
			})
			require.Nil(t, err)
			if (e == core.Event{}) {
				time.Sleep(1 * time.Second)
				continue
			}

			if withDeadline {
				if rand.Intn(10) == 0 {
					// force miss deadline
					time.Sleep(10 * time.Millisecond)
				}
				acked, err := testPeel.QAck(QAckCommand{
					Queue:         queue,
					ConsumerGroup: cgroup,
					EventID:       e.ID,
				})
				require.Nil(t, err)

				// If the ack didn't make the deadline we pretend like we never
				// had the event at all
				if !acked {
					continue
				}
			}

			ch <- e.ID
		}
	}
	for i := 0; i < 10; i++ {
		go consumer(i)
	}

	m := map[core.ID]bool{}
	for id := range ch {
		require.NotContains(t, m, id)
		m[id] = true
		if lm := len(m); lm == len(ii) {
			break
		} else if lm%(numEvents/20) == 0 {
			llog.Info("progress", llog.KV{"len(m)": lm})
		}
	}
	close(ch)

	llog.Info("checking m length")
	assert.Len(t, m, len(ii))
	llog.Info("checking each id in m")
	for _, id := range ii {
		assert.Contains(t, m, id)
	}

	llog.Info("checking inprogress sets")
	assertKey(t, queueInProgressByAck(queue, cgroup))
	llog.Info("checking redo set")
	assertKey(t, queueRedo(queue, cgroup))
	llog.Info("checking done set")
	// TODO fix this
	//assertKey(t, queueDone(queue, cgroup), ii...)
}
