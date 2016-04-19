package dispatch

import "github.com/mediocregopher/bananaq/core"

func queueAvailable(queue string) core.EventSet {
	return core.EventSet{Base: queue, Subs: []string{"available"}}
}
