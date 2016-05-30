package peel

import "github.com/mediocregopher/bananaq/core"

// We need some wrapper functions for a simple construct we use multiple times
// within a queue/consumer group. It's a single set, with arbitrary scores on
// each element, and an accompanying set with scores which are expires. With
// these two sets we can create an abstraction over what is essentially a single
// set, with each element having both an arbitrary score and an expiration after
// which it won't be returned
type exWrap struct {
	base  string
	byArb core.Key
	byExp core.Key
}

// newExWrap returns an exWrap around the given key. It does not matter if the
// key is marshalled/validated or not
func newExWrap(k core.Key) exWrap {
	var ew exWrap
	ew.base = k.Base
	ew.byArb = k
	ew.byExp = k.Copy()
	ew.byExp.Subs = append(ew.byExp.Subs, "expire")
	return ew
}

// returns actions which will add the IDs which are input into them to this
// exWrap. score will be the score with which all IDs will be added, if it's
// zero then each ID's T field will be used as its score
func (ew exWrap) addFromInput(score core.TS) []core.QueryAction {
	return []core.QueryAction{
		{
			QueryAddTo: &core.QueryAddTo{
				Keys:  []core.Key{ew.byArb},
				Score: score,
			},
		},
		{
			QueryAddTo: &core.QueryAddTo{
				Keys:          []core.Key{ew.byExp},
				ExpireAsScore: true,
			},
		},
	}
}

// returns actions which will add the given ID with the given score. If score is
// zero then the T field of the ID will be used as the score
func (ew exWrap) add(id core.ID, score core.TS) []core.QueryAction {
	aa := make([]core.QueryAction, 1, 3)
	aa[0] = core.QueryAction{
		QuerySelector: &core.QuerySelector{
			Key: ew.byArb,
			IDs: []core.ID{id},
		},
	}
	aa = append(aa, ew.addFromInput(score)...)
	return aa
}

func (ew exWrap) removeFromInput() core.QueryAction {
	return core.QueryAction{
		RemoveFrom: []core.Key{
			ew.byArb,
			ew.byExp,
		},
	}
}

// returns actions which will remove all events whose expire has passed (based
// on the given TS) from both underlying sets. The output from these actions
// will be the events which were removed
func (ew exWrap) removeExpired(now core.TS) []core.QueryAction {
	return []core.QueryAction{
		{
			QuerySelector: &core.QuerySelector{
				Key: ew.byExp,
				QueryRangeSelect: &core.QueryRangeSelect{
					QueryScoreRange: core.QueryScoreRange{
						Max: now,
					},
				},
			},
		},
		ew.removeFromInput(),
	}
}

// returns an action which will output either one ID or no IDs. The ID which is
// output will be the first one which whose score is higher than ts

// returns an action which will output up to limit IDs whose score is greater
// than the given TS. If limit is less than 1 there is no limit
func (ew exWrap) after(ts core.TS, limit int64) core.QueryAction {
	return core.QueryAction{
		QuerySelector: &core.QuerySelector{
			Key: ew.byArb,
			QueryRangeSelect: &core.QueryRangeSelect{
				QueryScoreRange: core.QueryScoreRange{
					Min:     ts,
					MinExcl: true,
				},
				Limit: limit,
			},
		},
	}
}

// returns an action which will output up to limit IDs from the set whose score
// is greater than the ID read from input's. If limit is less than 1 there is no
// limit
func (ew exWrap) afterInput(limit int64) core.QueryAction {
	return core.QueryAction{
		QuerySelector: &core.QuerySelector{
			Key: ew.byArb,
			QueryRangeSelect: &core.QueryRangeSelect{
				QueryScoreRange: core.QueryScoreRange{
					MinExcl:      true,
					MinFromInput: true,
				},
				Limit: limit,
			},
		},
	}
}

// returns an action which will output up to limit IDs whose score is less than
// the given TS. If limit is less than 1 there is no limit
func (ew exWrap) before(ts core.TS, limit int64) core.QueryAction {
	return core.QueryAction{
		QuerySelector: &core.QuerySelector{
			Key: ew.byArb,
			QueryRangeSelect: &core.QueryRangeSelect{
				QueryScoreRange: core.QueryScoreRange{
					Max:     ts,
					MaxExcl: true,
				},
				Limit:   limit,
				Reverse: true,
			},
		},
	}
}

// returns an action which will append a count of the number of non-expired IDs
// to the result from the Query. input is passed straight through to output
func (ew exWrap) countNotExpired(now core.TS) core.QueryAction {
	return core.QueryAction{
		QueryCount: &core.QueryCount{
			Key: ew.byExp,
			QueryScoreRange: core.QueryScoreRange{
				Min:     now,
				MinExcl: true,
			},
		},
	}
}

// returns an action which will append a count of the number of IDs whose scores
// are greater than the first ID's from the input
func (ew exWrap) countAfterInput() core.QueryAction {
	return core.QueryAction{
		QueryCount: &core.QueryCount{
			Key: ew.byArb,
			QueryScoreRange: core.QueryScoreRange{
				MinFromInput: true,
				MinExcl:      true,
			},
		},
	}
}
