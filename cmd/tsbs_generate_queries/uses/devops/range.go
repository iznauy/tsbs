package devops

import (
	"github.com/iznauy/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/iznauy/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/iznauy/tsbs/query"
	"time"
)

type RangeQuery struct {
	core utils.QueryGenerator
	span time.Duration
}

func NewRangeQuery(span time.Duration) func(utils.QueryGenerator) utils.QueryFiller {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &RangeQuery{core, span}
	}
}

// Fill fills in the query.Query with query details
func (d *RangeQuery) Fill(q query.Query) query.Query {
	fc, ok := d.core.(RangeQueryFilter)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.RangeQuery(q, d.span)
	return q
}
