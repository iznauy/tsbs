package devops

import (
	"github.com/iznauy/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/iznauy/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/iznauy/tsbs/query"
)

type TreeAggregation struct {
	core utils.QueryGenerator
	width int
}

func NewTreeAggregation(width int) func(utils.QueryGenerator) utils.QueryFiller {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &TreeAggregation{core, width}
	}
}

// Fill fills in the query.Query with query details
func (d *TreeAggregation) Fill(q query.Query) query.Query {
	fc, ok := d.core.(TreeAggregationFilter)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.TreeAggregation(q, d.width)
	return q
}

