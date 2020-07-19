package btrdb

import (
	"github.com/iznauy/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/iznauy/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/iznauy/tsbs/query"
	"time"
	pb "github.com/iznauy/BTrDB/grpcinterface"
)

type BaseGenerator struct {
}

func (g *BaseGenerator) GenerateEmptyQuery() query.Query {
	return query.NewBTrDB()
}

func (g *BaseGenerator) fillInQuery(qi query.Query, humanLabel, humanDesc string, queryType int,
	statisticsSubQueries []*pb.QueryStatisticsRequest, nearestSubQueries []*pb.QueryNearestValueRequest) {
	q := qi.(*query.BTrDB)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)

	q.QueryType = queryType
	q.StatisticsSubQueries = statisticsSubQueries
	q.NearestSubQueries = nearestSubQueries

}

func (g *BaseGenerator) NewDevops(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := devops.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	dev := &Devops{
		BaseGenerator: g,
		Core:          core,
		meta: make(map[int]map[string]string, 128),
	}
	if err := dev.init(); err != nil {
		return nil, err
	}

	return dev, nil
}
