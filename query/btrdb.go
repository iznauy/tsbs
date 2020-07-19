package query

import (
	"fmt"
	pb "github.com/iznauy/BTrDB/grpcinterface"
	"sync"
)

const (
	QueryStatistics = 1
	QueryNearest    = 2
)

type BTrDB struct {
	HumanLabel       []byte
	HumanDescription []byte
	id               uint64

	QueryType            int
	StatisticsSubQueries []*pb.QueryStatisticsRequest
	NearestSubQueries    []*pb.QueryNearestValueRequest
}

var BTrDBPool = sync.Pool{
	New: func() interface{} {
		return &BTrDB{
			HumanLabel:           make([]byte, 0, 1024),
			HumanDescription:     make([]byte, 0, 1024),
			StatisticsSubQueries: make([]*pb.QueryStatisticsRequest, 0, 10),
			NearestSubQueries:    make([]*pb.QueryNearestValueRequest, 0, 10),
		}
	},
}

func NewBTrDB() *BTrDB {
	return BTrDBPool.Get().(*BTrDB)
}

func (q *BTrDB) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.id = 0

	q.QueryType = 0
	q.StatisticsSubQueries = nil
	q.NearestSubQueries = nil
}

func (q *BTrDB) HumanLabelName() []byte {
	return q.HumanLabel
}

func (q *BTrDB) HumanDescriptionName() []byte {
	return q.HumanDescription
}

func (q *BTrDB) GetID() uint64 {
	return q.id
}

func (q *BTrDB) SetID(id uint64) {
	q.id = id
}

func (q *BTrDB) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, QueryType: %d, StatisticsQuery: %v, NearestQuery: %v", q.HumanLabel, q.HumanDescription, q.QueryType, q.StatisticsSubQueries, q.NearestSubQueries)

}
