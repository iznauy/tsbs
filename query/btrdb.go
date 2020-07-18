package query

import (
	"fmt"
	"sync"
)

const (
	QueryRange      = 0
	QueryStatistics = 1
	QueryNearest    = 2
)

type BTrDB struct {
	HumanLabel       []byte
	HumanDescription []byte
	id               uint64

	QueryType int
	SubQueries     []interface{}
}

var BTrDBPool = sync.Pool{
	New: func() interface{} {
		return &BTrDB{
			HumanLabel:       make([]byte, 0, 1024),
			HumanDescription: make([]byte, 0, 1024),
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
	q.SubQueries = nil
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
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, QueryType: %d, Query: %v", q.HumanLabel, q.HumanDescription, q.QueryType, q.SubQueries)

}
