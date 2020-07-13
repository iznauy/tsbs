package main

import (
	"github.com/iznauy/tsbs/load"
	"strings"
	"time"
)

type processor struct {
	backingOffChan chan bool
	backingOffDone chan struct{}
	client         *btrdbClient
}

func (p *processor) Init(workerNum int, doLoad bool) {
	p.client = NewBTrDBClient()
}

func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (metricCount, rowCount uint64) {
	batch := b.(*insertionBatch)
	if doLoad {
		if !useBatchInsert {
			count := 0
			for _, insert := range batch.insertions {
				if insert == nil {
					continue
				}
				err := p.client.insert(insert)

				if err != nil {
					if !strings.Contains(err.Error(), "DeadlineExceeded") {
						info("encounter error while inserting data into btrdb: %v", err)
					}
					count += 1
				}
				time.Sleep(backoff)
			}
			info("error rate in batch is: %d/%d", count, len(batch.insertions))
		} else {
			if err := p.client.batchInsert(batch); err != nil {
				info("encounter error while batch insert data into btrdb: %v", err)
			}
			info("Insert batch success!")
			time.Sleep(backoff)
		}
	}
	return batch.metrics, batch.rows
}

func (p *processor) Close(doLoad bool) {

}
