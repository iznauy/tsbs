package main

import (
	"context"
	"errors"
	pb "github.com/iznauy/BTrDB/grpcinterface"
	"github.com/iznauy/tsbs/query"
	"google.golang.org/grpc"
	"time"
)

type processor struct {
	client pb.BTrDBClient
}

func newProcessor() query.Processor {
	return &processor{}
}

func (p *processor) Init(_ int) {
	conn, err := grpc.Dial(url, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	p.client = pb.NewBTrDBClient(conn)
}

func (p *processor) ProcessQuery(q query.Query, _ bool) ([]*query.Stat, error) {
	qu := q.(*query.BTrDB)
	stat := query.GetStat()
	span := 0.0
	if qu.QueryType == query.QueryStatistics {
		for _, subquery := range qu.StatisticsSubQueries {
			partSpan, err := p.processStatisticsQuery(subquery)
			if err != nil {
				return nil, err
			}
			span += partSpan
		}
	} else if qu.QueryType == query.QueryNearest {
		for _, subquery := range qu.NearestSubQueries {
			partSpan, err := p.processNearestQuery(subquery)
			if err != nil {
				return nil, err
			}
			span += partSpan
		}
	} else if qu.QueryType == query.QueryRange {
		for _, subquery := range qu.RangeSubQueries {
			partSpan, err := p.processRangeQuery(subquery)
			if err != nil {
				return nil, err
			}
			span += partSpan
		}
	}
	stat.Init(q.HumanLabelName(), span)
	return []*query.Stat{stat}, nil
}

func (p *processor) processStatisticsQuery(req *pb.QueryStatisticsRequest) (span float64, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	start := time.Now()
	resp, err := p.client.QueryStatistics(ctx, req)
	span = float64(time.Since(start).Nanoseconds()) / 1e6

	if err != nil {
		return span, err
	}
	if resp.Status == nil {
		return span, errors.New("incomplete response")
	}
	if resp.Status.Code != 0 {
		return span, errors.New(resp.Status.Msg)
	}
	return span, nil
}

func (p *processor) processNearestQuery(req *pb.QueryNearestValueRequest) (span float64, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	start := time.Now()
	resp, err := p.client.QueryNearestValue(ctx, req)
	span = float64(time.Since(start).Nanoseconds()) / 1e6

	if err != nil {
		return span, err
	}
	if resp.Status == nil {
		return span, errors.New("incomplete response")
	}
	if resp.Status.Code != 0 {
		return span, errors.New(resp.Status.Msg)
	}
	return span, nil
}

func (p *processor) processRangeQuery(req *pb.QueryRangeRequest) (span float64, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	start := time.Now()
	resp, err := p.client.QueryRange(ctx, req)
	span = float64(time.Since(start).Nanoseconds()) / 1e6

	if err != nil {
		return span, err
	}
	if resp.Status == nil {
		return span, errors.New("incomplete response")
	}
	if resp.Status.Code != 0 {
		return span, errors.New(resp.Status.Msg)
	}
	return span, nil
}