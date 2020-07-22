package main

import (
	"context"
	"errors"
	"fmt"
	pb "github.com/iznauy/BTrDB/grpcinterface"
	"google.golang.org/grpc"
	"time"
)

type btrdbClient struct {
	client pb.BTrDBClient
}

func NewBTrDBClient() *btrdbClient {
	maxSize := 200 * 1024 * 1024
	diaOpt := grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxSize), grpc.MaxCallSendMsgSize(maxSize))
	conn, err := grpc.Dial(url, grpc.WithInsecure(), diaOpt)
	if err != nil {
		panic(err)
	}
	return &btrdbClient{
		client: pb.NewBTrDBClient(conn),
	}
}

func (c *btrdbClient) insert(insert *insertion) error {
	if insert == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1 * time.Second)
	defer cancel()
	req := &pb.InsertRequest{
		Uuid:   []byte(insert.Uuid),
		Values: insert.Points,
	}
	resp, err := c.client.Insert(ctx, req)
	if err != nil {
		return err
	}
	if resp.Status == nil {
		return errors.New("incomplete response")
	}
	if resp.Status.Code != 0 {
		return errors.New(resp.Status.Msg)
	}
	return nil
}

func (c *btrdbClient) batchInsert(b *insertionBatch) error {
	if b == nil {
		return nil
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 20 * time.Second)
	defer cancel()
	req := &pb.BatchInsertRequest{
		Inserts: make([]*pb.InsertRequest, 0, len(b.insertions)),
	}
	for _, insert := range b.insertions {
		req.Inserts = append(req.Inserts, &pb.InsertRequest{
			Uuid:   []byte(insert.Uuid),
			Values: insert.Points,
		})
	}
	resp, err := c.client.BatchInsert(ctx, req)
	if err != nil {
		return err
	}
	if resp.Status == nil {
		return errors.New("incomplete response")
	}
	if resp.Status.Code != 0 {
		return errors.New(resp.Status.Msg)
	}
	span := time.Now().Sub(start)
	fmt.Println("batch insert 序列化 + 请求耗时为：", span)
	return nil
}