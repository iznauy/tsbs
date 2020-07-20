package main

import (
	"bufio"
	"crypto/md5"
	"github.com/google/uuid"
	pb "github.com/iznauy/BTrDB/grpcinterface"
	"github.com/iznauy/tsbs/load"
	"strconv"
	"strings"
	"time"
)

type point struct {
	key       string
	timestamp int64
	values    map[string]float64
}

type insertion struct {
	Uuid   string
	Points []*pb.RawPoint
}

type insertionBatch struct {
	insertions map[[16]byte]*insertion
	rows       uint64
	metrics    uint64
}

func (b *insertionBatch) Len() int {
	return int(b.rows)
}

func (b *insertionBatch) Append(item *load.Point) {
	p := item.Data.(*point)
	for subKey, value := range p.values {
		b.metrics += 1
		key := md5.Sum([]byte(p.key + "," + subKey))
		id, _ := uuid.FromBytes(key[:])
		insert, ok := b.insertions[[16]byte(id)]
		if !ok {
			insert = &insertion{
				Uuid:   id.String(),
				Points: make([]*pb.RawPoint, 0, 16),
			}
		}
		insert.Points = append(insert.Points, &pb.RawPoint{Time: p.timestamp, Value: value})
		b.insertions[[16]byte(id)] = insert
	}
	b.rows += 1
}

type factory struct{}

func (f *factory) New() load.Batch {
	return &insertionBatch{
		insertions: make(map[[16]byte]*insertion, 128),
		rows:       0,
		metrics:    0,
	}
}

type decoder struct {
	scanner *bufio.Scanner
}

func (d *decoder) Decode(_ *bufio.Reader) *load.Point {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil {
		return nil
	} else if !ok {
		fatal("scan error: %v", d.scanner.Err())
		return nil
	}

	parts := strings.Split(d.scanner.Text(), "\t")
	if len(parts) != 3 {
		fatal("incorrect point format, points must has three parts")
		return nil
	}
	prefix := parts[0]
	ts, err := parseTime(parts[1])
	if err != nil {
		fatal("cannot parse timestamp: %v", err)
		return nil
	}
	values := parseValues(parts[2])

	return load.NewPoint(&point{
		key:       prefix,
		timestamp: ts.UnixNano(),
		values:    values,
	})
}

func parseTime(v string) (time.Time, error) {
	ts, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, ts), nil
}

func parseValues(s string) map[string]float64 {
	entries := strings.Split(s, ",")
	values := make(map[string]float64, len(entries))
	for _, entry := range entries {
		parts := strings.Split(entry, "=")
		if len(parts) != 2 {
			panic("incorrect point format, points field must has two parts")
		}
		value, _ := strconv.ParseFloat(parts[1], 64)
		values[parts[0]] = value
	}
	return values
}
