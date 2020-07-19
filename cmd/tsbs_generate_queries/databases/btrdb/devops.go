package btrdb

import (
	"bufio"
	"compress/gzip"
	"crypto/md5"
	"fmt"
	"github.com/google/uuid"
	pb "github.com/iznauy/BTrDB/grpcinterface"
	"github.com/iznauy/tsbs/cmd/tsbs_generate_queries/databases"
	"github.com/iznauy/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/iznauy/tsbs/query"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

const path = "/tmp/btrdb-data.gz"

type Devops struct {
	*BaseGenerator
	*devops.Core

	meta map[int]map[string]string
}

func (d *Devops) init() error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gr.Close()

	br := bufio.NewReader(gr)
	for i := 0; i < 100; i++ {
		line, _, err := br.ReadLine()
		if err == io.EOF {
			break
		}
		metaInfo := strings.Split(string(line), "\t")[0]
		tagKVs := strings.Split(metaInfo, ",")
		measurement := tagKVs[0]
		host := 0
		for i := 1; i < len(tagKVs); i++ {
			if strings.HasPrefix(tagKVs[i], "hostname") {
				host, err = strconv.Atoi(strings.Split(tagKVs[i], "_")[1])
				if err != nil {
					return err
				}
				break
			}
		}
		hostMeta, ok := d.meta[host]
		if !ok {
			hostMeta = make(map[string]string, 128)
			d.meta[host] = hostMeta
		}
		hostMeta[measurement] = metaInfo
	}
	return nil
}

func (d *Devops) randomHost() int {
	return rand.Intn(len(d.meta))
}

func (d *Devops) getHostCount() int {
	return len(d.meta)
}

func (d *Devops) getUUID(host int, measurement string, field string) uuid.UUID {
	hostMeta, ok := d.meta[host]
	if !ok {
		panic(fmt.Sprintf("unexpected host: %d", host))
	}
	prefix, ok := hostMeta[measurement]
	if !ok {
		panic(fmt.Sprintf("unexpected measurement: %s", measurement))
	}
	data := md5.Sum([]byte(prefix + "," + field))
	id, err := uuid.FromBytes(data[:])
	if err != nil {
		panic(fmt.Sprintf("cannot generate uuid: %v", err))
	}
	return id
}

func (d *Devops) GroupByTime(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	databases.PanicIfErr(err)

	subQueries := make([]*pb.QueryStatisticsRequest, 0, nHosts*numMetrics)
	for i := 0; i < nHosts; i++ {
		host := d.randomHost()
		for _, metric := range metrics {
			id := d.getUUID(host, "cpu", metric)
			subQuery := &pb.QueryStatisticsRequest{
				Uuid:       []byte(id.String()),
				Start:      interval.Start().UnixNano(),
				End:        interval.End().UnixNano(),
				Resolution: 38, // 最接近 5 分钟的为 2^38 = 4分35秒
			}
			subQueries = append(subQueries, subQuery)
		}
	}

	humanLabel := fmt.Sprintf("BTrDB %d cpu metric(s), random %d hosts, random %s by 5m", numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, query.QueryStatistics, subQueries, nil)
}

func (d *Devops) GroupByOrderByLimit(qi query.Query) {
	panic("GroupByOrderByLimit not supported in BTrDB")
}

func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	databases.PanicIfErr(err)

	subQueries := make([]*pb.QueryStatisticsRequest, 0, d.getHostCount()*numMetrics)
	for host := 0; host < d.getHostCount(); host++ {
		for _, metric := range metrics {
			id := d.getUUID(host, "cpu", metric)
			subQuery := &pb.QueryStatisticsRequest{
				Uuid:       []byte(id.String()),
				Start:      interval.Start().UnixNano(),
				End:        interval.End().UnixNano(),
				Resolution: 42,
			}
			subQueries = append(subQueries, subQuery)
		}
	}

	humanLabel := devops.GetDoubleGroupByLabel("BTrDB", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, query.QueryStatistics, subQueries, nil)
}

func (d *Devops) MaxAllCPU(qi query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.MaxAllDuration)
	metrics := devops.GetAllCPUMetrics()

	subQueries := make([]*pb.QueryStatisticsRequest, 0, nHosts*len(metrics))
	for i := 0; i < nHosts; i++ {
		host := d.randomHost()
		for _, metric := range metrics {
			id := d.getUUID(host, "cpu", metric)
			subQuery := &pb.QueryStatisticsRequest{
				Uuid:       []byte(id.String()),
				Start:      interval.Start().UnixNano(),
				End:        interval.End().UnixNano(),
				Resolution: 42, // 最接近 60 分钟的为 2^42 = 73分18秒
			}
			subQueries = append(subQueries, subQuery)
		}
	}

	humanLabel := devops.GetMaxAllLabel("BTrDB", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, query.QueryStatistics, subQueries, nil)
}

func (d *Devops) LastPointPerHost(qi query.Query) {
	metrics := devops.GetAllCPUMetrics()
	subQueries := make([]*pb.QueryNearestValueRequest, 0, d.getHostCount()*len(metrics))

	for host := 0; host < d.getHostCount(); host++ {
		for _, metric := range metrics {
			id := d.getUUID(host, "cpu", metric)
			subQuery := &pb.QueryNearestValueRequest{
				Uuid:      []byte(id.String()),
				Time:      d.Interval.End().UnixNano(),
				Backwards: true,
			}
			subQueries = append(subQueries, subQuery)
		}
	}

	humanLabel := "BTrDB last row per host"
	humanDesc := humanLabel + ": cpu"
	d.fillInQuery(qi, humanLabel, humanDesc, query.QueryNearest, nil, subQueries)
}

func (d *Devops) HighCPUForHosts(qi query.Query, nHosts int) {
	panic("GroupByOrderByLimit not supported in BTrDB")
}
