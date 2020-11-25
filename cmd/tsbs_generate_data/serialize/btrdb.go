package serialize

import (
	"crypto/md5"
	"fmt"
	"github.com/google/uuid"
	"io"
	"math/rand"
	"os"
)

const probFile = "~/prob.txt"

type BTrDBSerializer struct {
	FilterMap map[string]float64
	fd        *os.File
}

func NewBTrDBSerializer() *BTrDBSerializer {
	fd, err := os.OpenFile(probFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	return &BTrDBSerializer{
		FilterMap: map[string]float64{},
		fd:        fd,
	}
}

func (s *BTrDBSerializer) Serialize(p *Point, w io.Writer) error {
	// prefix = measurement + ',' + tagkv + ' ' + timestamp + ' '
	prefix := make([]byte, 0, 1024)
	prefix = append(prefix, p.measurementName...)

	for i := 0; i < len(p.tagKeys); i++ {
		if p.tagValues[i] == nil {
			continue
		}
		prefix = append(prefix, ',')
		prefix = append(prefix, p.tagKeys[i]...)
		prefix = append(prefix, '=')
		prefix = append(prefix, []byte(fmt.Sprint(p.tagValues[i]))...)
	}

	keyPrefix := string(prefix)
	prefix = append(prefix, '\t')
	prefix = fastFormatAppend(p.timestamp.UTC().UnixNano(), prefix)
	prefix = append(prefix, '\t')

	buf := make([]byte, 0, 64)
	for i := 0; i < len(p.fieldKeys); i++ {
		if p.fieldValues[i] == nil {
			continue
		}
		key := keyPrefix + "," + string(p.fieldKeys[i])
		// 计算一下当前 metrics 是否会被过滤
		prob, ok := s.FilterMap[key]
		if !ok {
			prob = 0.1 + rand.Float64()*0.9
			keyMD5 := md5.Sum([]byte(key))
			id, _ := uuid.FromBytes(keyMD5[:])
			if _, err := s.fd.WriteString(fmt.Sprintf("%s %f\n", id.String(), prob)); err != nil {
				panic(err)
			}
			if err := s.fd.Sync(); err != nil {
				panic(err)
			}
			s.FilterMap[key] = prob
		}
		if rand.Float64() > prob {
			continue
		}
		buf = append(buf, ',')
		buf = append(buf, p.fieldKeys[i]...)
		buf = append(buf, '=')
		val := p.fieldValues[i]
		switch val.(type) {
		case int:
			buf = append(buf, []byte(fmt.Sprint(val.(int)))...)
		case int32:
			buf = append(buf, []byte(fmt.Sprint(val.(int32)))...)
		case int64:
			buf = append(buf, []byte(fmt.Sprint(val.(int64)))...)
		case float32:
			buf = append(buf, []byte(fmt.Sprint(val.(float32)))...)
		case float64:
			buf = append(buf, []byte(fmt.Sprint(val.(float64)))...)
		default:
			// btrdb 只支持数字类型
			continue
		}
	}

	if len(buf) == 0 { // 运气不太好，所有的时间序列都被过滤掉了
		return nil
	}
	buf = append(buf, '\n')
	if _, err := w.Write(prefix); err != nil {
		return err
	}
	if _, err := w.Write(buf[1:]); err != nil {
		return err
	}
	return nil
}
