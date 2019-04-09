package app

import (
	"time"

	"github.com/aliyun/aliyun-log-go-sdk"
	"github.com/jaegertracing/jaeger/pkg/aliyunlog/config"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	emptyTopic             = ""
	defaultPageSizeForSpan = 1000
)

// LogStorage
type LogStorage struct {
	client         sls.ClientInterface
	depClient      sls.ClientInterface
	logger         *zap.Logger
	project        string
	spanLogStorage string
	depLogStorage  string
}

// NewLogStorage create logstorage
func NewLogStorage(cfg *config.Configuration, logger *zap.Logger) (*LogStorage, error) {
	var err error
	ls := LogStorage{
		logger: logger,
	}
	ls.client, ls.project, ls.spanLogStorage, err = cfg.NewClient(config.SpanType)

	return &ls, err
}

// QuerySpans query sls span logs between [from, to)
// processFunc maybe called multi times
func (ls *LogStorage) QuerySpans(from, to time.Time, processFunc func(logs []map[string]string) error) error {
	fromTimestamp := from.Unix()
	toTimestamp := to.Unix()

	ls.logger.
		With(zap.Int64("from", fromTimestamp)).
		With(zap.Int64("to", toTimestamp)).
		Info("Trying to get spans")

	topic := emptyTopic
	maxLineNum := int64(defaultPageSizeForSpan)
	offset := int64(0)
	reverse := false

	count, err := ls.getSpansCount(topic, fromTimestamp, toTimestamp)
	if err != nil {
		return err
	}

	ls.logger.
		With(zap.Int64("from", fromTimestamp)).
		With(zap.Int64("to", toTimestamp)).
		With(zap.Int64("count", count)).
		Info("Get span count")

	if count == 0 {
		return nil
	}
	curCount := int64(0)
	for {
		ls.logGetLogsParameters(topic, fromTimestamp, toTimestamp, "", maxLineNum, offset, reverse, "Trying to get span logs")
		resp, err := ls.client.GetLogs(ls.project, ls.spanLogStorage, topic, fromTimestamp, toTimestamp, "", maxLineNum, offset, reverse)
		if err != nil {
			return errors.Wrap(err, "Get span logs failed")
		}

		if err := processFunc(resp.Logs); err != nil {
			return errors.Wrap(err, "Callback process span failed")
		}
		curCount += resp.Count
		if curCount >= count {
			break
		}
		offset += resp.Count
	}

	return nil
}

func (ls *LogStorage) getSpansCount(topic string, from, to int64) (int64, error) {
	ls.logger.
		With(zap.Int64("from_time", from)).
		With(zap.Int64("to_time", to)).
		With(zap.String("topic", topic)).
		Info("Trying to get spans count")

	resp, err := ls.client.GetHistograms(ls.project, ls.spanLogStorage, topic, from, to, "")
	if err != nil {
		return 0, errors.Wrap(err, "Get span log failed")
	}
	return resp.Count, nil
}

func (ls *LogStorage) logGetLogsParameters(topic string, from int64, to int64, queryExp string, maxLineNum int64, offset int64, reverse bool, msg string) {
	ls.logger.
		With(zap.String("topic", topic)).
		With(zap.Int64("from", from)).
		With(zap.Int64("to", to)).
		With(zap.String("queryExp", queryExp)).
		With(zap.Int64("maxLineNum", maxLineNum)).
		With(zap.Int64("offset", offset)).
		With(zap.Bool("reverse", reverse)).
		Info(msg)
}
