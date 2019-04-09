package app

import (
	"flag"
	"fmt"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/model/adjuster"
	"github.com/jaegertracing/jaeger/plugin/storage/aliyunlog/spanstore"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/spf13/viper"
)

const (
	fromDataStr        = "from-date"
	toDataStr          = "to-date"
	defaultFromDataStr = "1970-01-01"
)

// AddFlags adds flags for DependencyCalculator
func AddFlags(flags *flag.FlagSet) {
	flags.String(
		fromDataStr,
		defaultFromDataStr,
		"start date of traces to be processed. e.g. 2018-03-27")
	flags.String(
		toDataStr,
		"",
		"start date of traces to be processed. e.g. 2018-03-27")
}

// StartOption opetions for calculator
type StartOption struct {
	queryStartDateStr string
	queryEndDateStr   string
}

// InitFromViper init options from viper
func (o *StartOption) InitFromViper(v *viper.Viper) *StartOption {
	o.queryStartDateStr = v.GetString(fromDataStr)
	o.queryEndDateStr = v.GetString(toDataStr)
	return o
}

// DependencyCalculator 计算依赖关系，存入数据库
// TODO
// 取数据: 流试区，流试计算.需要按tracid 聚合
// 最后合并。存入数据库
type DependencyCalculator struct {
	startOption      *StartOption
	spanLogStorage   *LogStorage
	dependencyWriter dependencystore.Writer
}

// NewDependencyCalculator calculate dependency of trace
func NewDependencyCalculator(logStorage *LogStorage, dependencyWriter dependencystore.Writer) *DependencyCalculator {
	return &DependencyCalculator{
		spanLogStorage:   logStorage,
		dependencyWriter: dependencyWriter,
	}
}

func (dc *DependencyCalculator) ParseDate(date string) (time.Time, error) {
	return time.Parse("2006-01-02", date)
}

// Start start calculate dependency
func (dc *DependencyCalculator) Start(option *StartOption) error {
	dc.startOption = option

	from, err := dc.ParseDate(dc.startOption.queryStartDateStr)
	if err != nil {
		return err
	}

	var to time.Time
	if dc.startOption.queryEndDateStr != "" {
		to, err = dc.ParseDate(dc.startOption.queryEndDateStr)
		if err != nil {
			to = time.Now()
		}
	} else {
		to = time.Now()
	}

	allTraces := make(map[string]*model.Trace)
	if err := dc.spanLogStorage.QuerySpans(from, to, func(logs []map[string]string) error {
		traces, err := spanstore.ToTraces(logs)
		if err != nil {
			return err
		}

		for _, newTrace := range traces {
			traceID := newTrace.Spans[0].TraceID.String()
			if trace, ok := allTraces[traceID]; ok {
				trace.Spans = append(trace.Spans, newTrace.Spans...)
			} else {
				allTraces[traceID] = newTrace
			}
		}
		return nil
	}); err != nil {
		return err
	}

	deps := map[string]*model.DependencyLink{}
	deduper := adjuster.SpanIDDeduper()
	for _, oriTrace := range allTraces {
		trace, err := deduper.Adjust(oriTrace)
		if err != nil {
			// FIXME logger
			continue
		}

		for _, s := range trace.Spans {
			parentSpan := findSpan(trace, s.ParentSpanID)
			if parentSpan != nil {
				if parentSpan.Process.ServiceName == s.Process.ServiceName {
					continue
				}
				depKey := parentSpan.Process.ServiceName + "&&&" + s.Process.ServiceName
				if _, ok := deps[depKey]; !ok {
					deps[depKey] = &model.DependencyLink{
						Parent:    parentSpan.Process.ServiceName,
						Child:     s.Process.ServiceName,
						CallCount: 1,
					}
				} else {
					deps[depKey].CallCount++
				}
			}
		}
	}

	depList := make([]model.DependencyLink, 0, len(deps))
	for key, dep := range deps {
		fmt.Println(key, dep)

		depList = append(depList, *dep)
	}

	err = dc.dependencyWriter.WriteDependencies(time.Now(), depList)
	return err
}

func findSpan(trace *model.Trace, spanID model.SpanID) *model.Span {
	for _, s := range trace.Spans {
		if s.SpanID == spanID {
			return s
		}
	}
	return nil
}
