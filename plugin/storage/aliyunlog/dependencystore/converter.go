// Copyright (c) 2017 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dependencystore

import (
	"strconv"
	"time"

	"github.com/aliyun/aliyun-log-go-sdk"
	"github.com/gogo/protobuf/proto"
	"github.com/jaegertracing/jaeger/model"
)

// FromDependencyLink converts a model.DependencyLink to a log record
func FromDependencyLinks(links []model.DependencyLink, topic, source string, ts time.Time) (*sls.LogGroup, error) {
	return converter{}.fromDependencyLinks(links, topic, source, ts)
}

// ToDependencyLink converts a log record to a model.DependencyLink
func ToDependencyLink(log map[string]string) (*model.DependencyLink, error) {
	return converter{}.toDependencyLink(log)
}

type converter struct{}

func (c converter) fromDependencyLinks(links []model.DependencyLink, topic, source string, ts time.Time) (*sls.LogGroup, error) {
	logs, err := c.fromDependencyLinkToLogs(links, ts)
	if err != nil {
		return nil, err
	}
	return &sls.LogGroup{
		Topic:  proto.String(topic),
		Source: proto.String(source),
		Logs:   logs,
	}, nil
}

func (c converter) fromDependencyLinkToLogs(links []model.DependencyLink, ts time.Time) ([]*sls.Log, error) {
	logTime := proto.Uint32(uint32(ts.Unix()))
	logs := make([]*sls.Log, 0, len(links))
	for _, link := range links {
		contents, err := c.fromDependencyLinkToLogContents(&link)
		if err != nil {
			return nil, err
		}

		logs = append(logs, &sls.Log{
			Time:     logTime,
			Contents: contents,
		})
	}

	return logs, nil
}

func (c converter) fromDependencyLinkToLogContents(link *model.DependencyLink) ([]*sls.LogContent, error) {
	contents := make([]*sls.LogContent, 0, 3)
	contents = c.appendContents(contents, parentFiled, link.Parent)
	contents = c.appendContents(contents, childField, link.Child)
	contents = c.appendContents(contents, callCountField, strconv.FormatUint(link.CallCount, 10))
	return contents, nil
}

func (c converter) appendContents(contents []*sls.LogContent, k, v string) []*sls.LogContent {
	content := sls.LogContent{
		Key:   proto.String(k),
		Value: proto.String(v),
	}
	return append(contents, &content)
}

func (c converter) toDependencyLink(log map[string]string) (*model.DependencyLink, error) {
	link := model.DependencyLink{}

	for k, v := range log {
		switch k {
		case parentFiled:
			link.Parent = v
		case childField:
			link.Child = v
		case callCountField:
			ct, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return nil, err
			}
			link.CallCount = ct
		}
	}
	return &link, nil
}
