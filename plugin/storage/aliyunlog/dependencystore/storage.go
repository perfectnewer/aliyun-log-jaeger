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
	"context"
	"time"

	"github.com/aliyun/aliyun-log-go-sdk"
	"github.com/jaegertracing/jaeger/model"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	defaultPageSizeForSpan = 1000
	parentFiled            = "parent"
	childField             = "child"
	callCountField         = "callCount"
	emptyTopic             = ""
	noSource               = "0.0.0.0"
)

// DependencyStore handles all queries and insertions to AliCloud Log Service dependencies
type DependencyStore struct {
	ctx      context.Context
	client   sls.ClientInterface
	project  string
	logstore string
	logger   *zap.Logger
}

// NewDependencyStore returns a DependencyStore
func NewDependencyStore(client sls.ClientInterface, project string, logstore string, logger *zap.Logger) *DependencyStore {
	store := &DependencyStore{
		ctx:      context.Background(),
		client:   client,
		project:  project,
		logstore: logstore,
		logger:   logger,
	}

	if store.valid() {
		if err := InitDependencyWriterLogstoreResource(store.client, store.project, store.logstore, store.logger); err != nil {
			store.logger.Error("Init dependency logstore failed",
				zap.String("project", store.project),
				zap.String("logstore", store.logstore),
				zap.Error(err))
		}
	}

	return store
}

func (s *DependencyStore) valid() bool {
	return s.client != nil && s.logstore != ""
}

// WriteDependencies implements dependencystore.Writer#WriteDependencies.
func (s *DependencyStore) WriteDependencies(ts time.Time, dependencies []model.DependencyLink) error {
	if !s.valid() {
		return nil
	}

	logGroup, err := FromDependencyLinks(dependencies, emptyTopic, noSource, ts)
	if err != nil {
		s.logger.Error("Failed to format dependencies to sls log", zap.Error(err))
		return err
	}

	s.logger.Info("Start to write dependency logs", zap.String("project", s.project), zap.String("logstore", s.logstore))
	err = s.client.PutLogs(s.project, s.logstore, logGroup)
	if err != nil {
		s.logger.Error("Failed to write dependencies", zap.Error(err))
	}
	return err
}

// GetDependencies returns all interservice dependencies
func (s *DependencyStore) GetDependencies(endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	if !s.valid() {
		return []model.DependencyLink{}, nil
	}

	from := endTs.Add(-lookback).Unix()
	to := endTs.Unix()
	return s.getDependencies(from, to)
}

func (s *DependencyStore) getDependencies(from, to int64) ([]model.DependencyLink, error) {
	s.logger.
		With(zap.Int64("from", from)).
		With(zap.Int64("to", to)).
		Info("Trying to get dependencies")

	topic := emptyTopic
	maxLineNum := int64(defaultPageSizeForSpan)
	offset := int64(0)
	reverse := false

	count, err := s.getDependenciesCount(topic, from, to)
	if err != nil {
		s.logger.
			With(zap.Int64("from", from)).
			With(zap.Int64("to", to)).
			With(zap.Error(err)).
			Error("Get dependency count failed")

		return nil, err
	}

	dependencies := make([]model.DependencyLink, 0)
	for curCount := int64(0); curCount < count; {
		s.logGetLogsParameters(topic, from, to, "", maxLineNum, offset, reverse, "Trying to get dependencies")
		resp, err := s.client.GetLogs(s.project, s.logstore, topic, from, to, "", maxLineNum, offset, reverse)
		if err != nil {
			return nil, errors.Wrap(err, "Get dependencies failed")
		}
		for _, log := range resp.Logs {
			dependency, err := ToDependencyLink(log)
			if err != nil {
				return nil, err
			}
			dependencies = append(dependencies, *dependency)
		}
		curCount += resp.Count
		offset += resp.Count
	}

	return dependencies, nil
}

func (s *DependencyStore) getDependenciesCount(topic string, from, to int64) (int64, error) {
	s.logGetHistograms(topic, s.project, s.logstore, from, to, "Trying to get count of dependencies")

	resp, err := s.client.GetHistograms(s.project, s.logstore, topic, from, to, "")
	if err != nil {
		return 0, errors.Wrap(err, "Failed to get dependencies count")
	}
	return resp.Count, nil
}

func (s *DependencyStore) logGetHistograms(topic, project, logstore string, from int64, to int64, msg string) {
	s.logger.
		With(zap.String("topic", topic)).
		With(zap.String("project", project)).
		With(zap.String("logstore", logstore)).
		With(zap.Int64("from", from)).
		With(zap.Int64("to", to)).
		Info(msg)
}

func (s *DependencyStore) logGetLogsParameters(topic string, from int64, to int64, queryExp string, maxLineNum int64, offset int64, reverse bool, msg string) {
	s.logger.
		With(zap.String("topic", topic)).
		With(zap.Int64("from", from)).
		With(zap.Int64("to", to)).
		With(zap.String("queryExp", queryExp)).
		With(zap.Int64("maxLineNum", maxLineNum)).
		With(zap.Int64("offset", offset)).
		With(zap.Bool("reverse", reverse)).
		Info(msg)
}
