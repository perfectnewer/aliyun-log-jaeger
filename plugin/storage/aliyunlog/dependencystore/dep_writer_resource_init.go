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
	"time"

	"github.com/aliyun/aliyun-log-go-sdk"
	"go.uber.org/zap"
)

var indexJSON = `
{"keys":{"callCount":{"alias":"","doc_value":true,"type":"long"},"child":{"alias":"","caseSensitive":false,"chn":false,"doc_value":true,"token":[","," ","'","\"",";","=","(",")","[","]","{","}","?","@","&","<",">","/",":","\n","\t","\r"],"type":"text"},"parent":{"alias":"","caseSensitive":false,"chn":false,"doc_value":true,"token":[","," ","'","\"",";","=","(",")","[","]","{","}","?","@","&","<",">","/",":","\n","\t","\r"],"type":"text"}},"line":{"caseSensitive":false,"chn":false,"token":[","," ","'","\"",";","=","(",")","[","]","{","}","?","@","&","<",">","/",":","\n","\t","\r"]}}
`

func retryCreateIndex(logger *zap.Logger, client sls.ClientInterface, project, logstore, indexStr string) (err error) {
	// create index, create index do not return error
	createFlag := true
	for i := 0; i < 10; i++ {
		if createFlag {
			err = client.CreateIndexString(project, logstore, indexStr)
		} else {
			err = client.UpdateIndexString(project, logstore, indexStr)
		}
		if err != nil {
			// if IndexAlreadyExist, just return
			if clientError, ok := err.(*sls.Error); ok && clientError.Code == "IndexAlreadyExist" {
				logger.With(zap.String("project", project)).
					With(zap.String("logstore", logstore)).
					Info("index already exist, try update index")
				createFlag = false
				continue
			}
			time.Sleep(time.Second)
		} else {
			logger.With(zap.String("project", project)).With(zap.String("logstore", logstore)).Info("create or update index success")
			break
		}
	}
	return err
}

func makesureLogstoreExist(logger *zap.Logger, client sls.ClientInterface, project, logstore string, shardCount, lifeCycle int) (new bool, err error) {
	for i := 0; i < 5; i++ {
		if ok, err := client.CheckLogstoreExist(project, logstore); err != nil {
			time.Sleep(time.Millisecond * 100)
		} else {
			if ok {
				return false, nil
			}
			break
		}
	}
	ttl := 180
	if shardCount <= 0 {
		shardCount = 2
	}
	// @note max init shard count limit : 10
	if shardCount > 10 {
		shardCount = 10
	}
	for i := 0; i < 5; i++ {
		err = client.CreateLogStore(project, logstore, ttl, shardCount, true, 32)
		if err != nil {
			time.Sleep(time.Millisecond * 100)
		} else {
			logger.With(zap.String("project", project)).With(zap.String("logstore", logstore)).Info("create logstore success")
			break
		}
	}
	if err != nil {
		return true, err
	}
	// after create logstore success, wait 1 sec
	time.Sleep(time.Second)
	return true, nil
}

func makesureProjectExist(logger *zap.Logger, client sls.ClientInterface, project string) error {
	ok := false
	var err error

	for i := 0; i < 5; i++ {
		if ok, err = client.CheckProjectExist(project); err != nil {
			time.Sleep(time.Millisecond * 100)
		} else {
			break
		}
	}
	if ok {
		return nil
	}
	for i := 0; i < 5; i++ {
		_, err = client.CreateProject(project, "istio log project, created by alibaba cloud jeager collector")
		if err != nil {
			time.Sleep(time.Millisecond * 100)
		} else {
			logger.With(zap.String("project", project)).Info("create project success")
			break
		}
	}
	return err
}

// InitDependencyWriterLogstoreResource create project, logstore, index for jeager query
func InitDependencyWriterLogstoreResource(client sls.ClientInterface, project string, logstore string, logger *zap.Logger) error {
	if err := makesureProjectExist(logger, client, project); err != nil {
		return err
	}
	if _, err := makesureLogstoreExist(logger, client, project, logstore, 2, 90); err != nil {
		return err
	}
	if err := retryCreateIndex(logger, client, project, logstore, indexJSON); err != nil {
		return err
	}
	return nil
}
