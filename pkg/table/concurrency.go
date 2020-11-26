/*
Copyright © 2020 Marvin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package table

import (
	"fmt"
	"time"

	"github.com/WentaoJin/tidba/pkg/db"
	"github.com/WentaoJin/tidba/zlog"
	"go.uber.org/zap"
)

type Task struct {
	TaskID   int
	ThreadID int
	Engine   *db.Engine
	SQL      string
	Err      error
	Fn       func(db *db.Engine, sql string) error
}

func (task *Task) Do() error {
	return task.Fn(task.Engine, task.SQL)
}

type WorkerPool struct {
	PoolSize  int
	TasksChan chan Task
}

func NewWorkerPool(tasks []Task, poolSize int) *WorkerPool {
	tasksChan := make(chan Task, len(tasks))
	for _, task := range tasks {
		tasksChan <- task
	}
	close(tasksChan)
	pool := &WorkerPool{PoolSize: poolSize, TasksChan: tasksChan}
	return pool
}

func (pool *WorkerPool) Start() {
	for i := 0; i < pool.PoolSize; i++ {
		go pool.worker(i)
	}
}

func (pool *WorkerPool) worker(threadID int) {
	for task := range pool.TasksChan {
		task.ThreadID = threadID
		startTime := time.Now()
		task.Err = task.Do()
		endTime := time.Now()
		if task.Err != nil {
			zlog.Logger.Error("Run task failed", zap.Int("taskID", task.TaskID), zap.Int("threadID", task.ThreadID), zap.String("sql", task.SQL), zap.String("error", fmt.Sprintf("%v", task.Err)), zap.String("timeElapsed", fmt.Sprintf("%vs", endTime.Sub(startTime).String())))
		}
		zlog.Logger.Info("Run task success", zap.Int("taskID", task.TaskID), zap.Int("threadID", task.ThreadID), zap.String("sql", task.SQL), zap.String("timeElapsed", fmt.Sprintf("%vs", endTime.Sub(startTime).String())))
	}
}
