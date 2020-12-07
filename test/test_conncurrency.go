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
package main

import (
	"fmt"
	"time"
)

func main() {
	fo()
	fmt.Println(1)
}
func fo() {
	for {
		t := time.Now()

		tasks := []Task{
			{Id: 1, fn: func() error {
				fmt.Println(11)
				return nil
			}},
			{Id: 2, fn: func() error {
				fmt.Println(112)
				return nil
			}},
		}
		pool := NewWorkerPoo(tasks, 2)
		pool.Start()

		tasks = pool.Results()
		fmt.Printf("all tasks finished, timeElapsed: %f s\n", time.Now().Sub(t).Seconds())
		for _, task := range tasks {
			fmt.Printf("result of task %d is %v\n", task.Id, task.Err)
		}
	}

}

type Task struct {
	Id  int
	Err error
	fn  func() error
}

func (task *Task) Do() error {
	return task.fn()
}

type WorkerPoo struct {
	PoolSize    int
	TaskSize    int
	TasksChan   chan Task
	ResultsChan chan Task
	Results     func() []Task
}

func NewWorkerPoo(tasks []Task, poolSize int) *WorkerPoo {
	tasksChan := make(chan Task, len(tasks))
	resultsChan := make(chan Task, len(tasks))
	for _, task := range tasks {
		tasksChan <- task
	}
	close(tasksChan)
	pool := &WorkerPoo{PoolSize: poolSize, TaskSize: len(tasks), TasksChan: tasksChan, ResultsChan: resultsChan}
	pool.Results = pool.results
	return pool
}

func (pool *WorkerPoo) Start() {
	for i := 0; i < pool.PoolSize; i++ {
		go pool.worker()
	}
}

func (pool *WorkerPoo) worker() {
	for task := range pool.TasksChan {
		task.Err = task.Do()
		pool.ResultsChan <- task
	}
}

func (pool *WorkerPoo) results() []Task {
	tasks := make([]Task, pool.TaskSize)
	for i := 0; i < pool.TaskSize; i++ {
		tasks[i] = <-pool.ResultsChan
	}
	return tasks
}
