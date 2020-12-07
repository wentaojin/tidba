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
)

// 任务的属性应该是一个业务函数
type Job struct {
	id int
	f  func() error // 函数名f, 无参，返回值为error
}

// 创建Task任务
func NewTask(id int, arg_f func() error) *Job {
	task := Job{
		id: id,
		f:  arg_f,
	}
	return &task
}

// Task绑定业务方法
func (task *Job) Execute() {
	task.f() // 调用任务中已经绑定好的业务方法
}

// ------------------------------------------------
type Pool struct {
	EntryChannel chan *Job // 对外的Task入口
	JobsChannel  chan *Job // 内部的Task队列
	workerNum    int       // 协程池中最大的woker数量
}

// 创建Pool
func NewPool(cap int) *Pool {
	pool := Pool{
		EntryChannel: make(chan *Job),
		JobsChannel:  make(chan *Job),
		workerNum:    cap,
	}
	return &pool
}

// Pool绑定干活的方法
func (pool *Pool) worker(workID int) {
	// worker工作 ： 永久从JobsChannel取任务 然后执行任务
	for task := range pool.JobsChannel {
		task.Execute()
		fmt.Println("task ID", task.id, "work ID ", workID, " has executed")
	}
}

// Pool绑定协程池工作方法
func (pool *Pool) run() {
	// 定义worker数量
	for i := 0; i < pool.workerNum; i++ {
		go pool.worker(i)
	}

	// 从EntryChannel去任务，发送给JobsChannel
	for task := range pool.EntryChannel {
		pool.JobsChannel <- task // 添加task优先级排序逻辑
	}
}

// ------------------------------------------------
func main() {
	fg()
}

func fg() {
	// 创建一些任务
	var tasks []*Job
	for i := 0; i <= 10; i++ {
		tasks = append(tasks, NewTask(i, func() error { // 匿名函数
			fmt.Println("marvin")
			return nil
		}))
	}

	// 创建协程池
	pool := NewPool(4)

	// 创建多任务，抛给协程池
	// 开启新的协程，防止阻塞

	go func() {
		defer close(pool.EntryChannel)
		for _, task := range tasks {
			pool.EntryChannel <- task
		}
	}()
	// 启动协程池
	pool.run()
}
