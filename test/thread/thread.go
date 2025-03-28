package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

type Task func(context.Context) error

type Pool struct {
	tasks   chan Task
	wg      sync.WaitGroup
	errOnce sync.Once
	err     error
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewPool(ctx context.Context, concurrency int) *Pool {
	ctx, cancel := context.WithCancel(ctx)
	p := &Pool{
		tasks:  make(chan Task),
		ctx:    ctx,
		cancel: cancel,
	}

	for i := 0; i < concurrency; i++ {
		p.wg.Add(1)
		go p.worker()
	}

	return p
}

func (p *Pool) worker() {
	defer p.wg.Done()
	for {
		select {
		case task, ok := <-p.tasks:
			if !ok {
				return
			}
			if err := task(p.ctx); err != nil {
				p.errOnce.Do(func() {
					p.err = err
					p.cancel()
				})
				return
			}
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *Pool) AddTask(task Task) {
	select {
	case <-p.ctx.Done():
		return
	case p.tasks <- task:
	}
}

func (p *Pool) Wait() error {
	close(p.tasks)
	p.wg.Wait()
	select {
	case <-p.ctx.Done():
		return p.err
	default:
		return nil
	}
}

type Msg struct {
	Data      string
	Err       string
	Completed bool
}

func (m Msg) String() string {
	js, _ := json.Marshal(&m)
	return string(js)
}

func main() {
	ctx := context.Background()
	pool := NewPool(ctx, 5)

	msg := make(chan Msg)

	go func() {
		for m := range msg {
			fmt.Println(m.String())
		}
	}()

	tasks := []Task{
		func(ctx context.Context) error {
			msg <- Msg{
				Data: "Task 1",
			}
			return nil
		},
		func(ctx context.Context) error {
			msg <- Msg{
				Data: "Task 2",
			}
			return nil
		},
		func(ctx context.Context) error {
			msg <- Msg{
				Data: "Task 3",
			}
			return errors.New("error in Task 3")
		},
		func(ctx context.Context) error {
			msg <- Msg{
				Data: "Task 4",
			}
			return nil
		},
		func(ctx context.Context) error {
			msg <- Msg{
				Data: "Task 5",
			}
			return nil
		},
		func(ctx context.Context) error {
			msg <- Msg{
				Data: "Task 6",
			}
			return nil
		},
		func(ctx context.Context) error {
			msg <- Msg{
				Data: "Task 7",
			}
			return nil
		},
		func(ctx context.Context) error {
			msg <- Msg{
				Data: "Task 8",
			}
			return nil
		},
		func(ctx context.Context) error {
			msg <- Msg{
				Data: "Task 9",
			}
			return nil
		},
	}

	for _, task := range tasks {
		pool.AddTask(task)
	}

	if err := pool.Wait(); err != nil {
		msg <- Msg{
			Err:       err.Error(),
			Completed: false,
		}
	} else {
		msg <- Msg{
			Data:      "All tasks completed successfully",
			Completed: true,
		}
	}
	close(msg) // Close the channel after all tasks are done
}
