package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
)

type task struct {
	index    int
	progress float64
	done     bool
}

type model struct {
	tasks     []task
	spinners  []spinner.Model
	progress  []progress.Model
	active    int
	completed int
	wg        *sync.WaitGroup
}

func newModel(taskCount, concurrency int) model {
	tasks := make([]task, taskCount)
	spinners := make([]spinner.Model, concurrency)
	progressBars := make([]progress.Model, concurrency)

	for i := 0; i < concurrency; i++ {
		sp := spinner.NewModel()
		sp.Spinner = spinner.Dot
		spinners[i] = sp

		pb := progress.NewModel(progress.WithScaledGradient("#FF7CCB", "#FDFF8C"))
		progressBars[i] = pb
	}

	return model{
		tasks:     tasks,
		spinners:  spinners,
		progress:  progressBars,
		active:    concurrency,
		completed: 0,
		wg:        &sync.WaitGroup{},
	}
}

func (m model) Init() tea.Cmd {
	return tea.Batch(m.startTasks()...)
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		if msg.String() == "q" {
			return m, tea.Quit
		}
	case taskMsg:
		m.tasks[msg.index].progress = msg.progress
		if msg.progress >= 1.0 {
			m.tasks[msg.index].done = true
			m.completed++
			m.active--

			if m.completed >= len(m.tasks) {
				return m, tea.Quit
			}

			next := m.completed + m.active
			if next < len(m.tasks) {
				m.active++
				cmds = append(cmds, m.startTask(next))
			}
		}
	}

	for i := range m.spinners {
		if i >= len(m.tasks) || m.tasks[i].done {
			continue
		}
		sp, cmd := m.spinners[i].Update(msg)
		m.spinners[i] = sp
		cmds = append(cmds, cmd)
	}

	for i := range m.progress {
		if i >= len(m.tasks) || m.tasks[i].done {
			continue
		}
		pb, cmd := m.progress[i].Update(msg)
		m.progress[i] = pb.(progress.Model) // 进行类型断言
		cmds = append(cmds, cmd)
	}

	return m, tea.Batch(cmds...)
}

func (m model) View() string {
	var result string

	for i := range m.tasks {
		if i >= len(m.progress) || i >= len(m.spinners) {
			continue
		}
		if m.tasks[i].done {
			result += fmt.Sprintf("Task %d: %s\n", i+1, m.progress[i].ViewAs(1.0))
		} else {
			result += fmt.Sprintf("Task %d: %s %s\n", i+1, m.spinners[i].View(), m.progress[i].ViewAs(m.tasks[i].progress))
		}
	}

	return result
}

type taskMsg struct {
	index    int
	progress float64
}

func (m *model) startTasks() []tea.Cmd {
	cmds := make([]tea.Cmd, len(m.spinners))
	for i := range m.spinners {
		cmds[i] = m.startTask(i)
	}
	return cmds
}

func (m *model) startTask(index int) tea.Cmd {
	m.wg.Add(1)
	return func() tea.Msg {
		defer m.wg.Done()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		for progress := 0.0; progress <= 1.0; progress += 0.1 {
			select {
			case <-time.After(time.Duration(rand.Intn(500)+500) * time.Millisecond):
				return taskMsg{index, progress}
			case <-ctx.Done():
				return nil
			}
		}
		return taskMsg{index, 1.0}
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	p := tea.NewProgram(newModel(100, 5))
	if err := p.Start(); err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}
