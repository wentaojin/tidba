package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/mattn/go-isatty"
)

var (
	mainStyle = lipgloss.NewStyle().MarginLeft(1)
)

func main() {
	var (
		daemonMode bool
		showHelp   bool
		opts       []tea.ProgramOption
	)

	flag.BoolVar(&daemonMode, "d", false, "run as a daemon")
	flag.BoolVar(&showHelp, "h", false, "show help")
	flag.Parse()

	if showHelp {
		flag.Usage()
		os.Exit(0)
	}

	if daemonMode || !isatty.IsTerminal(os.Stdout.Fd()) {
		// If we're in daemon mode don't render the TUI
		opts = []tea.ProgramOption{tea.WithoutRenderer()}
	} else {
		// If we're in TUI mode, discard log output
		log.SetOutput(io.Discard)
	}

	p := tea.NewProgram(newModel(), opts...)
	if _, err := p.Run(); err != nil {
		fmt.Println("Error starting Bubble Tea program:", err)
		os.Exit(1)
	}
}

type model struct {
	spinner   spinner.Model
	results   []respMsg
	msgChan   chan respMsg
	completed bool
}

func newModel() model {
	const showLastResults = 10

	sp := spinner.New()
	sp.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("206"))

	return model{
		spinner: sp,
		results: make([]respMsg, showLastResults),
		msgChan: make(chan respMsg, 1024),
	}
}

func (m model) Init() tea.Cmd {
	log.Println("Starting work...")
	return tea.Batch(
		m.spinner.Tick,
		listenForActivity(m.msgChan), // generate activity
		waitForActivity(m.msgChan),   // wait for activity
	)
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		return m, tea.Quit
	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	case respMsg:
		log.Printf("%d %s Job finished in %s", msg.id, msg.emoji, time.Duration(msg.duration))
		m.results = append(m.results[1:], msg)
		m.completed = msg.completed
		if m.completed {
			return m, tea.Quit
		}
		return m, waitForActivity(m.msgChan) // wait for next event
	default:
		return m, nil
	}
}

func (m model) View() string {
	var s string

	if m.completed {
		s = "✅ Splited successfully!\n\n"
	} else {
		s = m.spinner.View() + " Doing some work...(ctrl+c to quit)\n\n"
	}

	for _, res := range m.results {
		// if res.duration == 0 {
		// 	s += "........................\n"
		// } else {
		s += fmt.Sprintf("%d %s Job finished in %s\n", res.id, res.emoji, res.duration)
		// }
	}

	return mainStyle.Render(s)
}

func listenForActivity(ch chan respMsg) tea.Cmd {
	return func() tea.Msg {
		const numProcesses = 100
		var wg sync.WaitGroup
		wg.Add(numProcesses)

		for i := 0; i < numProcesses; i++ {
			go func(i int) {
				defer wg.Done()
				pause := time.Duration(rand.Int63n(899)+100) * time.Millisecond // nolint:gosec
				time.Sleep(pause)
				ch <- respMsg{id: i, duration: pause, emoji: randomEmoji(), completed: false}
			}(i)
		}

		go func() {
			wg.Wait()
			ch <- respMsg{completed: true}
			close(ch)
		}()

		return nil
	}
}

// A command that waits for the activity on a channel.
func waitForActivity(sub chan respMsg) tea.Cmd {
	return func() tea.Msg {
		return <-sub
	}
}

type respMsg struct {
	id        int
	duration  time.Duration
	emoji     string
	completed bool
}

func randomEmoji() string {
	emojis := []rune("🍦🧋🍡🤠👾😭🦊🐯🦆🥨🎏🍔🍒🍥🎮📦🦁🐶🐸🍕🥐🧲🚒🥇🏆🌽")
	return string(emojis[rand.Intn(len(emojis))]) // nolint:gosec
}
