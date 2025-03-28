package main

// A simple example that shows how to send messages to a Bubble Tea program
// from outside the program using Program.Send(Msg).

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/mattn/go-isatty"
)

var (
	spinnerStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("63"))
	helpStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("241")).Margin(1, 0)
	dotStyle      = helpStyle.UnsetMargins()
	durationStyle = dotStyle
	appStyle      = lipgloss.NewStyle().Margin(1, 2, 0, 2)
)

type resultMsg struct {
	duration  time.Duration
	food      string
	completed bool
}

func (r resultMsg) String() string {
	if r.duration == 0 {
		return dotStyle.Render(strings.Repeat(".", 30))
	}
	return fmt.Sprintf("🍔 Ate %s %s", r.food,
		durationStyle.Render(r.duration.String()))
}

type model struct {
	spinner   spinner.Model
	results   []resultMsg
	quitting  bool
	completed bool
}

func newModel() model {
	const numLastResults = 5
	s := spinner.New()
	s.Style = spinnerStyle
	return model{
		spinner: s,
		results: make([]resultMsg, numLastResults),
	}
}

func (m model) Init() tea.Cmd {
	log.Println("Starting work...")
	return m.spinner.Tick
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		m.quitting = true
		return m, tea.Quit
	case resultMsg:
		m.results = append(m.results[1:], msg)
		log.Println(msg.String())
		m.completed = msg.completed
		if msg.completed {
			return m, tea.Quit
		}
		return m, nil
	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	default:
		return m, nil
	}
}

func (m model) View() string {
	var s string

	if m.quitting || m.completed {
		s += "That’s all for today!"
	} else {
		s += m.spinner.View() + " Eating food..."
	}

	s += "\n\n"

	for _, res := range m.results {
		s += res.String() + "\n"
	}

	if !m.quitting && !m.completed {
		s += helpStyle.Render("Press any key to exit")
	}

	if m.quitting || m.completed {
		s += "\n"
	}

	return appStyle.Render(s)
}

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

	msgCh := make(chan resultMsg)

	// Simulate activity
	go func() {
		for msg := range msgCh {
			p.Send(msg)
		}
	}()

	var wg sync.WaitGroup
	// Concurrently send messages to the channel
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pause := time.Duration(rand.Int63n(899)+100) * time.Millisecond // nolint:gosec
			time.Sleep(pause)
			msgCh <- resultMsg{food: randomFood(), duration: pause}
		}()
	}

	go func() {
		wg.Wait()
		msgCh <- resultMsg{food: randomFood(), completed: true}
		close(msgCh)
	}()

	if _, err := p.Run(); err != nil {
		fmt.Println("Error running program:", err)
		os.Exit(1)
	}

}

func randomFood() string {
	food := []string{
		"an apple", "a pear", "a gherkin", "a party gherkin",
		"a kohlrabi", "some spaghetti", "tacos", "a currywurst", "some curry",
		"a sandwich", "some peanut butter", "some cashews", "some ramen",
	}
	return food[rand.Intn(len(food))] // nolint:gosec
}
