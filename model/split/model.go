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
package split

import (
	"context"
	"fmt"
	"log"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/wentaojin/tidba/model"
)

type TableSplitModel struct {
	ctx     context.Context
	cancel  context.CancelFunc
	output  string
	spinner spinner.Model
	Err     error
	mode    string
}

func NewTableSplitModel(output string) TableSplitModel {
	sp := spinner.New()
	sp.Spinner = spinner.Line
	sp.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("206"))

	ctx, cancel := context.WithCancel(context.Background())

	return TableSplitModel{
		ctx:     ctx,
		cancel:  cancel,
		spinner: sp,
		mode:    model.BubblesModeSpliting,
		output:  output,
	}
}

func (m TableSplitModel) Init() tea.Cmd {
	log.Println("Starting work...")
	return m.spinner.Tick
}

func (m TableSplitModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var (
		cmd tea.Cmd
	)
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyCtrlC:
			m.cancel()
			return m, tea.Quit
		default:
			return m, nil
		}
	case SplitRespMsg:
		m.mode = model.BubblesModeSplited
		if msg.Err != nil {
			log.Printf("Split table error: %v", msg.Err)
			m.Err = fmt.Errorf("split table error: %v", msg.Err)
		}
		return m, tea.Quit
	case spinner.TickMsg:
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	default:
		return m, m.spinner.Tick
	}
}

func (m TableSplitModel) View() string {
	switch m.mode {
	case model.BubblesModeSpliting:
		return fmt.Sprintf(
			"%s Spliting cluster table region... %s",
			m.spinner.View(),
			"(ctrl+c to quit)",
		)
	default:
		if m.Err != nil {
			return "❌ Splited failed!\n\n"
		}
		return fmt.Sprintf("✅ Splited successfully!\n\n🏆 Split file output dir: [%s]\n\n", m.output)
	}
}
