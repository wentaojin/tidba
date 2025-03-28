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
package inspect

import (
	"context"
	"fmt"

	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/database/sqlite"
	"github.com/wentaojin/tidba/model"
	"gopkg.in/yaml.v3"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textarea"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type InspectCreateModel struct {
	ctx         context.Context
	cancel      context.CancelFunc
	clusterName string
	textarea    textarea.Model
	spinner     spinner.Model
	mode        string // editing|submitting|submitted
	Msg         string // submit content
	Error       error
}

func NewInspectCreateModel(clusterName string) InspectCreateModel {
	ti := textarea.New()
	ti.ShowLineNumbers = false
	ti.CharLimit = 0                                     // no length limit
	ti.SetValue(DefaultInspectConfigTemplate().String()) // setted origin template
	ti.Focus()

	sp := spinner.New()
	sp.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("206"))

	ctx, cancel := context.WithCancel(context.Background())

	return InspectCreateModel{
		ctx:         ctx,
		cancel:      cancel,
		textarea:    ti,
		spinner:     sp,
		clusterName: clusterName,
		mode:        model.BubblesModeEditing,
	}
}

func (m InspectCreateModel) Init() tea.Cmd {
	return tea.Batch(
		textarea.Blink, // the original cursor is blinking
		m.spinner.Tick, // added spinner animation
	)
}

func (m InspectCreateModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var (
		cmds []tea.Cmd
		cmd  tea.Cmd
	)
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.textarea.SetWidth(msg.Width - 4)
		m.textarea.SetHeight(msg.Height - 5)
	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyEsc:
			if m.textarea.Focused() {
				m.textarea.Blur()
			}
		case tea.KeyCtrlC:
			m.cancel()
			return m, tea.Quit
		case tea.KeyCtrlS: // ctrl + S submit shortcut key
			m.mode = model.BubblesModeSubmitting
			return m, tea.Batch(
				m.spinner.Tick, // keep spinner animation
				submitInspCreateData(m.ctx, m.clusterName, m.textarea.Value()), // submit textarea data
			)
		default:
			if !m.textarea.Focused() {
				cmd = m.textarea.Focus()
				cmds = append(cmds, cmd)
			}
			m.textarea, cmd = m.textarea.Update(msg)
			cmds = append(cmds, cmd)
		}
	case spinner.TickMsg:
		m.spinner, cmd = m.spinner.Update(msg)
		cmds = append(cmds, cmd)
	case inspCreateResultMsg:
		if msg.err != nil {
			m.mode = model.BubblesModeEditing
			m.Error = msg.err
			// regain focus, refill error data, and re-edit
			cmd = m.textarea.Focus()
			cmds = append(cmds, cmd)
		} else {
			m.mode = model.BubblesModeSubmitted
			m.Msg = msg.data
			return m, tea.Quit
		}
	}

	return m, tea.Batch(cmds...)
}

func (m InspectCreateModel) View() string {
	switch m.mode {
	case model.BubblesModeSubmitting:
		return fmt.Sprintf(
			"%s Submitting cluster ispection config... %s",
			m.spinner.View(),
			"(ctrl+c to quit)",
		)
	case model.BubblesModeSubmitted:
		return "✅ Submitted successfully!\n\n"
	default:
		errorView := ""
		if m.Error != nil {
			errorView = fmt.Sprintf("\n❌ Submitted error: %s\n", m.Error.Error())
		}
		return fmt.Sprintf(
			"Edit your config (ctrl+s to submit)\n\n%s\n%s\n%s",
			m.textarea.View(),
			errorView,
			"(ctrl+c to quit)",
		)
	}
}

type inspCreateResultMsg struct {
	data string
	err  error
}

func submitInspCreateData(ctx context.Context, clusterName, content string) tea.Cmd {
	return func() tea.Msg {
		// validate required fields
		var data *InspectConfig
		if err := yaml.Unmarshal([]byte(content), &data); err != nil {
			return inspCreateResultMsg{data: content, err: fmt.Errorf("invalid YAML: %w", err)}
		}

		db, err := database.Connector.GetDatabase(database.DefaultSqliteClusterName)
		if err != nil {
			return inspCreateResultMsg{data: content, err: fmt.Errorf("invalid cluster [%s] database connector: %v", database.DefaultSqliteClusterName, err)}
		}

		_, err = db.(*sqlite.Database).CreateInspect(ctx, &sqlite.Inspect{
			ClusterName:   clusterName,
			InspectConfig: data.String(),
		})
		if err != nil {
			return inspCreateResultMsg{data: content, err: err}
		}
		return inspCreateResultMsg{data: content, err: nil}
	}
}
