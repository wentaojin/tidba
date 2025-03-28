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

type InspectUpdateModel struct {
	ctx         context.Context
	cancel      context.CancelFunc
	clusterName string
	textarea    textarea.Model
	spinner     spinner.Model
	mode        string         // editing|submitting|submitted
	Msg         *InspectConfig // textarea content
	Error       error
}

func NewInspectUpdateModel(clusterName string) InspectUpdateModel {
	ti := textarea.New()
	ti.ShowLineNumbers = false
	ti.CharLimit = 0
	ti.Focus()

	sp := spinner.New()
	sp.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("206"))

	ctx, cancel := context.WithCancel(context.Background())

	return InspectUpdateModel{
		ctx:         ctx,
		cancel:      cancel,
		clusterName: clusterName,
		textarea:    ti,
		spinner:     sp,
		mode:        model.BubblesModeQuering,
	}
}

func (m InspectUpdateModel) Init() tea.Cmd {
	return tea.Batch(
		textarea.Blink,
		m.spinner.Tick, // added spinner animation
	)
}

func (m InspectUpdateModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var (
		cmds []tea.Cmd
		cmd  tea.Cmd
	)
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.textarea.SetWidth(msg.Width - 4)
		m.textarea.SetHeight(msg.Height - 5)
		return m, nil
	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyEsc:
			if m.textarea.Focused() {
				m.textarea.Blur()
			}
			return m, nil
		case tea.KeyCtrlC:
			m.cancel()
			return m, tea.Quit
		case tea.KeyCtrlS: // ctrl + S submit shortcut key
			m.mode = model.BubblesModeSubmitting
			return m, tea.Batch(
				submitEditInspData(m.ctx, m.clusterName, m.textarea.Value()), // submit textarea data
				m.spinner.Tick, // keep spinner animation
			)
		default:
			if !m.textarea.Focused() {
				cmd = m.textarea.Focus()
				cmds = append(cmds, cmd)
			}
			m.textarea, cmd = m.textarea.Update(msg)
			cmds = append(cmds, cmd)
			return m, tea.Batch(cmds...)
		}
	case spinner.TickMsg:
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	case editInspResultMsg:
		if msg.err != nil {
			m.mode = model.BubblesModeEditing
			m.Error = msg.err
			// regain focus, refill error data, and re-edit
			cmd = m.textarea.Focus()
			return m, cmd
		} else {
			m.mode = model.BubblesModeSubmitted
			m.Msg = msg.data
			return m, tea.Quit
		}
	case queryInspResultMsg:
		m.mode = model.BubblesModeQueried
		if msg.err != nil {
			m.Error = msg.err
			return m, tea.Quit
		} else {
			m.textarea.SetValue(msg.data.String()) // setted origin template
			return m, nil
		}
	default:
		if m.mode == model.BubblesModeQuering {
			return m, tea.Batch(
				queryInspResultData(m.ctx, m.clusterName), // query cluster data
				m.spinner.Tick, // keep spinner animation
			)
		}
		return m, nil
	}
}

func (m InspectUpdateModel) View() string {
	switch m.mode {
	case model.BubblesModeQuering:
		return fmt.Sprintf(
			"%s Quering cluster inspect config...%s",
			m.spinner.View(),
			"(ctrl+c to quit)",
		)
	case model.BubblesModeSubmitting:
		return fmt.Sprintf(
			"%s Submitting cluster inspect config...%s",
			m.spinner.View(),
			"(ctrl+c to quit)",
		)
	case model.BubblesModeSubmitted:
		return "✅ Submitted successfully!\n\n"
	case model.BubblesModeQueried:
		if m.Error != nil {
			return fmt.Sprintf("\n❌ Queried error: %s\n", m.Error.Error())
		} else {
			return fmt.Sprintf(
				"Edit your config (ctrl+s to submit)\n\n%s\n%s",
				m.textarea.View(),
				"(ctrl+c to quit)",
			)
		}
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

type queryInspResultMsg struct {
	data *InspectConfig
	err  error
}

func queryInspResultData(ctx context.Context, clusterName string) tea.Cmd {
	return func() tea.Msg {
		var (
			data *InspectConfig
			err  error
		)
		db, err := database.Connector.GetDatabase(database.DefaultSqliteClusterName)
		if err != nil {
			return queryInspResultMsg{data: data, err: fmt.Errorf("invalid cluster [%s] database connector: %v", database.DefaultSqliteClusterName, err)}
		}

		c, err := db.(*sqlite.Database).GetInspect(ctx, clusterName)
		if err != nil {
			return queryInspResultMsg{data: nil, err: err}
		}
		if err := yaml.Unmarshal([]byte(c.InspectConfig), &data); err != nil {
			return queryInspResultMsg{data: nil, err: err}
		}

		return queryInspResultMsg{data: data, err: nil}
	}
}

type editInspResultMsg struct {
	data *InspectConfig
	err  error
}

func submitEditInspData(ctx context.Context, clusterName string, content string) tea.Cmd {
	return func() tea.Msg {
		// validate required fields
		var data *InspectConfig
		if err := yaml.Unmarshal([]byte(content), &data); err != nil {
			return editInspResultMsg{data: data, err: fmt.Errorf("invalid YAML: %w", err)}
		}

		db, err := database.Connector.GetDatabase(database.DefaultSqliteClusterName)
		if err != nil {
			return editInspResultMsg{data: data, err: fmt.Errorf("invalid cluster [%s] database connector: %v", database.DefaultSqliteClusterName, err)}
		}

		_, err = db.(*sqlite.Database).CreateInspect(ctx, &sqlite.Inspect{
			ClusterName:   clusterName,
			InspectConfig: data.String(),
		})
		if err != nil {
			return editInspResultMsg{data: data, err: err}
		}
		return editInspResultMsg{data: data, err: nil}
	}
}
