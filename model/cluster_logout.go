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
package model

import (
	"fmt"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/wentaojin/tidba/database"
)

type clusterLogoutModel struct {
	clusterName string
	spinner     spinner.Model
	mode        string
	err         string
}

func NewClusterLogoutModel(clusterName string) clusterLogoutModel {
	sp := spinner.New()
	sp.Spinner = spinner.Line
	sp.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("206"))

	return clusterLogoutModel{
		spinner:     sp,
		clusterName: clusterName,
		mode:        BubblesModeLogouting,
	}
}

func (m clusterLogoutModel) Init() tea.Cmd {
	return m.spinner.Tick
}

func (m clusterLogoutModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var (
		cmd tea.Cmd
	)
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyCtrlC:
			return m, tea.Quit
		default:
			return m, nil
		}
	case logoutResultMsg:
		m.mode = BubblesModeLogouted
		if msg.err != nil {
			m.err = msg.err.Error()
		}
		return m, tea.Quit
	default:
		m.spinner, cmd = m.spinner.Update(msg)
		return m, tea.Batch(
			cmd,
			submitLogoutData(m.clusterName), // submit logout data
		)
	}
}

func (m clusterLogoutModel) View() string {
	switch m.mode {
	case BubblesModeLogouting:
		return fmt.Sprintf(
			"%s Logouting cluster...%s",
			m.spinner.View(),
			"(ctrl+c to quit)",
		)
	default:
		if m.err != "" {
			return fmt.Sprintf("\n❌ Logouted error: %s\n", m.err)
		}
		return "✅ Logout successfully!\n"
	}
}

type logoutResultMsg struct {
	err error
}

func submitLogoutData(clusterName string) tea.Cmd {
	return func() tea.Msg {
		if err := database.Connector.CloseDatabase(clusterName); err != nil {
			return logoutResultMsg{err: err}
		}
		return logoutResultMsg{err: nil}
	}
}
