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
	"context"
	"fmt"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/database/sqlite"
)

type ClusterLoginModel struct {
	ctx         context.Context
	cancel      context.CancelFunc
	clusterName string
	spinner     spinner.Model
	mode        string
	Msgs        []*sqlite.Cluster
	Error       error
}

func NewClusterLoginModel(clusterName string) ClusterLoginModel {
	sp := spinner.New()
	sp.Spinner = spinner.Line
	sp.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("206"))

	ctx, cancel := context.WithCancel(context.Background())

	return ClusterLoginModel{
		ctx:         ctx,
		cancel:      cancel,
		spinner:     sp,
		clusterName: clusterName,
		mode:        BubblesModeLogining,
	}
}

func (m ClusterLoginModel) Init() tea.Cmd {
	return m.spinner.Tick
}

func (m ClusterLoginModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
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
	case spinner.TickMsg:
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	case loginResultMsg:
		m.mode = BubblesModeLogined
		if msg.err != nil {
			m.Error = msg.err
		} else {
			m.Msgs = msg.datas
		}
		return m, tea.Quit
	default:
		return m, tea.Batch(
			m.spinner.Tick,
			submitLoginData(m.ctx, m.clusterName), // submit login data
		)
	}
}

func (m ClusterLoginModel) View() string {
	switch m.mode {
	case BubblesModeLogining:
		return fmt.Sprintf(
			"%s Logining cluster...%s",
			m.spinner.View(),
			"(ctrl+c to quit)",
		)
	default:
		if m.Error != nil {
			return fmt.Sprintf("\n❌ Logined error: %s\n", m.Error.Error())
		}

		return "✅ Logined successfully!\n\n"
	}
}

type loginResultMsg struct {
	datas []*sqlite.Cluster
	err   error
}

func submitLoginData(ctx context.Context, clusterName string) tea.Cmd {
	return func() tea.Msg {
		datas, err := database.LoginClusterDatabase(ctx, clusterName)
		if err != nil {
			return loginResultMsg{datas: datas, err: err}
		}
		return loginResultMsg{datas: datas, err: nil}
	}
}
