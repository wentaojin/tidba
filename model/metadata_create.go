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
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/database/sqlite"
	"github.com/wentaojin/tidba/utils/cluster/operator"

	"github.com/go-playground/validator/v10"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textarea"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type Cluster struct {
	ClusterName string `json:"clusterName" validate:"required"`
	DbUser      string `json:"dbUser" validate:"required"`
	DbPassword  string `json:"dbPassword"`
	// If the host and port are not filled in, the meta.yaml will be automatically obtained according to the location of the metadata file and the host and port of the tidb-server will be randomly obtained.
	// If you are using load balancing software or directly connecting to the tidb-server, it is recommended to fill in the corresponding host and port
	DbHost     string `json:"dbHost"`
	DbPort     uint64 `json:"dbPort"`
	DbCharset  string `json:"dbCharset" validate:"required"`
	ConnParams string `json:"connParams"`
	Comment    string `json:"comment"`
}

func (c *Cluster) String() string {
	c.DbCharset = "utf8mb4"
	val, _ := json.MarshalIndent(c, "", " ")
	return string(val)
}

type clusterCreateModel struct {
	ctx         context.Context
	cancel      context.CancelFunc
	textarea    textarea.Model
	spinner     spinner.Model
	mode        string // editing|submitting|submitted
	submitMsg   string // submit content
	submitError string
}

func NewClusterCreateModel() clusterCreateModel {
	c := &Cluster{}
	ti := textarea.New()
	ti.SetHeight(30)
	ti.ShowLineNumbers = false
	ti.SetValue(c.String()) // setted origin template
	ti.Focus()

	sp := spinner.New()
	sp.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("206"))

	ctx, cancel := context.WithCancel(context.Background())

	return clusterCreateModel{
		ctx:      ctx,
		cancel:   cancel,
		textarea: ti,
		spinner:  sp,
		mode:     BubblesModeEditing,
	}
}

func (m clusterCreateModel) Init() tea.Cmd {
	return tea.Batch(
		textarea.Blink, // the original cursor is blinking
		m.spinner.Tick, // added spinner animation
	)
}

func (m clusterCreateModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var (
		cmds []tea.Cmd
		cmd  tea.Cmd
	)
	switch msg := msg.(type) {
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
			m.mode = BubblesModeSubmitting
			return m, tea.Batch(
				submitCreateData(m.ctx, m.textarea.Value()), // submit textarea data
				m.spinner.Tick, // keep spinner animation
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
	case createResultMsg:
		if msg.err != nil {
			m.mode = BubblesModeEditing
			m.submitError = msg.err.Error()
			// regain focus, refill error data, and re-edit
			cmd = m.textarea.Focus()
			cmds = append(cmds, cmd)
		} else {
			m.mode = BubblesModeSubmitted
			m.submitMsg = msg.data
			return m, tea.Quit
		}
	}

	return m, tea.Batch(cmds...)
}

func (m clusterCreateModel) View() string {
	switch m.mode {
	case BubblesModeSubmitting:
		return fmt.Sprintf(
			"%s Submitting cluster config...%s",
			m.spinner.View(),
			"(ctrl+c to quit)",
		)
	case BubblesModeSubmitted:
		return fmt.Sprintf(
			"✅ Submitted successfully!\n\ncluster config content:\n%s\n\n",
			m.submitMsg,
		)
	default:
		errorView := ""
		if m.submitError != "" {
			errorView = fmt.Sprintf("\n❌ Submitted error: %s\n", m.submitError)
		}
		return fmt.Sprintf(
			"Edit your config (ctrl+s to submit)\n\n%s\n%s\n%s",
			m.textarea.View(),
			errorView,
			"(ctrl+c to quit)",
		)
	}
}

type createResultMsg struct {
	data string
	err  error
}

func submitCreateData(ctx context.Context, content string) tea.Cmd {
	return func() tea.Msg {
		// validate local tiup whether existed
		if _, err := operator.IsExistedTiUPComponent(); err != nil {
			return createResultMsg{data: content, err: err}
		}
		// validate required fields
		var data *Cluster
		if err := json.Unmarshal([]byte(content), &data); err != nil {
			return createResultMsg{data: content, err: fmt.Errorf("invalid JSON: %w", err)}
		}

		validate := validator.New()
		if err := validate.Struct(data); err != nil {
			return createResultMsg{data: content, err: fmt.Errorf("validation failed: %w", err)}
		}

		db, err := database.Connector.GetDatabase(database.DefaultSqliteClusterName)
		if err != nil {
			return createResultMsg{data: content, err: fmt.Errorf("invalid cluster [%s] database connector: %v", database.DefaultSqliteClusterName, err)}
		}
		c, err := db.(*sqlite.Database).GetCluster(ctx, data.ClusterName)
		if err != nil {
			return createResultMsg{data: content, err: err}
		}

		if reflect.DeepEqual(c, &sqlite.Cluster{}) {
			// validate cluster existed
			// automatically fill in path and privateKey path
			metaC, err := operator.GetDeployedClusterList(data.ClusterName)
			if err != nil {
				return createResultMsg{data: content, err: err}
			}
			// automatically determine whether the host and port are empty.
			// If they are empty, get the first tidb server address from the topology.
			topo, err := operator.GetDeployedClusterTopology(data.ClusterName)
			if err != nil {
				return createResultMsg{data: content, err: err}
			}
			insts, err := topo.GetClusterTopologyComponentInstances(operator.ComponentNameTiDB)
			if err != nil {
				return createResultMsg{data: content, err: err}
			}

			var (
				host string
				port uint64
			)
			host = data.DbHost
			port = data.DbPort
			if host == "" {
				host = insts[0].Host
			}
			if port == 0 {
				port = insts[0].Port
			}

			newData, err := db.(*sqlite.Database).CreateCluster(ctx, &sqlite.Cluster{
				ClusterName: data.ClusterName,
				DbUser:      data.DbUser,
				DbPassword:  data.DbPassword,
				DbHost:      host,
				DbPort:      port,
				DbCharset:   data.DbCharset,
				ConnParams:  data.ConnParams,
				Path:        metaC.Path,
				PrivateKey:  metaC.PrivateKey,
				Entity: &sqlite.Entity{
					Comment: data.Comment,
				},
			})
			if err != nil {
				return createResultMsg{data: content, err: err}
			}
			return createResultMsg{data: newData.String(), err: nil}
		}
		return createResultMsg{data: content, err: fmt.Errorf("the cluster name [%s] is repeated", c.ClusterName)}
	}
}
