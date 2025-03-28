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
	"path/filepath"
	"reflect"

	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/database/sqlite"

	"github.com/go-playground/validator/v10"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textarea"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type NewCluster struct {
	ClusterName string `json:"clusterName" validate:"required"`
	DbUser      string `json:"dbUser" validate:"required"`
	DbPassword  string `json:"dbPassword"`
	// If the host and port are not filled in, the meta.yaml will be automatically obtained according to the location of the metadata file and the host and port of the tidb-server will be randomly obtained.
	// If you are using load balancing software or directly connecting to the tidb-server, it is recommended to fill in the corresponding host and port
	DbHost     string `json:"dbHost"`
	DbPort     uint64 `json:"dbPort"`
	DbCharset  string `json:"dbCharset" validate:"required"`
	ConnParams string `json:"connParams"`
	Path       string `json:"path" validate:"required"`
	PrivateKey string `json:"privateKey" validate:"required"`
	Comment    string `json:"comment"`
}

type ModifyCluster struct {
	DbUser     string `json:"dbUser" validate:"required"`
	DbPassword string `json:"dbPassword"`
	// If the host and port are not filled in, the meta.yaml will be automatically obtained according to the location of the metadata file and the host and port of the tidb-server will be randomly obtained.
	// If you are using load balancing software or directly connecting to the tidb-server, it is recommended to fill in the corresponding host and port
	DbHost     string `json:"dbHost"`
	DbPort     uint64 `json:"dbPort"`
	DbCharset  string `json:"dbCharset" validate:"required"`
	ConnParams string `json:"connParams"`
	Path       string `json:"path" validate:"required"`
	PrivateKey string `json:"privateKey" validate:"required"`
	Comment    string `json:"comment"`
}

func (c *ModifyCluster) String() string {
	val, _ := json.MarshalIndent(c, "", " ")
	return string(val)
}

type clusterUpdateModel struct {
	ctx         context.Context
	cancel      context.CancelFunc
	clusterName string
	textarea    textarea.Model
	spinner     spinner.Model
	mode        string // editing|submitting|submitted
	msg         string // textarea content
	err         string
}

func NewClusterUpdateModel(clusterName string) clusterUpdateModel {
	ti := textarea.New()
	ti.ShowLineNumbers = false
	ti.CharLimit = 0 // no length limit
	ti.Focus()

	sp := spinner.New()
	sp.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("206"))

	ctx, cancel := context.WithCancel(context.Background())

	return clusterUpdateModel{
		ctx:         ctx,
		cancel:      cancel,
		clusterName: clusterName,
		textarea:    ti,
		spinner:     sp,
		mode:        BubblesModeQuering,
	}
}

func (m clusterUpdateModel) Init() tea.Cmd {
	return tea.Batch(
		textarea.Blink,
		m.spinner.Tick, // added spinner animation
	)
}

func (m clusterUpdateModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var (
		cmds []tea.Cmd
		cmd  tea.Cmd
	)
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.textarea.SetWidth(msg.Width - 4)
		m.textarea.SetHeight(msg.Height - 22)
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
			m.mode = BubblesModeSubmitting
			return m, tea.Batch(
				submitEditData(m.ctx, m.clusterName, m.textarea.Value()), // submit textarea data
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
	case editResultMsg:
		if msg.err != nil {
			m.mode = BubblesModeEditing
			m.err = msg.err.Error()
			// regain focus, refill error data, and re-edit
			cmd = m.textarea.Focus()
			return m, cmd
		} else {
			m.mode = BubblesModeSubmitted
			m.msg = msg.data
			return m, tea.Quit
		}
	case queryResultMsg:
		m.mode = BubblesModeQueried

		if msg.err != nil {
			m.err = msg.err.Error()
			return m, tea.Quit
		} else {
			m.mode = BubblesModeEditing
			c := &ModifyCluster{
				DbUser:     msg.datas[0].DbUser,
				DbPassword: msg.datas[0].DbPassword,
				DbHost:     msg.datas[0].DbHost,
				DbPort:     msg.datas[0].DbPort,
				DbCharset:  msg.datas[0].DbCharset,
				ConnParams: msg.datas[0].ConnParams,
				Path:       msg.datas[0].Path,
				PrivateKey: msg.datas[0].PrivateKey,
				Comment:    msg.datas[0].Comment,
			}
			m.textarea.SetValue(c.String()) // setted origin template
			return m, nil
		}
	default:
		if m.mode == BubblesModeQuering {
			return m, tea.Batch(
				queryResultData(m.ctx, m.clusterName), // query cluster data
				m.spinner.Tick,                        // keep spinner animation
			)
		}
		return m, nil
	}
}

func (m clusterUpdateModel) View() string {
	switch m.mode {
	case BubblesModeQuering:
		return fmt.Sprintf(
			"%s Quering cluster config...%s",
			m.spinner.View(),
			"(ctrl+c to quit)",
		)
	case BubblesModeSubmitting:
		return fmt.Sprintf(
			"%s Submitting cluster config...%s",
			m.spinner.View(),
			"(ctrl+c to quit)",
		)
	case BubblesModeSubmitted:
		return fmt.Sprintf(
			"✅ Submitted successfully!\n\ncluster config content:\n%s\n\n",
			m.msg,
		)
	case BubblesModeQueried:
		if m.err != "" {
			return fmt.Sprintf("\n❌ Queried error: %s\n", m.err)
		} else {
			return fmt.Sprintf(
				"Edit your config (ctrl+s to submit)\n\n%s\n%s",
				m.textarea.View(),
				"(ctrl+c to quit)",
			)
		}
	default:
		errorView := ""
		if m.err != "" {
			errorView = fmt.Sprintf("\n❌ Submitted error: %s\n", m.err)
		}
		return fmt.Sprintf(
			"Edit your config (ctrl+s to submit)\n\n%s\n%s\n%s",
			m.textarea.View(),
			errorView,
			"(ctrl+c to quit)",
		)
	}
}

type queryResultMsg struct {
	datas []*sqlite.Cluster
	err   error
}

func queryResultData(ctx context.Context, clusterName string) tea.Cmd {
	return func() tea.Msg {
		var (
			datas []*sqlite.Cluster
			err   error
		)

		db, err := database.Connector.GetDatabase(database.DefaultSqliteClusterName)
		if err != nil {
			return queryResultMsg{datas: nil, err: fmt.Errorf("invalid cluster [%s] database connector: %v", database.DefaultSqliteClusterName, err)}
		}

		c, err := db.(*sqlite.Database).GetCluster(ctx, clusterName)
		if err != nil {
			return queryResultMsg{datas: nil, err: err}
		}
		if !reflect.DeepEqual(c, &sqlite.Cluster{}) {
			datas = append(datas, c)
		} else {
			return queryResultMsg{datas: datas, err: fmt.Errorf("the cluster_name [%s] not found, please use command [meta list] to check whether the cluster [%s] exists. if not, you can create it by using command [meta create] create", clusterName, clusterName)}
		}
		return queryResultMsg{datas: datas, err: nil}
	}
}

type editResultMsg struct {
	data string
	err  error
}

func submitEditData(ctx context.Context, clusterName string, content string) tea.Cmd {
	return func() tea.Msg {
		// validate required fields
		var data *NewCluster
		if err := json.Unmarshal([]byte(content), &data); err != nil {
			return editResultMsg{data: content, err: fmt.Errorf("invalid JSON: %w", err)}
		}

		data.ClusterName = clusterName

		validate := validator.New()
		if err := validate.Struct(data); err != nil {
			return editResultMsg{data: content, err: fmt.Errorf("validation failed: %w", err)}
		}

		if data.ClusterName != filepath.Base(filepath.Clean(data.Path)) {
			return editResultMsg{data: content, err: fmt.Errorf("make sure that the cluster name is consistent with the cluster name directory name of the .tiup metadata directory")}
		}

		db, err := database.Connector.GetDatabase(database.DefaultSqliteClusterName)
		if err != nil {
			return editResultMsg{data: content, err: fmt.Errorf("invalid cluster [%s] database connector: %v", database.DefaultSqliteClusterName, err)}
		}
		newData, err := db.(*sqlite.Database).CreateCluster(ctx, &sqlite.Cluster{
			ClusterName: data.ClusterName,
			DbUser:      data.DbUser,
			DbPassword:  data.DbPassword,
			DbHost:      data.DbHost,
			DbPort:      data.DbPort,
			DbCharset:   data.DbCharset,
			ConnParams:  data.ConnParams,
			Path:        data.Path,
			PrivateKey:  data.PrivateKey,
			Entity: &sqlite.Entity{
				Comment: data.Comment,
			},
		})
		if err != nil {
			return editResultMsg{data: content, err: err}
		}
		return editResultMsg{data: newData.String(), err: nil}
	}
}
