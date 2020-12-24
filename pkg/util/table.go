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
package util

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func QueryResultFormatTableWithBaseStyle(cols []string, res []map[string]string) {
	header, data := QueryResultProcess(cols, res)
	NewTable(os.Stdout).TableWithBaseStyle(header, data)
}

func QueryResultFormatTableWithCustomStyle(cols []string, res []map[string]string) string {
	header, data := QueryResultProcess(cols, res)
	tableString := &strings.Builder{}
	NewTableString(tableString).TableWithCustomStyle(header, data)
	return tableString.String()
}

func QueryResultProcess(cols []string, res []map[string]string) (header []string, data [][]string) {
	// origin data without order
	for _, r := range res {
		var (
			colData []string
		)
		for _, col := range cols {
			colData = append(colData, r[col])
		}
		data = append(data, colData)
		// slice clear
		// https://gist.github.com/moooofly/a003f53d438adda3ed49af2ec4cca3e4
		//colData = nil
	}
	return cols, data
}

type Table struct {
	*tablewriter.Table
}

func NewTable(wt io.Writer) *Table {
	tb := tablewriter.NewWriter(wt)
	return &Table{tb}

}

func (t *Table) TableWithBaseStyle(header []string, data [][]string) {
	t.SetHeader(header)
	t.SetAutoWrapText(false)
	t.SetAutoFormatHeaders(false)
	t.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	for _, v := range data {
		t.Append(v)
	}
	t.Render() // Send output
}

func NewTableString(wt io.Writer) *Table {
	tb := tablewriter.NewWriter(wt)
	return &Table{tb}

}
func (t *Table) TableWithCustomStyle(header []string, data [][]string) {
	t.SetHeader(header)
	t.SetAutoWrapText(false)
	t.SetAutoFormatHeaders(false)
	t.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	t.SetCaption(true, fmt.Sprintf("%d rows in set (0.66 sec)", len(data)))
	t.AppendBulk(data) // Add Bulk Data
	t.Render()         // Send output
}
