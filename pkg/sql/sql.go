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
package sql

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/WentaoJin/tidba/pkg/util"

	"github.com/WentaoJin/tidba/pkg/db"
)

type sqlRun struct {
	sqlFile      string
	sqlSeparator string
	writerFile   *os.File
	fileWriter   *bufio.Writer
	outFilePath  string
}

func RunSQL(engine *db.Engine, sqlFile, sqlSeparator, outDir string) error {
	// 1. get distinct value.
	s := &sqlRun{
		sqlFile:      sqlFile,
		sqlSeparator: sqlSeparator,
		outFilePath:  path.Join(outDir, "sql_by_tidba.sql"),
	}
	if err := s.initOutFile(); err != nil {
		return err
	}

	splitSQL, err := s.readSQLFile()
	if err != nil {
		return err
	}

	var sqlSlice []string
	// 处理连续空白字符，返回单独 SQL 数组
	for _, sql := range splitSQL {
		sqlSlice = append(sqlSlice, strings.Join(strings.Fields(sql), " "))
	}

	for _, sql := range sqlSlice {
		sqlBuf := bytes.NewBuffer(nil)
		sqlBuf.WriteString(sql)
		if _, err := s.fileWriter.WriteString(sqlBuf.String() + "\n"); err != nil {
			return err
		}
		cols, res, err := engine.QuerySQL(sql)
		if err != nil {
			return err
		}
		if len(res) != 0 {
			sqlBuf := bytes.NewBuffer(nil)
			sqlBuf.WriteString(util.QueryResultFormatTableWithCustomStyle(cols, res))
			if _, err := s.fileWriter.WriteString(sqlBuf.String() + "\n\n"); err != nil {
				return err
			}
		}
	}
	s.close()
	return nil
}

func (s *sqlRun) readSQLFile() ([]string, error) {
	var result []string

	sqlBytes, err := ioutil.ReadFile(s.sqlFile)
	if err != nil {
		return result, fmt.Errorf("error when reading file: %s", err)
	}

	result = strings.Split(string(sqlBytes), s.sqlSeparator)

	return result, nil

}

func (s *sqlRun) initOutFile() error {
	outFile, err := os.OpenFile(s.outFilePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	s.fileWriter, s.writerFile = bufio.NewWriter(outFile), outFile
	return nil
}

func (s *sqlRun) close() {
	if s.writerFile != nil {
		s.fileWriter.Flush()
		s.writerFile.Close()
	}
}
