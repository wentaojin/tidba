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
package exporter

import (
	"bufio"
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/WentaoJin/tidba/zlog"
	"go.uber.org/zap"

	"github.com/WentaoJin/tidba/pkg/db"
	"github.com/WentaoJin/tidba/pkg/util"
)

type exporterByUser struct {
	file           *os.File
	fileWriter     *bufio.Writer
	clusterVersion int
	outFilePath    string
	grantsForSQL   []string
}

func (s *exporterByUser) init(engine *db.Engine) ([]string, error) {
	var allUsers []string
	err := s.initOutFile()
	if err != nil {
		return allUsers, err
	}
	allUsers, err = s.getAllUsers(engine)
	if err != nil {
		return allUsers, err
	}

	return allUsers, nil
}

func (s *exporterByUser) initOutFile() error {
	outFile, err := os.OpenFile(s.outFilePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	s.fileWriter, s.file = bufio.NewWriter(outFile), outFile
	return nil
}

func (s *exporterByUser) getAllUsers(engine *db.Engine) ([]string, error) {
	querySQL := `SELECT
	 USER
FROM
	mysql.USER 
WHERE
	USER <> 'root'`

	var users []string

	_, res, err := engine.QuerySQL(querySQL)
	if err != nil {
		return users, fmt.Errorf("failed get all username from db: %v", err)
	}
	for _, r := range res {
		users = append(users, r["USER"])
	}
	return users, nil
}

func (s *exporterByUser) generateUserCreateSQL(userNames []string, engine *db.Engine) error {
	var (
		querySQL  string
		usernames []string
		createSQL []string
	)

	for _, user := range userNames {
		usernames = append(usernames, fmt.Sprintf("'%s'", user))
	}

	username := strings.Join(usernames, ",")
	if s.clusterVersion >= 4 {
		querySQL = fmt.Sprintf(`SELECT
	CONCAT( 'CREATE USER ', USER, '@''', HOST, ''' identified by password''', authentication_string, ''';' ) AS QUERY 
FROM
	mysql.USER 
WHERE
	USER IN (%s)`, username)
	} else {
		querySQL = fmt.Sprintf(`SELECT
	CONCAT( 'CREATE USER ', USER, '@''', HOST, ''' identified by password''', PASSWORD, ''';' ) AS QUERY 
FROM
	mysql.USER 
WHERE
	USER IN (%s)`, username)
	}
	_, res, err := engine.QuerySQL(querySQL)
	if err != nil {
		return fmt.Errorf("failed get username create sql from db: %v", err)
	}
	for _, r := range res {
		createSQL = append(createSQL, r["QUERY"])
	}

	sqlBuf := bytes.NewBuffer(nil)
	sqlBuf.WriteString("-- USER EXPORT CREATE" + "\n")
	for _, s := range createSQL {
		sqlBuf.WriteString(s + "\n")
	}

	_, err = s.fileWriter.WriteString(sqlBuf.String() + "\n\n")
	if err != nil {
		return err
	}
	return nil
}

func (s *exporterByUser) getUserGrantsForSQL(userNames []string, engine *db.Engine) error {
	var (
		querySQL     string
		usernames    []string
		grantsForSQL []string
	)

	for _, user := range userNames {
		usernames = append(usernames, fmt.Sprintf("'%s'", user))
	}

	username := strings.Join(usernames, ",")

	querySQL = fmt.Sprintf(`SELECT
	CONCAT( 'SHOW GRANTS FOR ''', USER, '''@''', HOST, '''' ) AS QUERY 
FROM
	mysql.USER 
WHERE
	USER IN (%s)`, username)

	_, res, err := engine.QuerySQL(querySQL)
	if err != nil {
		return fmt.Errorf("failed get username grants for sql from db: %v", err)
	}
	for _, r := range res {
		grantsForSQL = append(grantsForSQL, r["QUERY"])
	}
	s.grantsForSQL = grantsForSQL

	return nil
}

func (s *exporterByUser) generateUserGrantsSQL(engine *db.Engine) error {
	for _, grantSQL := range s.grantsForSQL {
		var userGrant []string
		if err := queryRows(engine.DB, grantSQL, func(row, cols []string) error {
			userGrant = append(userGrant, fmt.Sprintf("%s;", row[0]))
			return nil
		}); err != nil {
			return fmt.Errorf("run SQL [%s] failed: %v", grantSQL, err)
		}

		sqlBuf := bytes.NewBuffer(nil)
		sqlBuf.WriteString(fmt.Sprintf("-- %s\n", grantSQL))
		for _, grant := range userGrant {
			sqlBuf.WriteString(grant + ";\n")
		}
		_, err := s.fileWriter.WriteString(sqlBuf.String() + "\n\n")
		if err != nil {
			return err
		}
	}

	sqlBuf := bytes.NewBuffer(nil)
	sqlBuf.WriteString("-- FLUSH PRIVILEGES\n")
	_, err := s.fileWriter.WriteString(sqlBuf.String() + "FLUSH PRIVILEGES;")
	if err != nil {
		return err
	}
	return nil
}

func (s *exporterByUser) close() {
	if s.file != nil {
		s.fileWriter.Flush()
		s.file.Close()
	}
}

func IncludeUserExporter(clusterVersion int, includeUsers []string, outDir string, engine *db.Engine) error {
	s := &exporterByUser{
		clusterVersion: clusterVersion,
		outFilePath:    path.Join(outDir, "exporter_user_grants.sql"),
	}
	startTime := time.Now()
	allUsers, err := s.init(engine)
	if err != nil {
		return err
	}
	isSubset, notExistUsers := util.IsExistInclude(allUsers, includeUsers)
	if !isSubset {
		return fmt.Errorf("user '%v' not exist", notExistUsers)
	}

	if err := s.generateUserCreateSQL(includeUsers, engine); err != nil {
		return err
	}

	if err := s.getUserGrantsForSQL(includeUsers, engine); err != nil {
		return err
	}
	if err := s.generateUserGrantsSQL(engine); err != nil {
		return err
	}
	s.close()
	endTime := time.Now()
	zlog.Logger.Info("Run task info",
		zap.String("exporter user create and grants sql total cost time", endTime.Sub(startTime).String()),
	)
	return nil
}

func FilterUserExporter(clusterVersion int, excludeTables []string, outDir string, engine *db.Engine) error {
	s := &exporterByUser{
		clusterVersion: clusterVersion,
		outFilePath:    path.Join(outDir, "exporter_user_grants.sql"),
	}
	startTime := time.Now()
	allUsers, err := s.init(engine)
	if err != nil {
		return err
	}

	includeUsers := util.FilterFromAll(allUsers, excludeTables)
	if err := s.generateUserCreateSQL(includeUsers, engine); err != nil {
		return err
	}

	if err := s.getUserGrantsForSQL(includeUsers, engine); err != nil {
		return err
	}
	if err := s.generateUserGrantsSQL(engine); err != nil {
		return err
	}
	s.close()
	endTime := time.Now()
	zlog.Logger.Info("Run task info",
		zap.String("exporter user create and grants sql total cost time", endTime.Sub(startTime).String()),
	)
	return nil
}

func RegexpUserExporter(clusterVersion int, regex string, outDir string, engine *db.Engine) error {
	s := &exporterByUser{
		clusterVersion: clusterVersion,
		outFilePath:    path.Join(outDir, "exporter_user_grants.sql"),
	}
	startTime := time.Now()
	allUsers, err := s.init(engine)
	if err != nil {
		return err
	}

	includeUsers := util.RegexpFromAll(allUsers, regex)
	if err := s.generateUserCreateSQL(includeUsers, engine); err != nil {
		return err
	}

	if err := s.getUserGrantsForSQL(includeUsers, engine); err != nil {
		return err
	}
	if err := s.generateUserGrantsSQL(engine); err != nil {
		return err
	}
	s.close()
	endTime := time.Now()
	zlog.Logger.Info("Run task info",
		zap.String("exporter user create and grants sql total cost time", endTime.Sub(startTime).String()),
	)
	return nil
}

func AllUserExporter(clusterVersion int, outDir string, engine *db.Engine) error {
	s := &exporterByUser{
		clusterVersion: clusterVersion,
		outFilePath:    path.Join(outDir, "exporter_user_grants.sql"),
	}
	startTime := time.Now()
	allUsers, err := s.init(engine)
	if err != nil {
		return err
	}

	if err := s.generateUserCreateSQL(allUsers, engine); err != nil {
		return err
	}

	if err := s.getUserGrantsForSQL(allUsers, engine); err != nil {
		return err
	}
	if err := s.generateUserGrantsSQL(engine); err != nil {
		return err
	}
	s.close()
	endTime := time.Now()
	zlog.Logger.Info("Run task info",
		zap.String("exporter user create and grants sql total cost time", endTime.Sub(startTime).String()),
	)
	return nil
}

func queryRows(Engine *sql.DB, SQL string, fn func(row, cols []string) error) (err error) {
	rows, err := Engine.Query(SQL)
	if err == nil {
		defer rows.Close()
	}

	if err != nil {
		return err
	}

	cols, err1 := rows.Columns()
	if err1 != nil {
		return err1
	}
	// Read all rows.
	var actualRows [][]string
	for rows.Next() {

		rawResult := make([][]byte, len(cols))
		result := make([]string, len(cols))
		dest := make([]interface{}, len(cols))
		for i := range rawResult {
			dest[i] = &rawResult[i]
		}

		err1 = rows.Scan(dest...)
		if err1 != nil {
			return err1
		}

		for i, raw := range rawResult {
			if raw == nil {
				result[i] = "NULL"
			} else {
				val := string(raw)
				result[i] = val
			}
		}

		actualRows = append(actualRows, result)
	}
	if err = rows.Err(); err != nil {
		return err
	}

	for _, row := range actualRows {
		err := fn(row, cols)
		if err != nil {
			return err
		}
	}
	return nil
}
