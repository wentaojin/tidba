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
package diff

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/logrusorgru/aurora"
	"github.com/mattn/go-colorable"

	"github.com/WentaoJin/tidba/pkg/db"
)

const (
	sqlQueryVariables = `SELECT
	* 
FROM
	(
SELECT
	VARIABLE_NAME,
	VARIABLE_VALUE 
FROM
	information_schema.SESSION_VARIABLES 
WHERE
	VARIABLE_NAME LIKE '%tidb%' UNION ALL
SELECT
	VARIABLE_NAME,
	VARIABLE_VALUE 
FROM
	information_schema.GLOBAL_VARIABLES 
WHERE
	VARIABLE_NAME LIKE '%tidb%' UNION ALL
SELECT
	VARIABLE_NAME,
	VARIABLE_VALUE 
FROM
	mysql.GLOBAL_VARIABLES 
WHERE
	VARIABLE_NAME LIKE '%tidb%' 
	) T 
WHERE
	T.VARIABLE_NAME != 'tidb_config'`

	sqlQueryConfig = `SELECT
	VARIABLE_NAME,
	VARIABLE_VALUE 
FROM
	information_schema.SESSION_VARIABLES  
WHERE
	VARIABLE_NAME = 'tidb_config'`
	n
	tidbVariable = "variable"
	tidbConfig   = "config"
)

func ComponentTiDBDiff(baseAddr, baseUser, basePassword, newAddr, newUser, newPassword string, diffType, outFile string) error {
	if err := getClusterJsonDiff(baseAddr, baseUser, basePassword, newAddr, newUser, newPassword, diffType, outFile); err != nil {
		return nil
	}
	return nil
}

func getClusterJsonDiff(baseAddr, baseUser, basePassword, newAddr, newUser, newPassword string, diffType, outFile string) error {
	baseJsonByte, err := getClusterJson(baseAddr, baseUser, basePassword, diffType)
	if err != nil {
		return err
	}

	newJsonByte, err := getClusterJson(newAddr, newUser, newPassword, diffType)
	if err != nil {
		return err
	}

	var (
		opts   []Option
		w      io.Writer
		stdout io.Writer
		au     aurora.Aurora
	)
	if outFile == "-" {
		// init stout color
		// open aurora color ANSI output
		stdout = colorable.NewColorable(os.Stdout)
		au = aurora.NewAurora(true)
		w = stdout
	} else {
		// close aurora color ANSI output
		au = aurora.NewAurora(false)
		file, err := os.Create(outFile)
		if err != nil {
			return err
		}
		defer file.Close()
		w = file
	}

	jsonFmtFn := NewJSONFormatFunc(true)
	hunks, err := Diff(baseJsonByte, newJsonByte, opts...)
	if err != nil {
		return fmt.Errorf("Error: diff failed: %s\n", err)
	}

	if len(hunks) == 0 {
		// Indicates the same json value on both sides
		// return nil
		return Equivalent
	}

	if _, err := fmt.Fprintf(w, "--- %v\n", au.Green(baseAddr)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "+++ %v\n", au.Red(newAddr)); err != nil {
		return err
	}
	for i, hunk := range hunks {
		if i > 0 {
			if _, err := fmt.Fprintln(w); err != nil {
				return err
			}
		}
		if err := WriteHunk(w, au, hunk, jsonFmtFn); err != nil {
			return err
		}
	}
	return nil
}

func getClusterJson(addr, user, password string, diffType string) ([]byte, error) {
	addrs := strings.Split(addr, ":")
	baseEngine, err := db.NewMysqlDSN(user, password, addrs[0], addrs[1], "")
	if err != nil {
		return nil, err
	}

	if diffType == tidbVariable {
		baseData, err := getQueryVariablesResultBySQL(baseEngine)
		if err != nil {
			return nil, err
		}
		bd, err := json.Marshal(baseData)
		if err != nil {
			return bd, fmt.Errorf("newAddr [%s] json.Marshal failed: %v", addr, err)
		}
		return bd, nil

	} else {
		baseData, err := getQueryConfigResultBySQL(baseEngine)
		if err != nil {
			return nil, err

		}
		bd, err := json.Marshal(baseData)
		if err != nil {
			return bd, fmt.Errorf("newAddr [%s] json.Marshal failed: %v", addr, err)
		}
		return bd, nil
	}
}

func getQueryVariablesResultBySQL(engine *db.Engine) (map[string]interface{}, error) {
	var data map[string]interface{}
	_, res, err := engine.QuerySQL(sqlQueryVariables)
	if err != nil {
		return data, err
	}

	data = make(map[string]interface{}, len(res))
	for _, r := range res {
		data[r["VARIABLE_NAME"]] = r["VARIABLE_VALUE"]
	}
	return data, nil
}

func getQueryConfigResultBySQL(engine *db.Engine) (map[string]interface{}, error) {
	var (
		data string
		d    map[string]interface{}
	)
	_, res, err := engine.QuerySQL(sqlQueryConfig)
	if err != nil {
		return d, err
	}

	for _, r := range res {
		data = r["VARIABLE_VALUE"]
	}

	if err = json.Unmarshal([]byte(data), &d); err != nil {
		return d, err
	}
	return d, nil
}
