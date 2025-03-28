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
package mysql

import (
	"context"
	"fmt"
)

func (d *Database) GetDatabases(ctx context.Context) ([]string, error) {
	_, res, err := d.GeneralQuery(ctx, `SELECT
	schema_name AS SCHEMA_NAME
	FROM 
		INFORMATION_SCHEMA.SCHEMATA`)
	if err != nil {
		return nil, err
	}
	var schemas []string
	for _, s := range res {
		schemas = append(schemas, s["SCHEMA_NAME"])
	}
	return schemas, nil
}

func (d *Database) GetDatabaseTables(ctx context.Context, schemaName string) ([]string, error) {
	_, res, err := d.GeneralQuery(ctx, fmt.Sprintf(`SELECT
	table_name AS TABLE_NAME
FROM
	information_schema.TABLES 
WHERE
	table_schema = '%s'`, schemaName))
	if err != nil {
		return nil, err
	}
	var tables []string
	for _, s := range res {
		tables = append(tables, s["TABLE_NAME"])
	}
	return tables, nil
}
