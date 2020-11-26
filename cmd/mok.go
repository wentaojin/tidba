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
package cmd

import (
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/WentaoJin/tidba/pkg/mok"

	"github.com/pingcap/tidb/util/codec"
	"github.com/spf13/cobra"
)

// AppMok is decode or build key
type AppMok struct {
	*App       // embedded parent command storage
	KeyFormat  string
	TableID    int64
	IndexID    int64
	RowValue   string
	IndexValue string
}

func (app *App) AppMok() Cmder {
	return &AppMok{App: app}
}

func (app *AppMok) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "mok",
		Short:        "Mok used to decode or build tidb key",
		Long:         `Mok used to decode or build tidb key`,
		RunE:         app.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().StringVar(&app.KeyFormat, "format", "proto", "output format (go/hex/base64/proto)")
	cmd.Flags().Int64Var(&app.TableID, "table-id", 0, "table ID")
	cmd.Flags().Int64Var(&app.IndexID, "index-id", 0, "index ID")
	cmd.Flags().StringVar(&app.RowValue, "row-value", "", "row value")
	cmd.Flags().StringVar(&app.IndexValue, "index-value", "", "index value")
	return cmd
}

func (app *AppMok) RunE(cmd *cobra.Command, args []string) error {
	if cmd.Flags().NArg() == 1 { // Decode the given key.
		n := mok.N("key", []byte(cmd.Flags().Arg(0)))
		n.Expand().Print(app.KeyFormat)
	} else if cmd.Flags().NArg() == 0 { // Build a key with given flags.
		key := []byte{'t'}
		key = codec.EncodeInt(key, app.TableID)
		if app.TableID == 0 {
			fmt.Println("table ID shouldn't be 0")
			os.Exit(1)
		}

		switch {
		case app.IndexID == 0 && app.IndexValue == "" && app.RowValue != "":
			key = append(key, []byte("_r")...)
			rowValueInt, err := strconv.ParseInt(app.RowValue, 10, 64)
			if err != nil {
				fmt.Printf("invalid row value: %s\n", app.RowValue)
				os.Exit(1)
			}
			key = codec.EncodeInt(key, rowValueInt)
			key = codec.EncodeBytes([]byte{}, key)
			fmt.Printf("built key: %s\n", strings.ToUpper(hex.EncodeToString(key)))
		case app.IndexID != 0 && app.IndexValue == "" && app.RowValue == "":
			key = append(key, []byte("_i")...)
			key = codec.EncodeBytes([]byte{}, key)
			fmt.Printf("built key: %s\n", strings.ToUpper(hex.EncodeToString(key)))
		case app.IndexID != 0 && app.IndexValue != "" && app.RowValue == "":
			key = append(key, []byte("_i")...)
			key = codec.EncodeInt(key, app.IndexID)
			indexValueInt, err := strconv.ParseInt(app.IndexValue, 10, 64)
			if err != nil {
				fmt.Printf("invalid index value: %s, key parse int failed: %d\n", app.IndexValue, indexValueInt)
				os.Exit(1)
			}
			fmt.Println(indexValueInt)
			key = codec.EncodeInt(key, indexValueInt)
			key = codec.EncodeBytes([]byte{}, key)
			fmt.Printf("built key: %s\n", strings.ToUpper(hex.EncodeToString(key)))
		default:
			fmt.Println("usage:\nmok {flags} [key]")
			flag.PrintDefaults()
			os.Exit(1)
		}
	} else {
		fmt.Println("usage:\nmok {flags} [key]")
		flag.PrintDefaults()
		os.Exit(1)
	}
	return nil
}
