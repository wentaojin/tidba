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
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/mitchellh/colorstring"
)

func JSONDiff(baseJsonByte, newJsonByte []byte, baseAddr, newAddr, format string, coloring, quiet bool) error {
	differ := NewDiff()
	diff, err := differ.Compare(baseJsonByte, newJsonByte)
	if err != nil {
		return fmt.Errorf("failed to compare json file: %s\n", err.Error())
	}

	// Output the result
	if diff.Modified() || !quiet {
		var diffString string
		if format == "ascii" {
			var aJson map[string]interface{}
			if err := json.Unmarshal(baseJsonByte, &aJson); err != nil {
				return fmt.Errorf("failed to unmarshal file: %s\n", err.Error())
			}

			// header color print
			if coloring {
				colorstring.Printf("[red]--- %v\n", baseAddr)
				colorstring.Printf("[green]+++ %v\n", newAddr)
			} else {
				colorstring.Printf("--- %v\n", baseAddr)
				colorstring.Printf("+++ %v\n", newAddr)
			}

			// json content output
			config := AsciiFormatterConfig{
				ShowArrayIndex: true,
				Coloring:       coloring,
			}

			f := NewAsciiFormatter(aJson, config)
			diffString, err = f.Format(diff)
			if err != nil {
				return fmt.Errorf("failed to format diff result with ascii mode: %s\n", err.Error())
			}
		} else if format == "delta" {
			f := NewDeltaFormatter()
			diffString, err = f.Format(diff)
			if err != nil {
				return fmt.Errorf("failed to format diff result with delta mode: %s\n", err.Error())
			}
		} else {
			return fmt.Errorf("unknown foramt %s\n: not support\n", format)
		}
		fmt.Print(diffString)
	}
	return nil
}

func ReadJSONFile(name string) ([]byte, string, error) {
	var (
		data [][]byte
		dt   []byte
	)
	fileName := filepath.Base(name)
	file, err := os.Open(name)
	if err != nil {
		return dt, fileName, fmt.Errorf("os open file [%s] failed: %v", name, err)
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		return dt, fileName, fmt.Errorf("ioutil read file [%s] failed: %v", name, err)
	}

	// remove escape character \
	for _, v := range bytes.SplitN(content, []byte("\\"), -1) {
		data = append(data, v)
	}

	dt = bytes.Join(data, []byte(""))
	var config map[string]interface{}
	if err := json.Unmarshal(dt, &config); err != nil {
		return dt, fileName, fmt.Errorf("json.Unmarshal string [%s] failed: %v", name, err)
	}

	return dt, fileName, nil
}
