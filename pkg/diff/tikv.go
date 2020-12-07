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
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/logrusorgru/aurora"
	"github.com/mattn/go-colorable"
)

func ComponentTiKVDiffByAPI(baseTiKVAddr, newTiKVAddr, outFile string) error {
	var (
		baseTiKVJson, newTiKVJson string
		err                       error
		opts                      []Option
		w                         io.Writer
		stdout                    io.Writer
		au                        aurora.Aurora
	)
	if baseTiKVJson, err = getTiKVConfigByAPI(baseTiKVAddr); err != nil {
		return err
	}
	if newTiKVJson, err = getTiKVConfigByAPI(newTiKVAddr); err != nil {
		return err
	}

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
	hunks, err := Diff([]byte(baseTiKVJson), []byte(newTiKVJson), opts...)
	if err != nil {
		return fmt.Errorf("Error: diff by api failed: %s\n", err)
	}

	if len(hunks) == 0 {
		// Indicates the same json value on both sides
		// return nil
		return Equivalent
	}

	if _, err := fmt.Fprintf(w, "--- %v\n", au.Green(baseTiKVAddr)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "+++ %v\n", au.Red(newTiKVAddr)); err != nil {
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

func ComponentTiKVDiffByJSON(baseTiKVJson, newTiKVAddr, outFile string) error {
	var (
		baseTiKVJSON, newTiKVJson, fileName string
		err                                 error
		opts                                []Option
		w                                   io.Writer
		stdout                              io.Writer
		au                                  aurora.Aurora
	)
	if baseTiKVJSON, fileName, err = ReadJSONFile(baseTiKVJson); err != nil {
		return err
	}
	if newTiKVJson, err = getTiKVConfigByAPI(newTiKVAddr); err != nil {
		return err
	}

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
	hunks, err := Diff([]byte(baseTiKVJSON), []byte(newTiKVJson), opts...)
	if err != nil {
		return fmt.Errorf("Error: diff by json failed: %s\n", err)
	}

	if len(hunks) == 0 {
		// Indicates the same json value on both sides
		// return nil
		return Equivalent
	}

	if _, err := fmt.Fprintf(w, "--- %v\n", au.Green(fileName)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "+++ %v\n", au.Red(newTiKVAddr)); err != nil {
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

func getTiKVConfigByAPI(tikvAddr string) (string, error) {
	response, err := http.Get(fmt.Sprintf("http://%s/config", tikvAddr))
	if err != nil {
		return "", fmt.Errorf("http curl request get failed: %v", err)
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return "", fmt.Errorf("read request data failed: %v", err)
	}
	return string(body), nil
}
