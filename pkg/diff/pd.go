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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/logrusorgru/aurora"
	"github.com/mattn/go-colorable"
)

// Indicates the same json value on both sides
var Equivalent = errors.New("equivalent")

func ComponentPDDiff(basePDAddr, newPDAddr, outFile string) error {
	var (
		basePDJson, newPDJson string
		err                   error
		opts                  []Option
		w                     io.Writer
		stdout                io.Writer
		au                    aurora.Aurora
	)
	if basePDJson, err = getPDConfigByAPI(basePDAddr); err != nil {
		return err
	}
	if newPDJson, err = getPDConfigByAPI(newPDAddr); err != nil {
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
	hunks, err := Diff([]byte(basePDJson), []byte(newPDJson), opts...)
	if err != nil {
		return fmt.Errorf("Error: diff failed: %s\n", err)
	}

	if len(hunks) == 0 {
		// Indicates the same json value on both sides
		// return nil
		return Equivalent
	}

	if _, err := fmt.Fprintf(w, "--- %v\n", au.Green(basePDAddr)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "+++ %v\n", au.Red(newPDAddr)); err != nil {
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

func getPDConfigByAPI(pdAddr string) (string, error) {
	response, err := http.Get(fmt.Sprintf("http://%s/pd/api/v1/config", pdAddr))
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
