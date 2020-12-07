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
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/logrusorgru/aurora"
)

type Option func(*state)

type state struct {
	// NOTE: format only similar to $.property or $.array[0]
	Path string
}

func (st state) PushState(suffix string) state {
	st.Path = st.Path + suffix
	return st
}

func WriteHunk(w io.Writer, au aurora.Aurora, hunk Hunk, jsonFormatFn func(string, []byte) string) error {
	var oldStr string
	if hunk.Old != nil {
		oldStr = jsonFormatFn("- ", *hunk.Old)
	}
	var newStr string
	if hunk.New != nil {
		newStr = jsonFormatFn("+ ", *hunk.New)
	}
	if _, err := fmt.Fprintln(w, au.Cyan(fmt.Sprintf("@@ %s @@", hunk.Path))); err != nil {
		return err
	}
	if oldStr != "" {
		if _, err := fmt.Fprintln(w, au.Green(oldStr)); err != nil {
			return err

		}
	}
	if newStr != "" {
		if _, err := fmt.Fprintln(w, au.Red(newStr)); err != nil {
			return err

		}
	}
	return nil
}

func ReadJSONFile(name string) (string, string, error) {
	var data string
	fileName := filepath.Base(name)
	file, err := os.Open(name)
	if err != nil {
		return data, fileName, err
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		return data, fileName, err
	}

	data = string(content)

	if !strings.HasPrefix(data, "\"") {
		data = "\"" + data
	}
	if !strings.HasSuffix(data, "\"") {
		data = data + "\""
	}

	data, err = strconv.Unquote(data)
	if err != nil {
		return data, fileName, err
	}
	return data, fileName, nil
}

func encodeJson(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)

	encoder.SetEscapeHTML(false)

	if err := encoder.Encode(v); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil

}
