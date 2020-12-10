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
package main

import (
	"bytes"
	"fmt"
	"strings"
)

// start with two slightly different json documents
var aJSON = []byte(`{
  "a": 100,
  "foo": [1,2,3],
  "bar": false,
  "baz": {
    "a": {
      "b": 4,
      "c": false,
      "d": "apples-and-oranges"
    },
    "e": null,
    "g": "apples-and-oranges"
  }
}`)

var bJSON = []byte(`{
  "a": 100,
  "foo": [1,2,3],
  "bar": false,
  "baz": {
    "a": {
      "b": 4,
      "c": false,
      "d": "apples-and-oranges"
    },
    "e": "44556676788",
    "g": "apples-and-oranges"
  }
}`)

func main() {
	var dt []byte
	msg := `"{\"name\\\":\"zhangsan\", \"age\":18, \"id\":122463,\"sid\":122464}"`

	dt = bytes.Trim([]byte(msg), "\"")

	fmt.Println(string(dt))

	var s []string
	for _, v := range bytes.SplitN([]byte(dt), []byte("\\"), -1) {
		s = append(s, string(v))
	}
	data := strings.Join(s, "")
	fmt.Println(data)
}
