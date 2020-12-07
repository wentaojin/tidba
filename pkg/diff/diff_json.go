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
	"math"
	"reflect"
	"sort"
	"strings"
)

type Hunk struct {
	Path     string
	Old, New *[]byte
}

func NewJSONFormatFunc(prettyPrint bool) func(string, []byte) string {
	return func(pfx string, b []byte) string {
		if !prettyPrint {
			return pfx + string(b)
		}
		var out bytes.Buffer
		je := json.NewEncoder(&out)
		je.SetIndent(pfx, "  ")
		if err := je.Encode(json.RawMessage(b)); err != nil {
			panic(err)
		}
		return pfx + strings.TrimSuffix(out.String(), "\n")
	}
}

func Diff(a, b []byte, opts ...Option) ([]Hunk, error) {
	var st state
	for _, opt := range opts {
		opt(&st)
	}

	aData, err := unmarshalJSON(a)
	if err != nil {
		return nil, err
	}
	bData, err := unmarshalJSON(b)
	if err != nil {
		return nil, err
	}
	return compare(st.PushState("$"), aData, bData)
}

func compare(st state, a, b interface{}) (hunks []Hunk, err error) {
	if !eqType(a, b) {
		hunks = append(hunks, Hunk{
			Path: st.Path,
			Old:  toBytesPtr(mustMarshalJSON(a)),
			New:  toBytesPtr(mustMarshalJSON(b)),
		})
		return hunks, nil
	}
	switch {
	case isJSONObject(a):
		return compareJSONObject(st, a.(map[string]interface{}), b.(map[string]interface{}))
	case isJSONArray(a):
		return compareJSONArray(st, a.([]interface{}), b.([]interface{}))
	default:
		if reflect.DeepEqual(a, b) {
			return nil, nil
		} else {
			hunks = append(hunks, Hunk{
				Path: st.Path,
				Old:  toBytesPtr(mustMarshalJSON(a)),
				New:  toBytesPtr(mustMarshalJSON(b)),
			})
			return hunks, nil
		}
	}
}

func compareAOnly(st state, a interface{}) (hunks []Hunk, err error) {
	hunks = append(hunks, Hunk{
		Path: st.Path,
		Old:  toBytesPtr(mustMarshalJSON(a)),
		New:  nil,
	})
	return hunks, nil
}

func compareBOnly(st state, b interface{}) (hunks []Hunk, err error) {
	hunks = append(hunks, Hunk{
		Path: st.Path,
		Old:  nil,
		New:  toBytesPtr(mustMarshalJSON(b)),
	})
	return hunks, nil
}

func isJSONObject(v interface{}) bool {
	_, ok := v.(map[string]interface{})
	return ok
}

func isJSONArray(v interface{}) bool {
	_, ok := v.([]interface{})
	return ok
}

func compareJSONObject(st state, a, b map[string]interface{}) (hunks []Hunk, err error) {
	var aOnlyKeys []string
	var commonKeys []string
	for k := range a {
		if _, ok := b[k]; ok {
			commonKeys = append(commonKeys, k)
		} else {
			aOnlyKeys = append(aOnlyKeys, k)
		}
	}
	var bOnlyKeys []string
	for k := range b {
		if _, ok := a[k]; !ok {
			bOnlyKeys = append(bOnlyKeys, k)
		}
	}
	sort.Strings(aOnlyKeys)
	sort.Strings(commonKeys)
	sort.Strings(bOnlyKeys)

	for _, k := range aOnlyKeys {
		h, err := compareAOnly(st.PushState("."+k), a[k])
		if err != nil {
			return nil, err
		}
		hunks = append(hunks, h...)
	}
	for _, k := range commonKeys {
		h, err := compare(st.PushState("."+k), a[k], b[k])
		if err != nil {
			return nil, err
		}
		hunks = append(hunks, h...)
	}
	for _, k := range bOnlyKeys {
		h, err := compareBOnly(st.PushState("."+k), b[k])
		if err != nil {
			return nil, err
		}
		hunks = append(hunks, h...)
	}
	return hunks, nil
}

func compareJSONArray(st state, a, b []interface{}) (hunks []Hunk, err error) {
	aLen := len(a)
	bLen := len(b)
	cLen := int(math.Min(float64(aLen), float64(bLen)))
	if cLen > 0 {
		for i := 0; i < cLen; i++ {
			h, err := compare(st.PushState(fmt.Sprintf("[%d]", i)), a[i], b[i])
			if err != nil {
				return nil, err
			}
			hunks = append(hunks, h...)
		}
	}
	// a only eles
	if aLen > 0 {
		for i := cLen; i < aLen; i++ {
			h, err := compareAOnly(st.PushState(fmt.Sprintf("[%d]", i)), a[i])
			if err != nil {
				return nil, err
			}
			hunks = append(hunks, h...)
		}
	}
	// b only eles
	if bLen > 0 {
		for i := cLen; i < bLen; i++ {
			h, err := compareBOnly(st.PushState(fmt.Sprintf("[%d]", i)), b[i])
			if err != nil {
				return nil, err
			}
			hunks = append(hunks, h...)
		}
	}
	return hunks, nil
}

func toBytesPtr(v []byte) *[]byte {
	return &v
}

func mustMarshalJSON(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	var buffer bytes.Buffer
	if err := json.Compact(&buffer, b); err != nil {
		panic(err)
	}
	return buffer.Bytes()
}

func unmarshalJSON(data []byte) (interface{}, error) {
	var v interface{}
	jd := json.NewDecoder(bytes.NewReader(data))
	jd.UseNumber()
	if err := jd.Decode(&v); err == io.EOF {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return v, nil
}

func eqType(a, b interface{}) bool {
	aType := reflect.TypeOf(a)
	bType := reflect.TypeOf(b)
	return aType == bType
}
