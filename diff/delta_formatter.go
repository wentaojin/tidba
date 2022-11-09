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
	"errors"
	"fmt"
)

const (
	DeltaDelete   = 0
	DeltaTextDiff = 2
	DeltaMove     = 3
)

func NewDeltaFormatter() *DeltaFormatter {
	return &DeltaFormatter{
		PrintIndent: true,
	}
}

type DeltaFormatter struct {
	PrintIndent bool
}

func (f *DeltaFormatter) Format(diff Diff) (result string, err error) {
	jsonObject, err := f.formatObject(diff.Deltas())
	if err != nil {
		return "", err
	}
	var resultBytes []byte
	if f.PrintIndent {
		resultBytes, err = json.MarshalIndent(jsonObject, "", "  ")
	} else {
		resultBytes, err = json.Marshal(jsonObject)
	}
	if err != nil {
		return "", err
	}

	return string(resultBytes) + "\n", nil
}

func (f *DeltaFormatter) FormatAsJson(diff Diff) (json map[string]interface{}, err error) {
	return f.formatObject(diff.Deltas())
}

func (f *DeltaFormatter) formatObject(deltas []Delta) (deltaJson map[string]interface{}, err error) {
	deltaJson = map[string]interface{}{}
	for _, delta := range deltas {
		switch delta.(type) {
		case *Object:
			d := delta.(*Object)
			deltaJson[d.Position.String()], err = f.formatObject(d.Deltas)
			if err != nil {
				return nil, err
			}
		case *Array:
			d := delta.(*Array)
			deltaJson[d.Position.String()], err = f.formatArray(d.Deltas)
			if err != nil {
				return nil, err
			}
		case *Added:
			d := delta.(*Added)
			deltaJson[d.PostPosition().String()] = []interface{}{d.Value}
		case *Modified:
			d := delta.(*Modified)
			deltaJson[d.PostPosition().String()] = []interface{}{d.OldValue, d.NewValue}
		case *TextDiff:
			d := delta.(*TextDiff)
			deltaJson[d.PostPosition().String()] = []interface{}{d.DiffString(), 0, DeltaTextDiff}
		case *Deleted:
			d := delta.(*Deleted)
			deltaJson[d.PrePosition().String()] = []interface{}{d.Value, 0, DeltaDelete}
		case *Moved:
			return nil, errors.New("Delta type 'Move' is not supported in objects")
		default:
			return nil, errors.New(fmt.Sprintf("Unknown Delta type detected: %#v", delta))
		}
	}
	return
}

func (f *DeltaFormatter) formatArray(deltas []Delta) (deltaJson map[string]interface{}, err error) {
	deltaJson = map[string]interface{}{
		"_t": "a",
	}
	for _, delta := range deltas {
		switch delta.(type) {
		case *Object:
			d := delta.(*Object)
			deltaJson[d.Position.String()], err = f.formatObject(d.Deltas)
			if err != nil {
				return nil, err
			}
		case *Array:
			d := delta.(*Array)
			deltaJson[d.Position.String()], err = f.formatArray(d.Deltas)
			if err != nil {
				return nil, err
			}
		case *Added:
			d := delta.(*Added)
			deltaJson[d.PostPosition().String()] = []interface{}{d.Value}
		case *Modified:
			d := delta.(*Modified)
			deltaJson[d.PostPosition().String()] = []interface{}{d.OldValue, d.NewValue}
		case *TextDiff:
			d := delta.(*TextDiff)
			deltaJson[d.PostPosition().String()] = []interface{}{d.DiffString(), 0, DeltaTextDiff}
		case *Deleted:
			d := delta.(*Deleted)
			deltaJson["_"+d.PrePosition().String()] = []interface{}{d.Value, 0, DeltaDelete}
		case *Moved:
			d := delta.(*Moved)
			deltaJson["_"+d.PrePosition().String()] = []interface{}{"", d.PostPosition(), DeltaMove}
		default:
			return nil, errors.New(fmt.Sprintf("Unknown Delta type detected: %#v", delta))
		}
	}
	return
}
