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
package stringutil

import (
	"sync"
)

// ConcurrentArray is a concurrent-safe general-purpose array data structure
type ConcurrentArray struct {
	mu   sync.Mutex
	data []interface{}
}

func NewConcurrentArray() *ConcurrentArray {
	return &ConcurrentArray{
		data: make([]interface{}, 0),
	}
}

func (ca *ConcurrentArray) Append(element interface{}) {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	ca.data = append(ca.data, element)
}

func (ca *ConcurrentArray) Get(index int) (interface{}, bool) {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	if index < 0 || index >= len(ca.data) {
		return nil, false
	}
	return ca.data[index], true
}

func (ca *ConcurrentArray) Len() int {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	return len(ca.data)
}

func (ca *ConcurrentArray) All() []interface{} {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	// Return a copy to avoid external modification
	copiedData := make([]interface{}, len(ca.data))
	copy(copiedData, ca.data)
	return copiedData
}
