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
package inspect

import "sync"

// AutoIncrement struct to hold the current value and a mutex for thread-safety.
type AutoIncrement struct {
	mu    sync.Mutex
	value int
}

// NewAutoIncrement initializes and returns a new AutoIncrement with an initial value.
func NewAutoIncrement(initialValue int) *AutoIncrement {
	return &AutoIncrement{
		value: initialValue,
	}
}

// Next increments the value by 1 and returns the new value.
func (ai *AutoIncrement) Next() int {
	ai.mu.Lock()
	defer ai.mu.Unlock()
	ai.value++
	return ai.value
}
