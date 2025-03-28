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
	"sort"
	"sync"
)

type Container struct {
	m map[string]bool
	sync.RWMutex
}

func NewContainer() *Container {
	return &Container{
		m: map[string]bool{},
	}
}

func (s *Container) Add(items ...string) {
	s.Lock()
	defer s.Unlock()
	if len(items) == 0 {
		return
	}
	for _, item := range items {
		s.m[item] = true
	}
}

func (s *Container) Remove(items ...string) {
	s.Lock()
	defer s.Unlock()
	if len(items) == 0 {
		return
	}
	for _, item := range items {
		delete(s.m, item)
	}
}

func (s *Container) Has(item string) bool {
	s.RLock()
	defer s.RUnlock()
	_, ok := s.m[item]
	return ok
}

func (s *Container) Len() int {
	return len(s.List())
}

func (s *Container) Clear() {
	s.Lock()
	defer s.Unlock()
	s.m = map[string]bool{}
}

func (s *Container) IsEmpty() bool {
	return s.Len() == 0
}

func (s *Container) List() []string {
	s.RLock()
	defer s.RUnlock()
	list := []string{}
	for item := range s.m {
		list = append(list, item)
	}
	return list
}

func (s *Container) SortList() []string {
	s.RLock()
	defer s.RUnlock()
	list := []string{}
	for item := range s.m {
		list = append(list, item)
	}
	sort.Strings(list)
	return list
}
