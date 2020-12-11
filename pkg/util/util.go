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
package util

import (
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/scylladb/go-set"
	"github.com/scylladb/go-set/strset"
)

// IsExistInclude checks include is whether exist
func IsExistInclude(allTables, includeTables []string) (bool, []string) {
	s1 := set.NewStringSet()
	for _, t := range allTables {
		s1.Add(strings.ToLower(t))
	}
	s2 := set.NewStringSet()
	for _, t := range includeTables {
		s2.Add(strings.ToLower(t))
	}
	isSubset := s1.IsSubset(s2)
	var notExists []string
	if !isSubset {
		notExists = strset.Difference(s2, s1).List()
	}
	return isSubset, notExists
}

// FilterFromAll filters  from all
func FilterFromAll(allTables, excludeTables []string) []string {
	// exclude table from all tables
	s1 := set.NewStringSet()
	for _, t := range allTables {
		s1.Add(strings.ToLower(t))
	}
	s2 := set.NewStringSet()
	for _, t := range excludeTables {
		s2.Add(strings.ToLower(t))
	}
	return strset.Difference(s1, s2).List()
}

// RegexpFromAll regexp table from all
func RegexpFromAll(allTables []string, regex string) []string {
	var regexps []string
	rep := regexp.MustCompile(regex)
	for _, v := range allTables {
		if rep.MatchString(v) {
			regexps = append(regexps, v)
		}
	}
	return regexps
}

// IsExistPath checks path is whether exist
func IsExistPath(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// Paginate split slice by paginate
func Paginate(x []string, skip int, size int) []string {
	limit := func() int {
		if skip+size > len(x) {
			return len(x)
		} else {
			return skip + size
		}

	}

	start := func() int {
		if skip > len(x) {
			return len(x)
		} else {
			return skip
		}

	}
	return x[start():limit()]
}

//Int returns unique int values in a slice
func Int(slice []int) []int {
	uMap := make(map[int]struct{})
	result := []int{}
	for _, val := range slice {
		uMap[val] = struct{}{}
	}
	for key := range uMap {
		result = append(result, key)
	}
	sort.Ints(result)
	return result
}
