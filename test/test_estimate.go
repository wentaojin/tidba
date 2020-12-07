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
	"fmt"

	"github.com/twotwotwo/sorts/sortutil"
)

func main() {
	slice := []string{"1", "2", "76", "3", "6", "7", "4", "5", "90"}

	// sort newCols order by asc
	// Set sorts.MaxProcs if you want to limit concurrency.
	sortutil.Strings(slice)

	fmt.Printf("result is %v\n", paginate(slice, 0, 2))
	fmt.Printf("result is %v\n", paginate(slice, 2, 2))
	fmt.Printf("result is %v\n", paginate(slice, 4, 2))
	fmt.Printf("result is %v\n", paginate(slice, 6, 2))
	fmt.Printf("result is %v\n", paginate(slice, 8, 2))

	skip := 0
	step := 2
	var kp [][]string
	var kw []string
	for i := 0; i < 5; i++ {
		kp = append(kp, paginate(slice, skip, step))
		kw = append(kw, paginate(slice, skip, step)[0])
		skip = skip + step
	}
	fmt.Println(kp)

	sortutil.Strings(kw)
	fmt.Println(kw)

}

func paginate(x []string, skip int, size int) []string {
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
