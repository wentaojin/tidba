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
package printer

import "strings"

// DisplayMode control the output format
type DisplayMode int

// display modes
const (
	DisplayModeDefault DisplayMode = iota // default is the interactive output
	DisplayModePlain                      // plain text
	DisplayModeJSON                       // JSON
)

func fmtDisplayMode(m string) DisplayMode {
	var dp DisplayMode
	switch strings.ToLower(m) {
	case "json":
		dp = DisplayModeJSON
	case "plain", "text":
		dp = DisplayModePlain
	default:
		dp = DisplayModeDefault
	}
	return dp
}
