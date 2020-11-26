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
package mok

import (
	"bytes"
	"fmt"
)

var (
	newline         = []byte("\n")
	spaces          = []byte("                                        ")
	endBraceNewline = []byte("}\n")
	backslashN      = []byte{'\\', 'n'}
	backslashR      = []byte{'\\', 'r'}
	backslashT      = []byte{'\\', 't'}
	backslashDQ     = []byte{'\\', '"'}
	backslashBS     = []byte{'\\', '\\'}
	posInf          = []byte("inf")
	negInf          = []byte("-inf")
	nan             = []byte("nan")
)

func formatProto(s string) string {
	var buf bytes.Buffer
	// Loop over the bytes, not the runes.
	for i := 0; i < len(s); i++ {
		// Divergence from C++: we don't escape apostrophes.
		// There's no need to escape them, and the C++ parser
		// copes with a naked apostrophe.
		switch c := s[i]; c {
		case '\n':
			buf.Write(backslashN)
		case '\r':
			buf.Write(backslashR)
		case '\t':
			buf.Write(backslashT)
		case '"':
			buf.Write(backslashDQ)
		case '\\':
			buf.Write(backslashBS)
		default:
			if isprint(c) {
				buf.WriteByte(c)
			} else {
				fmt.Fprintf(&buf, "\\%03o", c)
			}
		}
	}
	return buf.String()
}

// equivalent to C's isprint.
func isprint(c byte) bool {
	return c >= 0x20 && c < 0x7f
}
