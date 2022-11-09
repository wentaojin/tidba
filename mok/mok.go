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
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/pingcap/tidb/util/codec"
)

type Node struct {
	typ      string // "key", "table_id", "row_id", "index_id", "index_values", "index_value", "ts"
	val      []byte
	variants []*Variant
}

type Variant struct {
	method   string
	children []*Node
}

func N(t string, v []byte) *Node {
	return &Node{typ: t, val: v}
}

func (n *Node) String(keyFormat string) string {
	switch n.typ {
	case "key", "index_values":
		switch keyFormat {
		case "hex":
			return `"` + strings.ToUpper(hex.EncodeToString(n.val)) + `"`
		case "base64":
			return `"` + base64.StdEncoding.EncodeToString(n.val) + `"`
		case "proto":
			return `"` + formatProto(string(n.val)) + `"`
		default:
			return fmt.Sprintf("%q", n.val)
		}
	case "table_id":
		_, id, _ := codec.DecodeInt(n.val)
		return fmt.Sprintf("table: %v", id)
	case "row_id":
		_, id, _ := codec.DecodeInt(n.val)
		return fmt.Sprintf("row: %v", id)
	case "index_id":
		_, id, _ := codec.DecodeInt(n.val)
		return fmt.Sprintf("index: %v", id)
	case "index_value":
		_, d, _ := codec.DecodeOne(n.val)
		s, _ := d.ToString()
		return fmt.Sprintf("kind: %v, value: %v", indexTypeToString[d.Kind()], s)
	case "ts":
		_, ts, _ := codec.DecodeUintDesc(n.val)
		return fmt.Sprintf("ts: %v (%v)", ts, GetTimeFromTS(uint64(ts)))
	}
	return fmt.Sprintf("%v:%q", n.typ, n.val)
}

func (n *Node) Expand() *Node {
	for _, fn := range rules {
		if t := fn(n); t != nil {
			for _, child := range t.children {
				child.Expand()
			}
			n.variants = append(n.variants, t)
		}
	}
	return n
}

func (n *Node) Print(keyFormat string) {
	fmt.Println(n.String(keyFormat))
	for i, t := range n.variants {
		t.PrintIndent(keyFormat, "", i == len(n.variants)-1)
	}
}

func (n *Node) PrintIndent(keyFormat, indent string, last bool) {
	indent = printIndent(indent, last)
	fmt.Println(n.String(keyFormat))
	for i, t := range n.variants {
		t.PrintIndent(keyFormat, indent, i == len(n.variants)-1)
	}
}

func (v *Variant) PrintIndent(keyFormat, indent string, last bool) {
	indent = printIndent(indent, last)
	fmt.Printf("## %s\n", v.method)
	for i, c := range v.children {
		c.PrintIndent(keyFormat, indent, i == len(v.children)-1)
	}
}

func printIndent(indent string, last bool) string {
	if last {
		fmt.Print(indent + "└─")
		return indent + "  "
	}
	fmt.Print(indent + "├─")
	return indent + "│ "
}

// Output used to split region by key
func (n *Node) Output(keyFormat string) []string {
	var (
		str []string
		out []string
	)
	for i, t := range n.variants {
		str = append(str, t.OutputIndent(keyFormat, "", i == len(n.variants)-1)...)
	}

	// delete null string
	for _, s := range str {
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func (v *Variant) OutputIndent(keyFormat, indent string, last bool) []string {
	var str []string
	for i, c := range v.children {
		f, s := c.OutputIndent(keyFormat, indent, i == len(v.children)-1)
		str = append(str, s...)
		str = append(str, f)
	}
	return str
}

func (n *Node) OutputIndent(keyFormat, indent string, last bool) (string, []string) {
	var str []string
	for i, t := range n.variants {
		str = append(str, t.OutputIndent(keyFormat, indent, i == len(n.variants)-1)...)
	}
	//fmt.Println(n.StringStr())
	return n.StringStr(), str
}

func (n *Node) StringStr() string {
	switch n.typ {
	case "row_id":
		_, id, _ := codec.DecodeInt(n.val)
		//return fmt.Sprintf("row: %v", id)
		return fmt.Sprintf("%v", id)
	case "index_value":
		_, d, _ := codec.DecodeOne(n.val)
		s, _ := d.ToString()
		//return fmt.Sprintf("kind: %v, value: %v", indexTypeToString[d.Kind()], s)
		return fmt.Sprintf("%v", s)
	default:
		return ""
	}
}
