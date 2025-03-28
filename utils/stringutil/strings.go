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
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/fatih/color"
	"golang.org/x/term"
)

// PromptForPassword reads a password input from console
func PromptForPassword(format string, a ...any) string {
	defer fmt.Println("")

	fmt.Printf(format, a...)

	input, err := term.ReadPassword(syscall.Stdin)

	if err != nil {
		return ""
	}
	return strings.TrimSpace(strings.Trim(string(input), "\n"))
}

// PromptForAnswerOrAbortError accepts string from console by user, generates AbortError if user does
// not input the pre-defined answer.
func PromptForAnswerOrAbortError(answer string, format string, a ...any) error {
	if pass, ans := PromptForConfirmAnswer(answer, format, a...); !pass {
		return fmt.Errorf("operation aborted by user (with incorrect answer '%s')", ans)
	}
	return nil
}

// PromptForConfirmAnswer accepts string from console by user, default to empty and only return
// true if the user input is exactly the same as pre-defined answer.
func PromptForConfirmAnswer(answer string, format string, a ...any) (bool, string) {
	ans := Prompt(fmt.Sprintf(format, a...) + fmt.Sprintf("\n(Type \"%s\" to continue)\n:", color.CyanString(answer)))
	if ans == answer {
		return true, ans
	}
	return false, ans
}

// Prompt accepts input from console by user
func Prompt(prompt string) string {
	if prompt != "" {
		prompt += " " // append a whitespace
	}
	fmt.Print(prompt)

	reader := bufio.NewReader(os.Stdin)
	input, err := reader.ReadString('\n')
	if err != nil {
		return ""
	}
	return strings.TrimSuffix(input, "\n")
}

// VersionOrdinal used for the database version comparison
func VersionOrdinal(version string) string {
	// ISO/IEC 14651:2011
	const maxByte = 1<<8 - 1
	vo := make([]byte, 0, len(version)+8)
	j := -1
	for i := 0; i < len(version); i++ {
		b := version[i]
		if '0' > b || b > '9' {
			vo = append(vo, b)
			j = -1
			continue
		}
		if j == -1 {
			vo = append(vo, 0x00)
			j = len(vo) - 1
		}
		if vo[j] == 1 && vo[j+1] == '0' {
			vo[j+1] = b
			continue
		}
		if vo[j]+1 > maxByte {
			panic("VersionOrdinal: invalid version")
		}
		vo = append(vo, b)
		vo[j]++
	}
	return BytesToString(vo)
}

// BytesToString used for bytes to string, reduce memory
// https://segmentfault.com/a/1190000037679588
func BytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// ParseDuration parses a duration string and returns the duration in hours.
func ParseDurationConvertHours(duration string) (float64, error) {
	var totalHours float64
	re := regexp.MustCompile(`(\d+h)?(\d+m)?(\d+s)?`)
	matches := re.FindStringSubmatch(duration)
	if matches == nil {
		return 0, fmt.Errorf("invalid duration format")
	}

	// Extract hours
	if matches[1] != "" {
		hours, err := strconv.Atoi(strings.TrimSuffix(matches[1], "h"))
		if err != nil {
			return 0, err
		}
		totalHours += float64(hours)
	}

	// Extract minutes
	if matches[2] != "" {
		minutes, err := strconv.Atoi(strings.TrimSuffix(matches[2], "m"))
		if err != nil {
			return 0, err
		}
		totalHours += float64(minutes) / 60
	}

	// Extract seconds
	if matches[3] != "" {
		seconds, err := strconv.Atoi(strings.TrimSuffix(matches[3], "s"))
		if err != nil {
			return 0, err
		}
		totalHours += float64(seconds) / 3600
	}

	return totalHours, nil
}

// TimeUnixToHours converts a Unix timestamp to hours since the Unix epoch.
func TimeUnixToHours(unixTime int64) float64 {
	duration := time.Duration(unixTime) * time.Second
	return duration.Hours()
}

// ChunkStrings divides a slice of strings into chunks of the specified size.
func ChunkStrings(slice []string, size int) [][]string {
	if size <= 0 {
		return nil
	}

	var chunks [][]string
	for size < len(slice) {
		slice, chunks = slice[size:], append(chunks, slice[0:size:size])
	}
	chunks = append(chunks, slice)
	return chunks
}

func FormatInterfaceToString(value interface{}) (string, error) {
	switch v := value.(type) {
	case int:
		return strconv.Itoa(v), nil
	case int8, int16, int32, int64:
		return strconv.FormatInt(v.(int64), 10), nil
	case uint, uint8, uint16, uint32, uint64:
		return strconv.FormatUint(v.(uint64), 10), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case bool:
		return strconv.FormatBool(v), nil
	case string:
		return v, nil
	default:
		return "", fmt.Errorf("unsupported data type: [%v]", v)
	}
}

func IsContainString(str string, sli []string) bool {
	for _, s := range sli {
		if str == s {
			return true
		}
	}
	return false
}

func IsContainStringIgnoreCase(str string, sli []string) bool {
	for _, s := range sli {
		if strings.EqualFold(str, s) {
			return true
		}
	}
	return false
}

// RemoveDeduplicateSlice takes a slice of strings and returns a new slice with duplicates removed.
func RemoveDeduplicateSlice(input []string) []string {
	encountered := make(map[string]bool)
	result := make([]string, 0)

	for _, value := range input {
		if encountered[value] {
			continue
		}
		encountered[value] = true
		result = append(result, value)
	}
	return result
}

// RandomSampleStringSlice randomly selects n elements from the given []string
func RandomSampleStringSlice(slice []string, n int) ([]string, error) {
	if len(slice) < n {
		return slice, nil
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	perm := make([]int, len(slice))
	for i := range perm {
		perm[i] = i
	}

	// Shuffle the index array
	for i := range perm {
		j := r.Intn(i + 1)
		perm[i], perm[j] = perm[j], perm[i]
	}

	// Select the first n elements according to the shuffled index
	result := make([]string, n)
	for i := 0; i < n; i++ {
		result[i] = slice[perm[i]]
	}

	return result, nil
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

func ArrayStringGroups(arr []string, num int64) [][]string {
	max := int64(len(arr))
	if max <= num {
		return [][]string{arr}
	}
	var quantity int64
	if max%num == 0 {
		quantity = max / num
	} else {
		quantity = (max / num) + 1
	}
	var segments = make([][]string, 0)
	var start, end, i int64
	for i = 1; i <= quantity; i++ {
		end = i * num
		if i != quantity {
			segments = append(segments, arr[start:end])
		} else {
			segments = append(segments, arr[start:])
		}
		start = i * num
	}
	return segments
}

func CompareSliceString(slice1, slice2 []string) []string {
	set := make(map[string]bool)
	for _, v := range slice2 {
		set[v] = true
	}

	var result []string
	for _, v := range slice1 {
		if !set[v] {
			result = append(result, v)
		}
	}
	return result
}
