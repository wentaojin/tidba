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
package operator

import (
	"fmt"
	"strings"
	"time"
)

var globalParser *UniversalTimeParser

func init() {
	// 全局解析器实例
	globalParser = NewUniversalTimeParser()
}

// UniversalTimeParser 通用时间解析器，支持多种时间格式
type UniversalTimeParser struct {
	// 支持的时间格式列表，按优先级排序
	formats []string
}

// NewUniversalTimeParser 创建新的通用时间解析器
func NewUniversalTimeParser() *UniversalTimeParser {
	return &UniversalTimeParser{
		formats: []string{
			// 1. ISO 8601 格式 (带时区)
			time.RFC3339,     // "2006-01-02T15:04:05Z07:00"
			time.RFC3339Nano, // "2006-01-02T15:04:05.999999999Z07:00"

			// 2. 常见数据库/日志格式
			"2006-01-02 15:04:05.999999999 -0700 MST",
			"2006-01-02 15:04:05.999999999 -0700",
			"2006-01-02 15:04:05.999999999",

			// 3. 标准日期时间格式
			"2006-01-02 15:04:05", // 用户指定的第一种格式
			"2006/01/02 15:04:05",
			"2006-01-02T15:04:05", // ISO 8601 不带时区

			// 4. 带毫秒/微秒/纳秒的格式
			"2006-01-02 15:04:05.999",
			"2006-01-02 15:04:05.999999",
			"2006-01-02T15:04:05.999",
			"2006-01-02T15:04:05.999999",
			"2006-01-02T15:04:05.999999999",

			// 5. 仅日期格式
			"2006-01-02",
			"2006/01/02",
			"02/01/2006",
			"01/02/2006",

			// 6. 时间戳格式
			time.RFC1123,  // "Mon, 02 Jan 2006 15:04:05 MST"
			time.RFC1123Z, // "Mon, 02 Jan 2006 15:04:05 -0700"
			time.RFC822,   // "02 Jan 06 15:04 MST"
			time.RFC822Z,  // "02 Jan 06 15:04 -0700"
			time.RFC850,   // "Monday, 02-Jan-06 15:04:05 MST"

			// 7. Unix 时间戳（字符串格式）
			"1136239445",       // Unix 时间戳（秒）
			"1136239445123",    // Unix 时间戳（毫秒）
			"1136239445123456", // Unix 时间戳（微秒）
		},
	}
}

// AddFormat 添加自定义时间格式
func (p *UniversalTimeParser) AddFormat(format string) {
	p.formats = append([]string{format}, p.formats...)
}

// Parse 尝试解析时间字符串，支持多种格式
func (p *UniversalTimeParser) Parse(timeStr string) (time.Time, error) {
	// 尝试所有预定义格式
	for _, format := range p.formats {
		if t, err := time.Parse(format, timeStr); err == nil {
			return t, nil
		}
	}

	// 如果所有预定义格式都失败，尝试解析为 Unix 时间戳
	if timestamp, err := parseUnixTimestamp(timeStr); err == nil {
		return timestamp, nil
	}

	return time.Time{}, fmt.Errorf("not valid time string: %s", timeStr)
}

// parseUnixTimestamp 尝试将字符串解析为 Unix 时间戳
func parseUnixTimestamp(timeStr string) (time.Time, error) {
	// 移除可能的空格
	timeStr = strings.TrimSpace(timeStr)

	// 尝试解析为不同精度的 Unix 时间戳
	var timestamp int64
	var err error

	// 根据字符串长度判断精度
	switch len(timeStr) {
	case 10: // 秒
		_, err = fmt.Sscanf(timeStr, "%d", &timestamp)
		if err == nil {
			return time.Unix(timestamp, 0), nil
		}
	case 13: // 毫秒
		_, err = fmt.Sscanf(timeStr, "%d", &timestamp)
		if err == nil {
			return time.Unix(0, timestamp*int64(time.Millisecond)), nil
		}
	case 16: // 微秒
		_, err = fmt.Sscanf(timeStr, "%d", &timestamp)
		if err == nil {
			return time.Unix(0, timestamp*int64(time.Microsecond)), nil
		}
	case 19: // 纳秒
		_, err = fmt.Sscanf(timeStr, "%d", &timestamp)
		if err == nil {
			return time.Unix(0, timestamp), nil
		}
	}

	return time.Time{}, fmt.Errorf("not valid unix timestamp: %s", timeStr)
}

// ParseWithLocation 解析时间字符串并转换为指定时区
func (p *UniversalTimeParser) ParseWithLocation(timeStr string, loc *time.Location) (time.Time, error) {
	t, err := p.Parse(timeStr)
	if err != nil {
		return time.Time{}, err
	}

	// 如果时间没有时区信息，使用指定时区
	if t.Location() == time.UTC {
		return t.In(loc), nil
	}

	return t, nil
}

// MustParse 解析时间字符串，如果失败则 panic
func (p *UniversalTimeParser) MustParse(timeStr string) time.Time {
	t, err := p.Parse(timeStr)
	if err != nil {
		panic(err)
	}
	return t
}

// ParseTime 使用全局解析器解析时间字符串
func ParseTime(timeStr string) (time.Time, error) {
	return globalParser.Parse(timeStr)
}

// ParseTimeWithLocation 使用全局解析器解析时间字符串并转换为指定时区
func ParseTimeWithLocation(timeStr string, loc *time.Location) (time.Time, error) {
	return globalParser.ParseWithLocation(timeStr, loc)
}
