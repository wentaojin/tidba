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

import (
	"fmt"
	"time"
)

// CalculateAutoStep 根据 Grafana 自动步长计算逻辑，动态计算合适的采样步长
// 参数：
// - duration: 时间范围总时长
// - panelWidth: 面板宽度（像素）
// 返回值：
// - step: 计算得到的步长（毫秒）
// 说明：
// - 该函数动态计算合适的采样步长，确保在给定面板宽度下，时间范围内的数据点数量不超过最大点数限制（Prometheus 默认11000）。
// - 步长会根据时间范围自动调整，确保在短时间范围内（如10分钟内）有更多数据点，而在长时间范围内（如1天内）有更少数据点。
// - 步长会根据面板宽度自动调整，确保每个数据点至少占用2像素，避免在高分辨率面板上数据点过密。
// step = max(初始步长, duration / (面板宽度 × 每点像素占比))
func CalculateAutoStep(duration time.Duration, panelWidth int) int64 {
	// 时间范围转换为毫秒
	durationMs := duration.Milliseconds()
	if durationMs <= 0 {
		return 1000 // 默认步长1秒
	}

	// 1. 基于时间范围确定初始步长（指数级增长的步长阶梯）
	var initialStepMs int64
	switch {
	case duration < 10*time.Minute:
		// < 10分钟：1-5秒
		initialStepMs = 1000 // 1秒
	case duration < 1*time.Hour:
		// 10分钟 ~ 1小时：15-30秒
		initialStepMs = 30000 // 30秒
	case duration < 6*time.Hour:
		// 1小时 ~ 6小时：30秒 ~ 1分钟
		initialStepMs = 30000 // 30秒
	case duration < 24*time.Hour:
		// 6小时 ~ 24小时：1分钟 ~ 5分钟
		initialStepMs = 300000 // 5分钟
	case duration < 7*24*time.Hour:
		// 1天 ~ 7天：5分钟 ~ 1小时
		initialStepMs = 300000 // 5分钟
	case duration < 30*24*time.Hour:
		// 7天 ~ 30天：1小时 ~ 6小时
		initialStepMs = 3600000 // 1小时
	default:
		// > 30天：6小时 ~ 1天
		initialStepMs = 21600000 // 6小时
	}

	// 2. 结合面板宽度微调步长，确保每个数据点至少占用2像素
	pixelsPerPoint := 2.0
	minStepMs := int64(float64(durationMs) / (float64(panelWidth) * pixelsPerPoint))
	if minStepMs < 1 {
		minStepMs = 1
	}

	// 3. 计算基于最大点数限制的步长
	maxPoints := 11000 // 默认 Prometheus 最大点数
	maxStepMs := int64(float64(durationMs) / float64(maxPoints))
	if maxStepMs < 1 {
		maxStepMs = 1
	}

	// 取最大值：初始步长、基于面板宽度的最小步长、基于最大点数的步长
	step := initialStepMs
	if step < minStepMs {
		step = minStepMs
	}
	if step < maxStepMs {
		step = maxStepMs
	}

	// 4. 步长对齐优化（可选）：将步长调整为常见的时间间隔（如1s, 5s, 10s, 30s, 1m, 5m等）
	// 这有助于提高查询效率和数据可读性
	return alignStep(step)
}

// alignStep 将步长对齐到常见的时间间隔
func alignStep(stepMs int64) int64 {
	// 常见时间间隔（毫秒）
	commonIntervals := []int64{
		1000,     // 1秒
		5000,     // 5秒
		10000,    // 10秒
		30000,    // 30秒
		60000,    // 1分钟
		300000,   // 5分钟
		600000,   // 10分钟
		1800000,  // 30分钟
		3600000,  // 1小时
		10800000, // 3小时
		21600000, // 6小时
		43200000, // 12小时
		86400000, // 1天
	}

	// 找到大于等于当前步长的最小常见间隔
	for _, interval := range commonIntervals {
		if interval >= stepMs {
			return interval
		}
	}

	// 如果当前步长大于所有常见间隔，返回最大的常见间隔
	return commonIntervals[len(commonIntervals)-1]
}

// CalculateAutoStepByRange 根据起始时间和结束时间计算自动步长
// 参数：
// - start: 起始时间
// - end: 结束时间
// - panelWidth: 面板宽度（像素）
// 返回值：
// - step: 计算得到的步长（毫秒）
func CalculateAutoStepByRange(start, end time.Time, panelWidth int) string {
	return GetFriendlyStep(CalculateAutoStep(end.Sub(start), panelWidth))
}

// CalculateAutoStepByRangeDefaultPanelWidth 使用默认面板宽度计算自动步长
// 参数：
// - start: 起始时间
// - end: 结束时间
// 返回值：
// - step: 计算得到的步长（毫秒）
// 说明：在直接API访问时，使用默认面板宽度1000像素，这是Grafana中典型的面板宽度值
func CalculateAutoStepByRangeDefaultPanelWidth(start, end time.Time) string {
	return CalculateAutoStepByRange(start, end, 1000) // 默认使用1000像素面板宽度
}

// GetFriendlyStep converts milliseconds to a friendly Prometheus step format
func GetFriendlyStep(milliseconds int64) string {
	// Convert milliseconds to seconds
	seconds := milliseconds / 1000

	// Define friendly step intervals in seconds with their string representations
	type friendlyStep struct {
		seconds int64
		format  string
	}

	friendlySteps := []friendlyStep{
		{30, "30s"},
		{60, "1m"},
		{300, "5m"},
		{900, "15m"},
		{1800, "30m"},
		{3600, "1h"},
		{7200, "2h"},
		{10800, "3h"},
		{21600, "6h"},
		{43200, "12h"},
		{86400, "24h"},
	}

	// Find the smallest friendly step that is >= the calculated step
	for _, fs := range friendlySteps {
		if fs.seconds >= seconds {
			return fs.format
		}
	}

	// If calculated step is larger than all friendly steps, use hours format
	return fmt.Sprintf("%dh", (seconds+3599)/3600) // Round up to nearest hour
}
