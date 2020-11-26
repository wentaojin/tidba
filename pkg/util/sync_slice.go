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

type ServiceData struct {
	Ch   chan []string // 用来 同步的channel
	Data []string      // 存储数据的slice
}

func (s *ServiceData) Schedule(nums int) {
	// 从 channel 接收数据
	for i := range s.Ch {
		for j := 0; j < nums; j++ {
			s.Data = append(s.Data, i...)
		}
	}
}

func (s *ServiceData) Close() {
	// 最后关闭 channel
	close(s.Ch)
}

func (s *ServiceData) AddData(v []string) {
	s.Ch <- v // 发送数据到 channel
}

func NewScheduleJob(size int, nums int, done func()) *ServiceData {
	s := &ServiceData{
		Ch:   make(chan []string, size),
		Data: make([]string, 0),
	}

	go func(j int) {
		// 并发地 append 数据到 slice
		s.Schedule(j)
		done()
	}(nums)

	return s
}
