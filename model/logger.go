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
package model

import (
	"io"
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func NewConsoleOutput() {
	output := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		NoColor:    true, // 禁用颜色
		TimeFormat: "2006-01-02 15:04:05.000",
	}
	log.Logger = log.Output(output)
}

func NewDisableConsoleOutput() {
	log.Logger = log.Output(io.Discard)
}
