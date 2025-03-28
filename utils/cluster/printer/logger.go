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

import (
	"fmt"
	"io"
	"os"
)

// ContextKey is key used to store values in context
type ContextKey string

// ContextKeyLogger is the key used for logger stored in context
const ContextKeyLogger ContextKey = "logger"

// Logger is a set of fuctions writing output to custom writters, but still
// using the global zap logger as our default config does not writes everything
// to a memory buffer.
type Logger struct {
	stdout    io.Writer
	stderr    io.Writer
	outputFmt DisplayMode //  output format of logger
}

// NewLogger creates a Logger with default settings
func NewLogger(m string) *Logger {
	return &Logger{
		stdout:    os.Stdout,
		stderr:    os.Stderr,
		outputFmt: fmtDisplayMode(m),
	}
}

// SetStdout redirect stdout to a custom writer
func (l *Logger) SetStdout(w io.Writer) {
	l.stdout = w
}

// SetStderr redirect stderr to a custom writer
func (l *Logger) SetStderr(w io.Writer) {
	l.stderr = w
}

// GetDisplayMode returns the current output format
func (l *Logger) GetDisplayMode() DisplayMode {
	return l.outputFmt
}

// Infof output the log message to console
func (l *Logger) Infof(format string, args ...any) {
	printLog(l.stdout, format, args...)
}

// Warnf output the warning message to console
func (l *Logger) Warnf(format string, args ...any) {
	printLog(l.stderr, format, args...)
}

// Errorf output the error message to console
func (l *Logger) Errorf(format string, args ...any) {
	printLog(l.stderr, format, args...)
}

func printLog(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, format+"\n", args...)
}
