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
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// PathNotExistOrCreate used for the filepath is whether exist, if not exist, then create
func PathNotExistOrCreate(path string) error {
	_, err := os.Stat(path)
	if err == nil {
		return nil
	}
	if os.IsNotExist(err) {
		err = os.MkdirAll(path, os.ModePerm)
		if err != nil {
			return fmt.Errorf("file dir MkdirAll failed: %v", err)
		}
	}
	return err
}

// IsPathExist check whether a path is exist
func IsPathExist(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

// CreateDir used to create dir
func CreateDir(path string) error {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return os.MkdirAll(path, 0755)
		}
		return err
	}
	return nil
}

// MkdirAll basically copied from os.MkdirAll, but use max(parent permission,minPerm)
func MkdirAll(path string, minPerm os.FileMode) error {
	// Fast path: if we can tell whether path is a directory or file, stop with success or error.
	dir, err := os.Stat(path)
	if err == nil {
		if dir.IsDir() {
			return nil
		}
		return &os.PathError{Op: "mkdir", Path: path, Err: syscall.ENOTDIR}
	}

	// Slow path: make sure parent exists and then call Mkdir for path.
	i := len(path)
	for i > 0 && os.IsPathSeparator(path[i-1]) { // Skip trailing path separator.
		i--
	}

	j := i
	for j > 0 && !os.IsPathSeparator(path[j-1]) { // Scan backward over element.
		j--
	}

	if j > 1 {
		// Create parent.
		err = MkdirAll(path[:j-1], minPerm)
		if err != nil {
			return err
		}
	}

	perm := minPerm
	fi, err := os.Stat(filepath.Dir(path))
	if err == nil {
		perm |= fi.Mode().Perm()
	}

	// Parent now exists; invoke Mkdir and use its result; inheritance parent perm.
	err = os.Mkdir(path, perm)
	if err != nil {
		// Handle arguments like "foo/." by
		// double-checking that directory doesn't exist.
		dir, err1 := os.Lstat(path)
		if err1 == nil && dir.IsDir() {
			return nil
		}
		return err
	}
	return nil
}
