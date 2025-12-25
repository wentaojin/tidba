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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// LoadYAMLToGeneric 从文件路径读取YAML，将其解析为通用的Go数据结构（interface{}）
func LoadYAMLToGeneric(filePath string) (interface{}, error) {
	// 1. 打开文件
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("打开文件失败: %w", err)
	}
	defer file.Close()

	// 2. 读取文件全部内容
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("读取文件失败: %w", err)
	}

	// 3. 解析YAML到通用的interface{}类型
	// yaml.v3 库的 Unmarshal 函数能够将YAML内容解析为
	// map[interface{}]interface{}, []interface{}, string, int, bool 等嵌套组合
	var result interface{}
	err = yaml.Unmarshal(data, &result)
	if err != nil {
		return nil, fmt.Errorf("解析YAML失败: %w", err)
	}

	return result, nil
}

// GetSubKeyContent 获取指定父key中的某个子key的所有内容
// parentKeyPath: 父key的路径，可以是点分隔的字符串（如"database.replicas"）或切片
// subKey: 要获取的子key名称
// 返回值: 找到的所有子key内容（可能是切片），以及错误信息
func GetSubKeyContent(data interface{}, parentKeyPath interface{}, subKey string) ([]interface{}, error) {
	var results []interface{}
	var parentPath []string

	// 处理parentKeyPath参数，统一转换为字符串切片
	switch v := parentKeyPath.(type) {
	case string:
		// 如果是点分隔的字符串，拆分为切片
		if v == "" {
			parentPath = []string{}
		} else {
			parentPath = strings.Split(v, ".")
		}
	case []string:
		parentPath = v
	default:
		return nil, fmt.Errorf("parentKeyPath must be string or string slice, received: %T", v)
	}

	// 辅助函数：递归查找父key
	var findParent func(interface{}, []string) (interface{}, error)
	findParent = func(currentData interface{}, path []string) (interface{}, error) {
		if len(path) == 0 {
			return currentData, nil
		}

		switch node := currentData.(type) {
		case map[interface{}]interface{}:
			// 转换为map[string]interface{}以便处理
			strMap := make(map[string]interface{})
			for k, v := range node {
				if keyStr, ok := k.(string); ok {
					strMap[keyStr] = v
				}
			}
			if nextNode, ok := strMap[path[0]]; ok {
				return findParent(nextNode, path[1:])
			}
		case map[string]interface{}:
			if nextNode, ok := node[path[0]]; ok {
				return findParent(nextNode, path[1:])
			}
		case []interface{}:
			// 如果当前节点是数组，则在每个元素中查找
			var allResults []interface{}
			for _, item := range node {
				if result, err := findParent(item, path); err == nil {
					allResults = append(allResults, result)
				}
			}
			if len(allResults) > 0 {
				return allResults, nil
			}
		}
		return nil, fmt.Errorf("not found parent key path: %v", path)
	}

	// 找到父节点
	parentNode, err := findParent(data, parentPath)
	if err != nil {
		return nil, err
	}

	// 如果subKey为空，则返回父节点的所有内容
	if subKey == "" {
		switch node := parentNode.(type) {
		case []interface{}:
			// 如果父节点是数组，直接返回数组内容
			results = node
		default:
			// 如果父节点不是数组，返回包含父节点的切片
			results = []interface{}{parentNode}
		}
		return results, nil
	}

	// 辅助函数：从父节点中提取子key内容
	var extractSubKey func(interface{}, string) []interface{}
	extractSubKey = func(parent interface{}, key string) []interface{} {
		var extracted []interface{}

		switch node := parent.(type) {
		case map[interface{}]interface{}:
			// 转换为map[string]interface{}以便处理
			strMap := make(map[string]interface{})
			for k, v := range node {
				if keyStr, ok := k.(string); ok {
					strMap[keyStr] = v
				}
			}
			if val, ok := strMap[key]; ok {
				extracted = append(extracted, val)
			}
		case map[string]interface{}:
			if val, ok := node[key]; ok {
				extracted = append(extracted, val)
			}
		case []interface{}:
			// 如果父节点是数组，遍历每个元素提取子key
			for _, item := range node {
				extracted = append(extracted, extractSubKey(item, key)...)
			}
		}
		return extracted
	}

	// 从父节点中提取子key内容
	results = extractSubKey(parentNode, subKey)

	if len(results) == 0 {
		return nil, fmt.Errorf("not found sub key [%s] in parent key path: %v", subKey, parentPath)
	}

	return results, nil
}

// FindAllKeys 辅助函数：查找YAML数据中所有的key路径
func FindAllKeys(data interface{}, currentPath string) map[string]interface{} {
	result := make(map[string]interface{})

	var traverse func(interface{}, string)
	traverse = func(node interface{}, path string) {
		switch v := node.(type) {
		case map[interface{}]interface{}:
			for key, val := range v {
				if keyStr, ok := key.(string); ok {
					fullPath := path
					if fullPath != "" {
						fullPath += "."
					}
					fullPath += keyStr
					result[fullPath] = val
					traverse(val, fullPath)
				}
			}
		case map[string]interface{}:
			for key, val := range v {
				fullPath := path
				if fullPath != "" {
					fullPath += "."
				}
				fullPath += key
				result[fullPath] = val
				traverse(val, fullPath)
			}
		case []interface{}:
			for i, item := range v {
				fullPath := fmt.Sprintf("%s[%d]", path, i)
				traverse(item, fullPath)
			}
		}
	}

	traverse(data, "")
	return result
}

type ServerInstance struct {
	Host       string                 `json:"host"`
	SshPort    int                    `json:"ssh_port"`
	Patched    bool                   `json:"patched" default:"false"`
	Port       int                    `json:"port"`
	StatusPort int                    `json:"status_port"`
	DeployDir  string                 `json:"deploy_dir"`
	LogDir     string                 `json:"log_dir"`
	Config     map[string]interface{} `json:"config"`
	Arch       string                 `json:"arch"`
	Os         string                 `json:"os"`
}

func ParseComputeComponentInstanceLogDir(results2 []interface{}) ([]*ServerInstance, error) {
	var insts []*ServerInstance

	for _, res := range results2 {
		if vals, ok := res.(map[string]interface{}); ok {
			jsStr, err := json.Marshal(vals)
			if err != nil {
				return nil, fmt.Errorf("parse component log dir marshal json failed, error: %v", err)
			}
			var inst *ServerInstance
			err = json.Unmarshal(jsStr, &inst)
			if err != nil {
				return nil, fmt.Errorf("parse component log dir unmarshal json failed, error: %v", err)
			}
			insts = append(insts, inst)
		}
	}
	return insts, nil
}
