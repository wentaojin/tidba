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
package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/wentaojin/tidba/utils/cluster/operator"
)

func main() {
	// 示例YAML文件内容，模拟一个动态、结构未知的配置
	exampleYAMLContent := `---
app_name: "动态配置示例"
version: 2.5
features:
  - name: "登录"
    enabled: true
    params:
      max_retry: 3
      timeout_sec: 30
  - name: "支付"
    enabled: false
    params:
      max_retry: 5
      timeout_sec: 60
database:
  host: "localhost"
  port: 3306
  replicas: 
    - name: "replica1"
      host: "192.168.1.101"
    - name: "replica2"
      host: "192.168.1.102"
metadata:
  tags:
    env: "test"
    owner: "team-awesome"
  extra_info: null
services:
  auth:
    endpoint: "/api/auth"
    timeout: 30
  payment:
    endpoint: "/api/payment"
    timeout: 60
`
	// 1. 创建一个临时文件用于测试
	tmpFile, err := os.CreateTemp("", "example-*.yaml")
	if err != nil {
		log.Fatalf("创建临时文件失败: %v", err)
	}
	defer os.Remove(tmpFile.Name()) // 程序结束后清理临时文件

	// 将示例内容写入临时文件
	if _, err := tmpFile.Write([]byte(exampleYAMLContent)); err != nil {
		log.Fatalf("写入临时文件失败: %v", err)
	}
	tmpFile.Close()

	// 2. 调用函数读取并解析YAML
	parsedData, err := operator.LoadYAMLToGeneric(tmpFile.Name())
	if err != nil {
		log.Fatalf("加载YAML失败: %v", err)
	}

	// 示例2: 获取features下的name（父节点是数组）
	fmt.Println("\n示例2: 获取features下的name")
	results2, err := operator.GetSubKeyContent(parsedData, "features", "")
	if err != nil && !strings.Contains(err.Error(), "not found parent key path") {
		panic(err)
	}
	if len(results2) == 0 {
		fmt.Println("未找到子key内容")
	}
	fmt.Println("features 下的 name 内容:", results2)

	inst, err := operator.ParseComputeComponentInstanceLogDir(results2)
	if err != nil {
		panic(err)
	}
	fmt.Println("features 下的 name 内容:", inst)
}
