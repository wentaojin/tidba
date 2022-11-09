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
package diff

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

func ComponentPDDiff(basePDAddr, newPDAddr, format string, coloring, quiet bool) error {
	var (
		basePDJson, newPDJson string
		err                   error
	)
	if basePDJson, err = getPDConfigByAPI(basePDAddr); err != nil {
		return err
	}
	if newPDJson, err = getPDConfigByAPI(newPDAddr); err != nil {
		return err
	}
	if err := JSONDiff([]byte(basePDJson), []byte(newPDJson), basePDAddr, newPDAddr, format, coloring, quiet); err != nil {
		return err
	}
	return nil
}

func getPDConfigByAPI(pdAddr string) (string, error) {
	response, err := http.Get(fmt.Sprintf("http://%s/pd/api/v1/config", pdAddr))
	if err != nil {
		return "", fmt.Errorf("http curl request get failed: %v", err)
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return "", fmt.Errorf("read request data failed: %v", err)
	}
	return string(body), nil
}
