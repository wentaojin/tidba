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

import "github.com/wentaojin/tidba/utils/cluster/executor"

// Options represents the operation options
type Options struct {
	SSHUser             string           // the ssh user
	SSHPort             int              // the ssh user port
	SSHTimeout          uint64           // timeout in seconds when connecting an SSH server
	OptTimeout          uint64           // timeout in seconds for operations that support it, not to confuse with SSH timeout
	SSHType             executor.SSHType // the ssh type: 'builtin', 'system', 'none'
	Concurrency         int              // max number of parallel tasks to run
	SSHProxyHost        string           // the ssh proxy host
	SSHProxyPort        int              // the ssh proxy port
	SSHProxyUser        string           // the ssh proxy user
	SSHProxyIdentity    string           // the ssh proxy identity file
	SSHProxyUsePassword bool             // use password instead of identity file for ssh proxy connection
	SSHProxyTimeout     uint64           // timeout in seconds when connecting the proxy host
}
