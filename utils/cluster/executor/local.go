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
package executor

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"github.com/joomcode/errorx"

	"github.com/wentaojin/tidba/utils/stringutil"
)

// Local execute the command at local host.
type Local struct {
	Config *SSHConfig
	Sudo   bool   // all commands run with this executor will be using sudo
	Locale string // the locale used when executing the command
}

var _ Executor = &Local{}

// Execute implements Executor interface.
func (l *Local) Execute(ctx context.Context, cmd string, sudo bool, execTimeout ...time.Duration) ([]byte, []byte, error) {
	// change wd to default home
	cmd = fmt.Sprintf("cd; %s", cmd)
	// get current user name
	u, err := user.Current()
	if err != nil {
		return nil, nil, err
	}

	// try to acquire root permission
	if l.Sudo || sudo {
		cmd = fmt.Sprintf("/usr/bin/sudo -H -u root bash -c '%s'", cmd)
	} else if l.Config.User != u.Name {
		cmd = fmt.Sprintf("/usr/bin/sudo -H -u %s bash -c '%s'", l.Config.User, cmd)
	}

	// set a basic PATH in case it's empty on login
	cmd = fmt.Sprintf("PATH=$PATH:/bin:/sbin:/usr/bin:/usr/sbin %s", cmd)

	if l.Locale != "" {
		cmd = fmt.Sprintf("export LANG=%s; %s", l.Locale, cmd)
	}

	// run command on remote host
	// default timeout is 60s in easyssh-proxy
	if len(execTimeout) == 0 && l.Config.ExecuteTimeout != 0 {
		execTimeout = append(execTimeout, l.Config.ExecuteTimeout)
	} else if len(execTimeout) == 0 && l.Config.ExecuteTimeout == 0 {
		execTimeout = append(execTimeout, time.Duration(DefaultExecuteTimeout)*time.Second)
	}

	if len(execTimeout) > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, execTimeout[0])
		defer cancel()
	}

	command := exec.CommandContext(ctx, "/bin/bash", "-c", cmd)

	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)
	command.Stdout = stdout
	command.Stderr = stderr

	err = command.Run()

	if err != nil {
		baseErr := ErrSSHExecuteFailed.
			Wrap(err, "Failed to execute command locally").
			WithProperty(ErrPropSSHCommand, cmd).
			WithProperty(ErrPropSSHStdout, stdout).
			WithProperty(ErrPropSSHStderr, stderr)
		if len(stdout.Bytes()) > 0 || len(stderr.Bytes()) > 0 {
			output := strings.TrimSpace(strings.Join([]string{stdout.String(), stderr.String()}, "\n"))
			baseErr = baseErr.
				WithProperty(
					errorx.RegisterPrintableProperty(fmt.Sprintf("Command output on local host %s", l.Config.Host)), output)
		}
		return stdout.Bytes(), stderr.Bytes(), baseErr
	}

	return stdout.Bytes(), stderr.Bytes(), err
}

// Transfer implements Executer interface.
func (l *Local) Transfer(ctx context.Context, src, dst string, download bool, limit int) error {
	targetPath := filepath.Dir(dst)
	if err := stringutil.MkdirAll(targetPath, 0755); err != nil {
		return err
	}

	cmd := ""
	u, err := user.Current()
	if err != nil {
		return err
	}
	if download || u.Username == l.Config.User {
		cmd = fmt.Sprintf("cp %s %s", src, dst)
	} else {
		cmd = fmt.Sprintf("/usr/bin/sudo -H -u root bash -c \"cp %[1]s %[2]s && chown %[3]s:$(id -g -n %[3]s) %[2]s\"", src, dst, l.Config.User)
	}

	command := exec.Command("/bin/bash", "-c", cmd)
	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)
	command.Stdout = stdout
	command.Stderr = stderr

	err = command.Run()
	if err != nil {
		baseErr := ErrSSHExecuteFailed.
			Wrap(err, "Failed to transfer file over local cp").
			WithProperty(ErrPropSSHCommand, cmd).
			WithProperty(ErrPropSSHStdout, stdout).
			WithProperty(ErrPropSSHStderr, stderr)
		if len(stdout.Bytes()) > 0 || len(stderr.Bytes()) > 0 {
			output := strings.TrimSpace(strings.Join([]string{stdout.String(), stderr.String()}, "\n"))
			baseErr = baseErr.
				WithProperty(errorx.RegisterPrintableProperty(fmt.Sprintf("Command output on local host %s", l.Config.Host)), output)
		}
		return baseErr
	}

	return err
}
