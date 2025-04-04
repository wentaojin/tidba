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
package task

import (
	"context"
	"fmt"
	"time"

	"github.com/wentaojin/tidba/utils/cluster/ctxt"
	"github.com/wentaojin/tidba/utils/cluster/executor"
)

// RootSSH is used to establish a SSH connection to the target host with specific key
type RootSSH struct {
	host            string           // hostname of the SSH server
	port            int              // port of the SSH server
	user            string           // username to login to the SSH server
	password        string           // password of the user
	keyFile         string           // path to the private key file
	passphrase      string           // passphrase of the private key file
	timeout         uint64           // timeout in seconds when connecting via SSH
	exeTimeout      uint64           // timeout in seconds waiting command to finish
	proxyHost       string           // hostname of the proxy SSH server
	proxyPort       int              // port of the proxy SSH server
	proxyUser       string           // username to login to the proxy SSH server
	proxyPassword   string           // password of the proxy user
	proxyKeyFile    string           // path to the private key file
	proxyPassphrase string           // passphrase of the private key file
	proxyTimeout    uint64           // timeout in seconds when connecting via SSH
	sshType         executor.SSHType // the type of SSH chanel
	sudo            bool
}

// Execute implements the Task interface
func (s *RootSSH) Execute(ctx context.Context) error {
	sc := executor.SSHConfig{
		Host:           s.host,
		Port:           s.port,
		User:           s.user,
		Password:       s.password,
		KeyFile:        s.keyFile,
		Passphrase:     s.passphrase,
		ConnectTimeout: time.Second * time.Duration(s.timeout),
		ExecuteTimeout: time.Second * time.Duration(s.exeTimeout),
	}
	if len(s.proxyHost) > 0 {
		sc.Proxy = &executor.SSHConfig{
			Host:           s.proxyHost,
			Port:           s.proxyPort,
			User:           s.proxyUser,
			Password:       s.proxyPassword,
			KeyFile:        s.proxyKeyFile,
			Passphrase:     s.proxyPassphrase,
			ConnectTimeout: time.Second * time.Duration(s.proxyTimeout),
		}
	}
	e, err := executor.New(s.sshType, s.sudo, sc)
	if err != nil {
		return err
	}

	ctxt.GetInner(ctx).SetExecutor(s.host, e)
	return nil
}

// Rollback implements the Task interface
func (s *RootSSH) Rollback(ctx context.Context) error {
	ctxt.GetInner(ctx).SetExecutor(s.host, nil)
	return nil
}

// String implements the fmt.Stringer interface
func (s *RootSSH) String() string {
	if len(s.keyFile) > 0 {
		return fmt.Sprintf("RootSSH: user=%s, host=%s, port=%d, key=%s", s.user, s.host, s.port, s.keyFile)
	}
	return fmt.Sprintf("RootSSH: user=%s, host=%s, port=%d", s.user, s.host, s.port)
}

// UserSSH is used to establish an SSH connection to the target host with generated key
type UserSSH struct {
	host            string
	port            int
	deployUser      string
	timeout         uint64
	exeTimeout      uint64 // timeout in seconds waiting command to finish
	proxyHost       string // hostname of the proxy SSH server
	proxyPort       int    // port of the proxy SSH server
	proxyUser       string // username to login to the proxy SSH server
	proxyPassword   string // password of the proxy user
	proxyKeyFile    string // path to the private key file
	proxyPassphrase string // passphrase of the private key file
	proxyTimeout    uint64 // timeout in seconds when connecting via SSH
	sshType         executor.SSHType
}

// Execute implements the Task interface
func (s *UserSSH) Execute(ctx context.Context) error {
	sc := executor.SSHConfig{
		Host:           s.host,
		Port:           s.port,
		KeyFile:        ctxt.GetInner(ctx).PrivateKeyPath,
		User:           s.deployUser,
		ConnectTimeout: time.Second * time.Duration(s.timeout),
		ExecuteTimeout: time.Second * time.Duration(s.exeTimeout),
	}

	if len(s.proxyHost) > 0 {
		sc.Proxy = &executor.SSHConfig{
			Host:           s.proxyHost,
			Port:           s.proxyPort,
			User:           s.proxyUser,
			Password:       s.proxyPassword,
			KeyFile:        s.proxyKeyFile,
			Passphrase:     s.proxyPassphrase,
			ConnectTimeout: time.Second * time.Duration(s.proxyTimeout),
		}
	}
	e, err := executor.New(s.sshType, false, sc)
	if err != nil {
		return err
	}

	ctxt.GetInner(ctx).SetExecutor(s.host, e)
	return nil
}

// Rollback implements the Task interface
func (s *UserSSH) Rollback(ctx context.Context) error {
	ctxt.GetInner(ctx).SetExecutor(s.host, nil)
	return nil
}

// String implements the fmt.Stringer interface
func (s *UserSSH) String() string {
	return fmt.Sprintf("UserSSH: user=%s, host=%s", s.deployUser, s.host)
}
