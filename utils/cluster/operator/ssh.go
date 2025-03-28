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
	"fmt"
	"os"

	"errors"

	"github.com/ScaleFT/sshkeys"
	"github.com/wentaojin/tidba/utils/stringutil"
	"golang.org/x/crypto/ssh"
)

// SSHConnectionProps is SSHConnectionProps
type SSHConnectionProps struct {
	Password               string
	IdentityFile           string
	IdentityFilePassphrase string
}

// ReadIdentityFileOrPassword is ReadIdentityFileOrPassword
func ReadIdentityFileOrPassword(identityFilePath string, usePass bool) (*SSHConnectionProps, error) {
	// If identity file is not specified, prompt to read password
	if usePass {
		password := stringutil.PromptForPassword("Input SSH password: ")
		return &SSHConnectionProps{
			Password: password,
		}, nil
	}

	// Identity file is specified, check identity file
	if len(identityFilePath) > 0 && stringutil.IsPathExist(identityFilePath) {
		buf, err := os.ReadFile(identityFilePath)
		if err != nil {
			return nil, fmt.Errorf(`failed to read SSH identity file [%s], please check whether your SSH identity file [%v] exists and have access permission, error detail: %v`, identityFilePath, identityFilePath, err)
		}

		// Try to decode as not encrypted
		_, err = ssh.ParsePrivateKey(buf)
		if err == nil {
			return &SSHConnectionProps{
				IdentityFile: identityFilePath,
			}, nil
		}

		// Other kind of error.. e.g. not a valid SSH key
		var passphraseMissingError *ssh.PassphraseMissingError
		if !errors.As(err, &passphraseMissingError) {
			return nil, fmt.Errorf(`failed to read SSH identity file [%s], looks like your SSH private key [%v] is invalid, error detail: %v`, identityFilePath, identityFilePath, err)
		}

		// SSH key is passphrase protected
		passphrase := stringutil.PromptForPassword("The SSH identity key is encrypted. Input its passphrase: ")
		if _, err := sshkeys.ParseEncryptedPrivateKey(buf, []byte(passphrase)); err != nil {
			return nil, fmt.Errorf(`failed to decrypt SSH identity file [%s], error detail: %v`, identityFilePath, err)
		}

		return &SSHConnectionProps{
			IdentityFile:           identityFilePath,
			IdentityFilePassphrase: passphrase,
		}, nil
	}

	// No password, nor identity file were specified, check ssh-agent via the env SSH_AUTH_SOCK
	sshAuthSock := os.Getenv("SSH_AUTH_SOCK")
	if len(sshAuthSock) == 0 {
		return nil, fmt.Errorf("none of ssh password, identity file, SSH_AUTH_SOCK specified")
	}
	stat, err := os.Stat(sshAuthSock)
	if err != nil {
		return nil, fmt.Errorf(`failed to stat SSH_AUTH_SOCK file [%s], error detail: %v`, sshAuthSock, err)
	}
	if stat.Mode()&os.ModeSocket == 0 {
		return nil, fmt.Errorf("the SSH_AUTH_SOCK file [%s] is not a valid unix socket file, error detail: %v", sshAuthSock, err)
	}

	return &SSHConnectionProps{}, nil
}
