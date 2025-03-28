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
package request

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net/http"
	"os"
)

const (
	DefaultRequestMethodGet  = "GET"
	DefaultRequestMethodPost = "POST"
)

func Request(method, url string, body []byte, cacertPath, certPath string) ([]byte, error) {
	client, err := createHTTPClient(cacertPath, certPath)
	if err != nil {
		return nil, err
	}

	respBody, err := doRequest(client, method, url, body)
	if err != nil {
		return nil, err
	}

	return respBody, nil
}

// createHTTPClient creates an HTTP client with optional TLS configuration.
func createHTTPClient(cacertPath, certPath string) (*http.Client, error) {
	var tlsConfig *tls.Config

	// If either cacertPath or certPath is provided, set up TLS config
	if cacertPath != "" || certPath != "" {
		caCertPool, err := loadCACert(cacertPath)
		if err != nil {
			return nil, err
		}

		cert, err := loadClientCert(certPath)
		if err != nil {
			return nil, err
		}

		tlsConfig = &tls.Config{
			RootCAs:      caCertPool,
			Certificates: []tls.Certificate{cert},
		}
	}

	transport := &http.Transport{TLSClientConfig: tlsConfig}
	client := &http.Client{Transport: transport}
	return client, nil
}

// loadCACert loads the CA certificate from the given path.
func loadCACert(cacertPath string) (*x509.CertPool, error) {
	if cacertPath == "" {
		return nil, nil
	}

	caCert, err := os.ReadFile(cacertPath)
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	return caCertPool, nil
}

// loadClientCert loads the client certificate from the given path.
func loadClientCert(certPath string) (tls.Certificate, error) {
	if certPath == "" {
		return tls.Certificate{}, nil
	}

	cert, err := tls.LoadX509KeyPair(certPath, certPath)
	if err != nil {
		return tls.Certificate{}, err
	}

	return cert, nil
}

// doRequest performs the actual HTTP request.
func doRequest(client *http.Client, method, url string, body []byte) ([]byte, error) {
	var reqBody io.Reader
	if body != nil {
		reqBody = bytes.NewBuffer(body)
	}

	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return nil, err
	}

	if method != http.MethodGet && body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return respBody, nil
}
