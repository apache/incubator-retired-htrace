/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"org/apache/htrace/common"
	"org/apache/htrace/conf"
)

// A golang client for htraced.
// TODO: fancier APIs for streaming spans in the background, optimize TCP stuff

func NewClient(cnf *conf.Config) (*Client, error) {
	hcl := Client{restAddr: cnf.Get(conf.HTRACE_WEB_ADDRESS)}
	return &hcl, nil
}

type Client struct {
	// REST address of the htraced server.
	restAddr string
}

// Get the htraced server information.
func (hcl *Client) GetServerInfo() (*common.ServerInfo, error) {
	buf, _, err := hcl.makeGetRequest("server/info")
	if err != nil {
		return nil, err
	}
	var info common.ServerInfo
	err = json.Unmarshal(buf, &info)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error: error unmarshalling response "+
			"body %s: %s", string(buf), err.Error()))
	}
	return &info, nil
}

// Get information about a trace span.  Returns nil, nil if the span was not found.
func (hcl *Client) FindSpan(sid common.SpanId) (*common.Span, error) {
	buf, rc, err := hcl.makeGetRequest(fmt.Sprintf("span/%016x", uint64(sid)))
	if err != nil {
		if rc == http.StatusNoContent {
			return nil, nil
		}
		return nil, err
	}
	var span common.Span
	err = json.Unmarshal(buf, &span)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error unmarshalling response "+
			"body %s: %s", string(buf), err.Error()))
	}
	return &span, nil
}

func (hcl *Client) WriteSpan(span *common.Span) error {
	buf, err := json.Marshal(span)
	if err != nil {
		return err
	}
	_, _, err = hcl.makeRestRequest("POST", "writeSpans", bytes.NewReader(buf))
	if err != nil {
		return err
	}
	return nil
}

// Find the child IDs of a given span ID.
// TODO: add offset as well as limit?
func (hcl *Client) FindChildren(sid common.SpanId, lim int) ([]common.SpanId, error) {
	buf, _, err := hcl.makeGetRequest(fmt.Sprintf("span/%016x/children?lim=%d",
		uint64(sid), lim))
	if err != nil {
		return nil, err
	}
	var spanIds []common.SpanId
	err = json.Unmarshal(buf, &spanIds)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error: error unmarshalling response "+
			"body %s: %s", string(buf), err.Error()))
	}
	return spanIds, nil
}

// Make a query
func (hcl *Client) Query(query *common.Query) ([]common.Span, error) {
	in, err := json.Marshal(query)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error marshalling query: %s", err.Error()))
	}
	var out []byte
	var url = fmt.Sprintf("query?query=%s", in)
	out, _, err = hcl.makeGetRequest(url)
	if err != nil {
		return nil, err
	}
	var spans []common.Span
	err = json.Unmarshal(out, &spans)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Error unmarshalling results: %s", err.Error()))
	}
	return spans, nil
}

func (hcl *Client) makeGetRequest(reqName string) ([]byte, int, error) {
	return hcl.makeRestRequest("GET", reqName, nil)
}

// Make a general JSON REST request.
// Returns the request body, the response code, and the error.
// Note: if the response code is non-zero, the error will also be non-zero.
func (hcl *Client) makeRestRequest(reqType string, reqName string, reqBody io.Reader) ([]byte, int, error) {
	url := fmt.Sprintf("http://%s/%s", hcl.restAddr, reqName)
	req, err := http.NewRequest(reqType, url, reqBody)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, -1, errors.New(fmt.Sprintf("Error: error making http request to %s: %s\n", url,
			err.Error()))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode,
			errors.New(fmt.Sprintf("Error: got bad response status from %s: %s\n", url, resp.Status))
	}
	var body []byte
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, -1, errors.New(fmt.Sprintf("Error: error reading response body: %s\n", err.Error()))
	}
	return body, 0, nil
}
