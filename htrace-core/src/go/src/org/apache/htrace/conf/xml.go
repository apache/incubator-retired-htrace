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

package conf

import (
	"encoding/xml"
	"io"
	"log"
)

type configuration struct {
	Properties []propertyXml `xml:"property"`
}

type propertyXml struct {
	Name  string `xml:"name"`
	Value string `xml:"value"`
}

// Parse an XML configuration file.
func parseXml(reader io.Reader, m map[string]string) error {
	dec := xml.NewDecoder(reader)
	configurationXml := configuration{}
	err := dec.Decode(&configurationXml)
	if err != nil {
		return err
	}
	props := configurationXml.Properties
	for p := range props {
		key := props[p].Name
		value := props[p].Value
		if key == "" {
			log.Println("Warning: ignoring element with missing or empty <name>.")
			continue
		}
		if value == "" {
			log.Println("Warning: ignoring element with key " + key + " with missing or empty <value>.")
			continue
		}
		//log.Printf("setting %s to %s\n", key, value)
		m[key] = value
	}
	return nil
}
