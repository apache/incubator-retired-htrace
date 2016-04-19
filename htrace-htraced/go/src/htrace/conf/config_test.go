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
	"bytes"
	"os"
	"strings"
	"testing"
)

// Test that parsing command-line arguments of the form -Dfoo=bar works.
func TestParseArgV(t *testing.T) {
	t.Parallel()
	argv := []string{"-Dfoo=bar", "-Dbaz=123", "-DsillyMode", "-Dlog.path="}
	bld := &Builder{Argv: argv,
		Defaults: map[string]string{
			"log.path": "/log/path/default",
		}}
	cnf, err := bld.Build()
	if err != nil {
		t.Fatal()
	}
	if "bar" != cnf.Get("foo") {
		t.Fatal()
	}
	if 123 != cnf.GetInt("baz") {
		t.Fatal()
	}
	if !cnf.GetBool("sillyMode") {
		t.Fatal()
	}
	if cnf.GetBool("otherSillyMode") {
		t.Fatal()
	}
	if "" != cnf.Get("log.path") {
		t.Fatal()
	}
}

// Test that default values work.
// Defaults are used only when the configuration option is not present or can't be parsed.
func TestDefaults(t *testing.T) {
	t.Parallel()
	argv := []string{"-Dfoo=bar", "-Dbaz=invalidNumber"}
	defaults := map[string]string{
		"foo":  "notbar",
		"baz":  "456",
		"foo2": "4611686018427387904",
	}
	bld := &Builder{Argv: argv, Defaults: defaults}
	cnf, err := bld.Build()
	if err != nil {
		t.Fatal()
	}
	if "bar" != cnf.Get("foo") {
		t.Fatal()
	}
	if 456 != cnf.GetInt("baz") {
		t.Fatal()
	}
	if 4611686018427387904 != cnf.GetInt64("foo2") {
		t.Fatal()
	}
}

// Test that we can parse our XML configuration file.
func TestXmlConfigurationFile(t *testing.T) {
	t.Parallel()
	xml := `
<?xml version="1.0"?>
<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>
<configuration>
  <property>
    <name>foo.bar</name>
    <value>123</value>
  </property>
  <property>
    <name>foo.baz</name>
    <value>xmlValue</value>
  </property>
  <!--<property>
    <name>commented.out</name>
    <value>stuff</value>
  </property>-->
</configuration>
`
	xmlReader := strings.NewReader(xml)
	argv := []string{"-Dfoo.bar=456"}
	defaults := map[string]string{
		"foo.bar":     "789",
		"cmdline.opt": "4611686018427387904",
	}
	bld := &Builder{Argv: argv, Defaults: defaults, Reader: xmlReader}
	cnf, err := bld.Build()
	if err != nil {
		t.Fatal()
	}
	// The command-line argument takes precedence over the XML and the defaults.
	if 456 != cnf.GetInt("foo.bar") {
		t.Fatal()
	}
	if "xmlValue" != cnf.Get("foo.baz") {
		t.Fatalf("foo.baz = %s", cnf.Get("foo.baz"))
	}
	if "" != cnf.Get("commented.out") {
		t.Fatal()
	}
	if 4611686018427387904 != cnf.GetInt64("cmdline.opt") {
		t.Fatal()
	}
}

// Test our handling of the HTRACE_CONF_DIR environment variable.
func TestGetHTracedConfDirs(t *testing.T) {
	os.Setenv("HTRACED_CONF_DIR", "")
	dlog := new(bytes.Buffer)
	dirs := getHTracedConfDirs(dlog)
	if len(dirs) != 1 || dirs[0] != getDefaultHTracedConfDir() {
		t.Fatal()
	}
	os.Setenv("HTRACED_CONF_DIR", "/foo/bar:/baz")
	dirs = getHTracedConfDirs(dlog)
	if len(dirs) != 2 || dirs[0] != "/foo/bar" || dirs[1] != "/baz" {
		t.Fatal()
	}
}
