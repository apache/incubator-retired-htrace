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

package main

//
// The bundler turns files into resources contained in go code.
//
// This is useful for serving HTML and Javascript files from a self-contained binary.
//

import (
	"bufio"
	"fmt"
	"gopkg.in/alecthomas/kingpin.v1"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
)

const APACHE_HEADER = `/*
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
`

const GENERATED_CODE_COMMENT = "// THIS IS GENERATED CODE.  DO NOT EDIT."

var SEP string = string(os.PathSeparator)

// Return true if a file contains a given string.
func fileContainsString(path, line string) (bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), line) {
			return true, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return false, err
	}
	return false, nil
}

// Converts a source file path to a destination file path
func sfileToDfile(sfile string) string {
	return strings.Replace(sfile, SEP, "__", -1) + ".go"
}

// Delete generated files that are in dfiles but not sfiles.
// sfiles and dfiles must be sorted by file name.
func deleteUnusedDst(sfiles []string, dst string, dfiles []os.FileInfo) error {
	s := 0
	for d := range dfiles {
		fullDst := dst + SEP + dfiles[d].Name()
		generated, err := fileContainsString(fullDst, GENERATED_CODE_COMMENT)
		if err != nil {
			return err
		}
		if !generated {
			// Skip this destination file, since it is not generated.
			continue
		}
		found := false
		for {
			if s >= len(sfiles) {
				break
			}
			tgt := sfileToDfile(sfiles[s])
			if tgt == dfiles[d].Name() {
				found = true
				break
			}
			if tgt > dfiles[d].Name() {
				break
			}
			s++
		}
		if !found {
			log.Printf("Removing %s\n", fullDst)
			err := os.Remove(fullDst)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func createBundleFile(pkg, src, sfile, dst string) error {
	// Open destination file and write header.
	tgt := sfileToDfile(sfile)
	fullDst := dst + SEP + tgt
	out, err := os.Create(fullDst)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = out.WriteString(APACHE_HEADER)
	if err != nil {
		return err
	}
	_, err = out.WriteString("\n" + GENERATED_CODE_COMMENT + "\n")
	if err != nil {
		return err
	}
	_, err = out.WriteString(fmt.Sprintf("\npackage %s\n", pkg))
	if err != nil {
		return err
	}
	_, err = out.WriteString(fmt.Sprintf("var _ = addResource(\"%s\", `\n", tgt[:len(tgt)-3]))
	if err != nil {
		return err
	}

	// Open source file and create scanner.
	fullSrc := src + SEP + sfile
	in, err := os.Open(fullSrc)
	if err != nil {
		return err
	}
	defer in.Close()
	reader := bufio.NewReader(in)
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		_, err := out.WriteString(strings.Replace(scanner.Text(), "`", "` + \"`\" + `", -1) + "\n")
		if err != nil {
			return err
		}
	}
	_, err = out.WriteString("`)\n")
	if err != nil {
		return err
	}
	err = out.Close()
	if err != nil {
		return err
	}
	return nil
}

func main() {
	app := kingpin.New("bundler", "The HTrace resource bundling build utility.")
	src := app.Flag("src", "Source path for bundled resources.").Default("").String()
	dst := app.Flag("dst", "Destination path for bundled resources.").Default("").String()
	pkg := app.Flag("pkg", "Package name to use for bundled resources.").Default("resource").String()
	kingpin.MustParse(app.Parse(os.Args[1:]))
	if *src == "" {
		log.Fatal("You must supply a src directory to bundle.")
	}
	if *dst == "" {
		log.Fatal("You must supply a dst directory for output.")
	}
	sfiles := make([]string, 0, 32)
	absSrc, err := filepath.Abs(filepath.Clean(*src))
	if err != nil {
		log.Fatalf("Error getting absolute path for %s: %s\n", *src, err.Error())
	}
	err = filepath.Walk(absSrc, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		var suffix string
		suffix, err = filepath.Rel(absSrc, path)
		if err != nil {
			return err
		}
		sfiles = append(sfiles, suffix)
		return nil
	})
	if err != nil {
		log.Fatal("Error listing files in src directory %s: %s\n", absSrc, err.Error())
	}
	var dfiles []os.FileInfo
	dfiles, err = ioutil.ReadDir(*dst)
	if err != nil {
		log.Fatal("Error listing files in output directory %s: %s\n", *dst, err.Error())
	}
	deleteUnusedDst(sfiles, *dst, dfiles)
	for s := range sfiles {
		err = createBundleFile(*pkg, absSrc, sfiles[s], *dst)
		if err != nil {
			log.Fatalf("Error creating bundle file for %s in %s: %s\n",
				sfiles[s], *dst, err.Error())
		}
		log.Printf("Bundled %s as %s\n", absSrc, absSrc + SEP + sfileToDfile(sfiles[s]))
	}
}
