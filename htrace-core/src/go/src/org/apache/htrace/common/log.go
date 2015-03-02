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

package common

import (
	"errors"
	"fmt"
	"org/apache/htrace/conf"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// A logSink is a place logs can be written to.
type logSink struct {
	path     logPath
	file     *os.File
	lock     sync.Mutex
	refCount int // protected by logFilesLock
}

// Write to the logSink.
func (sink *logSink) write(str string) {
	sink.lock.Lock()
	defer sink.lock.Unlock()
	_, err := sink.file.Write([]byte(str))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error logging to '%s': %s\n", sink.path, err.Error())
	}
}

// Unreference the logSink.  If there are no more references, and the logSink is
// closeable, then we will close it here.
func (sink *logSink) Unref() {
	logFilesLock.Lock()
	defer logFilesLock.Unlock()
	sink.refCount--
	if sink.refCount <= 0 {
		if sink.path.IsCloseable() {
			err := sink.file.Close()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error closing log file %s: %s\n",
					sink.path, err.Error())
			}
		}
		logSinks[sink.path] = nil
	}
}

type logPath string

// An empty LogPath represents "stdout."
const STDOUT_LOG_PATH = ""

// Convert a path to a logPath.
func logPathFromString(path string) logPath {
	if path == STDOUT_LOG_PATH {
		return logPath("")
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get absolute path of %s: %s\n",
			path, err.Error())
		return logPath(path)
	}
	return logPath(absPath)
}

// Convert the path to a human-readable string.
func (path logPath) String() string {
	if path == "" {
		return "(stdout)"
	} else {
		return string(path)
	}
}

// Return true if the path is closeable.  stdout is not closeable.
func (path logPath) IsCloseable() bool {
	return path != STDOUT_LOG_PATH
}

func (path logPath) Open() *logSink {
	if path == STDOUT_LOG_PATH {
		return &logSink{path: path, file: os.Stdout}
	}
	file, err := os.OpenFile(string(path), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0777)
	if err != nil {
		sink := &logSink{path: STDOUT_LOG_PATH, file: os.Stdout}
		fmt.Fprintf(os.Stderr, "Failed to open log file %s: %s\n",
			path, err.Error())
		return sink
	}
	return &logSink{path: path, file: file}
}

var logFilesLock sync.Mutex

var logSinks map[logPath]*logSink = make(map[logPath]*logSink)

func getOrCreateLogSink(pathStr string) *logSink {
	path := logPathFromString(pathStr)
	logFilesLock.Lock()
	defer logFilesLock.Unlock()
	sink := logSinks[path]
	if sink == nil {
		sink = path.Open()
		logSinks[path] = sink
	}
	sink.refCount++
	return sink
}

type Level int

const (
	TRACE Level = iota
	DEBUG
	INFO
	WARN
	ERROR
)

var levelToString map[Level]string = map[Level]string{
	TRACE: "TRACE",
	DEBUG: "DEBUG",
	INFO:  "INFO",
	WARN:  "WARN",
	ERROR: "ERROR",
}

func (level Level) String() string {
	return levelToString[level]
}

func (level Level) LogString() string {
	return level.String()[0:1]
}

func LevelFromString(str string) (Level, error) {
	for k, v := range levelToString {
		if strings.ToLower(v) == strings.ToLower(str) {
			return k, nil
		}
	}
	var levelNames sort.StringSlice
	levelNames = make([]string, len(levelToString))
	var i int
	for _, v := range levelToString {
		levelNames[i] = v
		i++
	}
	sort.Sort(levelNames)
	return TRACE, errors.New(fmt.Sprintf("No such level as '%s'.  Valid "+
		"levels are '%v'\n", str, levelNames))
}

type Logger struct {
	sink  *logSink
	Level Level
}

func NewLogger(faculty string, cnf *conf.Config) *Logger {
	path, level := parseConf(faculty, cnf)
	sink := getOrCreateLogSink(path)
	return &Logger{sink: sink, Level: level}
}

func parseConf(faculty string, cnf *conf.Config) (string, Level) {
	facultyLogPathKey := faculty + "." + conf.HTRACE_LOG_PATH
	var facultyLogPath string
	if cnf.Contains(facultyLogPathKey) {
		facultyLogPath = cnf.Get(facultyLogPathKey)
	} else {
		facultyLogPath = cnf.Get(conf.HTRACE_LOG_PATH)
	}
	facultyLogLevelKey := faculty + conf.HTRACE_LOG_LEVEL
	var facultyLogLevelStr string
	if cnf.Contains(facultyLogLevelKey) {
		facultyLogLevelStr = cnf.Get(facultyLogLevelKey)
	} else {
		facultyLogLevelStr = cnf.Get(conf.HTRACE_LOG_LEVEL)
	}
	level, err := LevelFromString(facultyLogLevelStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error configuring log level: %s.  Using TRACE.\n")
		level = TRACE
	}
	return facultyLogPath, level
}

func (lg *Logger) Trace(str string) {
	lg.write(TRACE, str)
}

func (lg *Logger) Tracef(format string, v ...interface{}) {
	lg.write(TRACE, fmt.Sprintf(format, v...))
}

func (lg *Logger) Debug(str string) {
	lg.write(DEBUG, str)
}

func (lg *Logger) Debugf(format string, v ...interface{}) {
	lg.write(DEBUG, fmt.Sprintf(format, v...))
}

func (lg *Logger) Info(str string) {
	lg.write(INFO, str)
}

func (lg *Logger) Infof(format string, v ...interface{}) {
	lg.write(INFO, fmt.Sprintf(format, v...))
}

func (lg *Logger) Warn(str string) error {
	lg.write(WARN, str)
	return errors.New(str)
}

func (lg *Logger) Warnf(format string, v ...interface{}) error {
	str := fmt.Sprintf(format, v...)
	lg.write(WARN, str)
	return errors.New(str)
}

func (lg *Logger) Error(str string) error {
	lg.write(ERROR, str)
	return errors.New(str)
}

func (lg *Logger) Errorf(format string, v ...interface{}) error {
	str := fmt.Sprintf(format, v...)
	lg.write(ERROR, str)
	return errors.New(str)
}

func (lg *Logger) write(level Level, str string) {
	if level >= lg.Level {
		lg.sink.write(time.Now().Format(time.RFC3339) + " " +
			level.LogString() + ": " + str)
	}
}

func (lg *Logger) Close() {
	lg.sink.Unref()
	lg.sink = nil
}
