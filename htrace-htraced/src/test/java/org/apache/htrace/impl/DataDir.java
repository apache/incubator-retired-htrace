/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.htrace.impl;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;

/**
 * Small util for making a data directory for tests to use when running tests. We put it up at
 * target/test-data/UUID.  Create an instance of this class per unit test run and it will take
 * care of setting up the dirs for you.  Pass what is returned here as location from which to
 * have daemons and tests dump data.
 */
public class DataDir implements Closeable {
  private final File dir;

  public DataDir() throws IOException {
    String baseDir = System.getProperty(
        "test.data.base.dir", "target");
    File testData = new File(new File(baseDir), "test-data");
    this.dir = new File(testData, UUID.randomUUID().toString());
    Files.createDirectories(this.dir.toPath());
  }

  public File get() {
    return dir;
  }

  @Override
  public void close() throws IOException {
    /*for (File file : this.dir.listFiles()) {
      file.delete();
    }
    Files.delete(this.dir.toPath()); */
  }

  @Override
  public String toString() {
    return "DataDir{" + dir.getAbsolutePath() + "}";
  }
}