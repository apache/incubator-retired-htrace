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
package org.apache.htrace.util;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

/**
 * Small util for making a data directory for tests to use when running tests. We put it up at
 * target/test-data/UUID.  Create an instance of this class per unit test run and it will take
 * care of setting up the dirs for you.  Pass what is returned here as location from which to
 * have daemons and tests dump data.
 * TODO: Add close on exit.
 */
public class DataDir {
  private File baseTestDir = null;
  private File testDir = null;

  /**
   * System property key to get base test directory value
   */
  public static final String TEST_BASE_DIRECTORY_KEY = "test.data.base.dir";

  /**
   * Default base directory for test output.
   */
  public static final String TEST_BASE_DIRECTORY_DEFAULT = "target";

  public static final String TEST_BASE_DIRECTORY_NAME = "test-data";

  /**
   * @return Where to write test data on local filesystem; usually
   * {@link #TEST_BASE_DIRECTORY_DEFAULT}
   * Should not be used directly by the unit tests, hence its's private.
   * Unit test will use a subdirectory of this directory.
   * @see #setupDataTestDir()
   */
  private synchronized File getBaseTestDir() {
    if (this.baseTestDir != null) return this.baseTestDir;
    String testHome = System.getProperty(TEST_BASE_DIRECTORY_KEY, TEST_BASE_DIRECTORY_DEFAULT);
    this.baseTestDir = new File(testHome, TEST_BASE_DIRECTORY_NAME);
    return this.baseTestDir;
  }

  /**
   * @return Absolute path to the dir created by this instance.
   * @throws IOException 
   */
  public synchronized File getDataDir() throws IOException {
    if (this.testDir != null) return this.testDir;
    this.testDir = new File(getBaseTestDir(), UUID.randomUUID().toString());
    if (!this.testDir.exists()) {
      if (!this.testDir.mkdirs()) throw new IOException("Failed mkdirs for " + this.testDir);
    }
    // Return absolute path. A relative passed to htraced will have it create data dirs relative
    // to its data dir rather than in it.
    return this.testDir.getAbsoluteFile();
  }

  /**
   * Fragile. Ugly. Presumes paths. Best we can do for now until htraced comes local to this module
   * and is moved out of src dir.
   * @param dataDir A datadir gotten from {@link #getDataDir()}
   * @return Top-level of the checkout.
   */
  public static File getTopLevelOfCheckout(final File dataDir) {
    // Need absolute else we run out of road when dir is relative to this module.
    File absolute = dataDir.getAbsoluteFile();
    // Check we are where we think we are.
    File testDataDir = absolute.getParentFile();
    if (!testDataDir.getName().equals(TEST_BASE_DIRECTORY_NAME)) {
      throw new IllegalArgumentException(dataDir.toString());
    }
    // Do another check.
    File targetDir = testDataDir.getParentFile();
    if (!targetDir.getName().equals(TEST_BASE_DIRECTORY_DEFAULT)) {
      throw new IllegalArgumentException(dataDir.toString());
    }
    // Back up last two dirs out of the htrace-htraced dir.
    return targetDir.getParentFile().getParentFile();
  }
}