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
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ProcessBuilder.Redirect;
import java.net.URL;

/**
 * To get instance of HTraced up and running, create an instance of this class.
 * Upon successful construction, htraced is running using <code>dataDir</code> as directory to
 * host data (leveldbs and logs).
 * TODO: We expect to find the htraced in a very particular place. Fragile. Will break if stuff
 * moves.
 * TODO: What if a port clash? How to have it come up another port then ask the process what port
 * it is running on?
 */
public class HTracedProcess extends Process {
  private final Process delegate;

  public HTracedProcess(final File pathToHTracedBinary, final File dataDir, final URL url)
  throws IOException {
    // web.address for htraced is hostname ':' port; no 'scheme' yet.
    String webAddress = url.getHost() + ":" + url.getPort();
    // Pass cmdline args to htraced to it uses our test dir for data.
    ProcessBuilder pb = new ProcessBuilder(pathToHTracedBinary.toString(),
      "-Dweb.address=" + webAddress,
      "-Ddata.store.clear=true",
      "-Ddata.store.directories=" + dataDir.toString());
    pb.redirectErrorStream(true);
    // Inherit STDERR/STDOUT i/o; dumps on console for now.  Can add logs later.
    pb.inheritIO();
    pb.directory(dataDir);
    this.delegate = pb.start();
    assert pb.redirectInput() == Redirect.PIPE;
    assert pb.redirectOutput().file() == dataDir;
    assert this.delegate.getInputStream().read() == -1;
  }

  public int hashCode() {
    return delegate.hashCode();
  }

  public OutputStream getOutputStream() {
    throw new UnsupportedOperationException("Unsupported until complaint; output on STDOUT");
  }

  public InputStream getInputStream() {
    throw new UnsupportedOperationException("Unsupported until complaint; output on STDOUT");
  }

  public boolean equals(Object obj) {
    return delegate.equals(obj);
  }

  public InputStream getErrorStream() {
    throw new UnsupportedOperationException("Unsupported until complaint; output on STDOUT");
  }

  public int waitFor() throws InterruptedException {
    return delegate.waitFor();
  }

  public int exitValue() {
    return delegate.exitValue();
  }

  public void destroy() {
    delegate.destroy();
  }

  public String toString() {
    return delegate.toString();
  }

  /**
   * Ugly but how else to do file-math?
   * @param topLevel Presumes top-level of the htrace checkout.
   * @return Path to the htraced binary.
   */
  public static File getPathToHTraceBinaryFromTopLevel(final File topLevel) {
    return new File(new File(new File(new File(new File(topLevel, "htrace-core"), "src"), "go"),
      "build"), "htraced");
  }
}