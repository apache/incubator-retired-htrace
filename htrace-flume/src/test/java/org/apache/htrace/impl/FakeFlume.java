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

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.Server;
import org.apache.flume.api.RpcTestUtils;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

final class FakeFlume implements TestRule {

  private final BlockingQueue<AvroFlumeEvent> receivedEvents =
      new ArrayBlockingQueue<AvroFlumeEvent>(1);

  private Server flumeServer;
  private AvroSourceProtocol protocol = new AvroSourceProtocol() {

    @Override
    public Status append(AvroFlumeEvent event) {
      receivedEvents.add(event);
      return Status.OK;
    }

    @Override
    public Status appendBatch(List<AvroFlumeEvent> events) {
      receivedEvents.addAll(events);
      return Status.OK;
    }
  };

  FakeFlume alwaysFail() {
    this.protocol = new RpcTestUtils.FailedAvroHandler();
    return this;
  }

  FakeFlume alwaysOk() {
    this.protocol = new RpcTestUtils.OKAvroHandler();
    return this;
  }

  String nextEventBodyAsString() throws InterruptedException {
    return new String(receivedEvents.take().getBody().array(), Charset.forName("UTF-8"));
  }

  @Override
  public Statement apply(final Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        start();
        try {
          base.evaluate();
        } finally {
          stop();
        }
      }
    };
  }

  private void start() {
    flumeServer = RpcTestUtils.startServer(new AvroSourceProtocol(){

      @Override
      public Status append(AvroFlumeEvent event) throws AvroRemoteException {
        return protocol.append(event);
      }

      @Override
      public Status appendBatch(List<AvroFlumeEvent> events) throws AvroRemoteException {
        return protocol.appendBatch(events);
      }
    });
  }

  private void stop() {
    if (flumeServer != null) {
      RpcTestUtils.stopServer(flumeServer);
      flumeServer = null;
    }
  }

  int getPort() {
    return flumeServer.getPort();
  }
}
