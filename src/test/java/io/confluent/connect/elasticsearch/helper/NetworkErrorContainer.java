/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.elasticsearch.helper;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class NetworkErrorContainer extends GenericContainer<NetworkErrorContainer> {

  private static final String DEFAULT_DOCKER_IMAGE = "gaiaadm/pumba:latest";

  private static final String PUMBA_PAUSE_COMMAND = "--log-level info --interval 120s pause --duration 10s ";
  private static final String DOCKER_SOCK = "/var/run/docker.sock";

  public NetworkErrorContainer(String containerToInterrupt) {
    this(DEFAULT_DOCKER_IMAGE, containerToInterrupt);
  }

  public NetworkErrorContainer(
      String dockerImageName,
      String containerToInterrupt
  ) {
    super(dockerImageName);

    setCommand(PUMBA_PAUSE_COMMAND + containerToInterrupt);
    addFileSystemBind(DOCKER_SOCK, DOCKER_SOCK, BindMode.READ_WRITE);
    setWaitStrategy(Wait.forLogMessage(".*pausing container.*", 1));
    withLogConsumer(l -> System.out.print(l.getUtf8String()));
  }
}