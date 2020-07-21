/*
 * Copyright [2018 - 2018] Confluent Inc.
 */

package io.confluent.connect.elasticsearch.integration;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class PumbaDelayContainer extends GenericContainer<PumbaDelayContainer> {

  private static final String DEFAULT_DOCKER_IMAGE = "gaiaadm/pumba:latest";

  private static final String PUMBA_PAUSE_COMMAND =
      "--log-level debug --interval 5s netem --tc-image "
          + "gaiadocker/iproute2 --duration 1s delay es-container";

  public PumbaDelayContainer() {
    this(DEFAULT_DOCKER_IMAGE);
  }

  public PumbaDelayContainer(String dockerImageName) {
    super(dockerImageName);
    this.logger().info("Starting an Pumba delay container using [{}]", dockerImageName);
    this.setCommand(PUMBA_PAUSE_COMMAND);
    this.addFileSystemBind("/var/run/docker.sock", "/var/run/docker.sock", BindMode.READ_WRITE);
    this.withCreateContainerCmdModifier(cmd -> cmd.withName("pumba"));
    this.setWaitStrategy(Wait.defaultWaitStrategy());
  }
}
