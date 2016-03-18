package com.linkedin.venice.integration.utils;

import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.utils.Utils;

/**
 */
public class InstanceWrapper extends ProcessWrapper {
  public static final String SERVICE_NAME = "Instance"; // TODO: "Instance" is kind of a shitty name. Too generic.

  private final Instance instance;

  private InstanceWrapper(Instance instance) {
    super(SERVICE_NAME, null);
    this.instance = instance;
  }

  static ServiceProvider<InstanceWrapper> generateService() {
    String hostName = Utils.getHostName();
    return (serviceName, port) -> new InstanceWrapper(
        new Instance(hostName + "_0", hostName, port, TestUtils.getFreePort())
    );
  }

  @Override
  public String getHost() {
    return instance.getHost();
  }

  @Override
  public int getPort() {
    return instance.getHttpPort();
  }

  public int getAdminPort() {
    return instance.getAdminPort();
  }

  public String getNodeId() {
    return instance.getNodeId();
  }

  public Instance getInstance() {
    return instance;
  }

  @Override
  protected void start() throws Exception {
    // no-op
  }

  @Override
  protected void stop() throws Exception {
    // no-op
  }
}
