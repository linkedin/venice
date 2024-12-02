package com.linkedin.venice.integration.utils;

import com.linkedin.venice.d2.D2Server;
import com.linkedin.venice.utils.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class MockD2ServerWrapper extends MockHttpServerWrapper {
  private static final Logger LOGGER = LogManager.getLogger(MockD2ServerWrapper.class);
  private final ZkServerWrapper zkServer;
  private final D2Server d2Server;

  static StatefulServiceProvider<MockD2ServerWrapper> generateService(String d2ServiceName) {
    return (serviceName, dataDirectory) -> new MockD2ServerWrapper(serviceName, TestUtils.getFreePort(), d2ServiceName);
  }

  public MockD2ServerWrapper(String serviceName, int port, String d2ServiceName) {
    super(serviceName, port);
    this.zkServer = ServiceFactory.getZkServer();

    String zkAddress = zkServer.getAddress();
    String d2ClusterName = D2TestUtils.setupD2Config(zkAddress, false, d2ServiceName);
    d2Server = D2TestUtils.createD2Server(zkAddress, "http://localhost:" + port, d2ClusterName);
  }

  @Override
  protected void internalStart() throws Exception {
    super.internalStart();
    d2Server.forceStart();
  }

  @Override
  protected void internalStop() throws Exception {
    super.internalStop();
    try {
      d2Server.notifyShutdown();
    } catch (RuntimeException e) {
      LOGGER.error("D2 announcer {} failed to shutdown properly", d2Server, e);
    }

    zkServer.close();
  }

  public String getZkAddress() {
    return zkServer.getAddress();
  }
}
