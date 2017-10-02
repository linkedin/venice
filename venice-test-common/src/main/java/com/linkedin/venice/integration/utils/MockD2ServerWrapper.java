package com.linkedin.venice.integration.utils;

import com.linkedin.d2.server.factory.D2Server;
import org.apache.log4j.Logger;


public class MockD2ServerWrapper extends MockHttpServerWrapper {
  private final Logger logger = Logger.getLogger(MockD2ServerWrapper.class);
  private ZkServerWrapper zkServer;
  private D2Server d2Server;

  static StatefulServiceProvider<MockD2ServerWrapper> generateService(String d2ClusterName, String d2ServiceName) {
    return ((serviceName, port, dataDirectory) -> new MockD2ServerWrapper(serviceName, port, d2ClusterName, d2ServiceName));
  }

  public MockD2ServerWrapper(String serviceName, int port, String d2ClusterName, String d2ServiceName) {
    super(serviceName, port);
    this.zkServer = ServiceFactory.getZkServer();

    String zkAddress = zkServer.getAddress();
    D2TestUtils.setupD2Config(zkAddress, false, d2ClusterName, d2ServiceName);
    d2Server = D2TestUtils.getD2Server(zkAddress, "http://localhost:" + port, d2ClusterName);
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
      logger.error("D2 announcer " + d2Server + " failed to shutdown properly", e);
    }

    zkServer.close();
  }

  public String getZkAddress() {
    return zkServer.getAddress();
  }
}
