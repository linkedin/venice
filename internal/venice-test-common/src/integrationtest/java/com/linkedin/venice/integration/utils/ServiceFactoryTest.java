package com.linkedin.venice.integration.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.Test;


//  com.linkedin.venice.integration.utils.ServiceFactoryTest
public class ServiceFactoryTest {
  private static final Logger LOGGER = LogManager.getLogger(ServiceFactoryTest.class);

  public void sayHello() {
    System.out.println("###Hello, folks!");
  }

  @Test
  public void simpleLoadingTest() {
    System.out.println(ServiceFactory.testGame());
    // pubsub.backend.class.fqdn
  }

  public static void main(String[] args) throws InterruptedException {
    System.out.println(ServiceFactory.testGame());
    // ZkServerWrapper zkServerWrapper0 = ServiceFactory.getZkServer();
    // LOGGER.info("##ZK1: {}", zkServerWrapper0);
    // ZkServerWrapper zkServerWrapper1 = ServiceFactory.getZkServer();
    // LOGGER.info("##ZK1: {}", zkServerWrapper1);
    // ZkServerWrapper zkServerWrapper2 = ServiceFactory.getZkServer();
    // LOGGER.info("##ZK1: {}", zkServerWrapper2);
    // Thread.sleep(Long.MAX_VALUE);
  }
}
