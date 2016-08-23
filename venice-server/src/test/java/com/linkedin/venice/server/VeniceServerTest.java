package com.linkedin.venice.server;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import org.testng.Assert;
import org.testng.annotations.Test;


public class VeniceServerTest {

  @Test
  public void testStartServerWhenDisableWhitelistChecking() {
    VeniceClusterWrapper clusterWrapper = ServiceFactory.getVeniceCluster();
    try {
      VeniceServerWrapper serverWrapper = clusterWrapper.getVeniceServers().get(0);
      Assert.assertTrue(serverWrapper.getVeniceServer().isStarted());
    } finally {
      clusterWrapper.close();
    }
  }

  @Test
  public void testStartServerWhenEnableWhitelistCheckingFailed() {
    String clusterName = "testStartServerWhenEnableWhitelistChecking";
    KafkaBrokerWrapper kafkaBrokerWrapper = ServiceFactory.getKafkaBroker();
    VeniceControllerWrapper controllerWrapper = ServiceFactory.getVeniceController(clusterName, kafkaBrokerWrapper);
    try {
      ServiceFactory.getVeniceServer(clusterName, kafkaBrokerWrapper, true, false);
      Assert.fail("Checking whitelist should fail because server has not been added into white list.");
    } catch (VeniceException e) {
      //expcted.
    } finally {
      controllerWrapper.close();
      kafkaBrokerWrapper.close();
    }
  }

  @Test
  public void testStartServerWhenEnableWhitelistCheckingSuccessful() {
    String clusterName = "testStartServerWhenEnableWhitelistChecking";
    KafkaBrokerWrapper kafkaBrokerWrapper = ServiceFactory.getKafkaBroker();
    VeniceControllerWrapper controllerWrapper = ServiceFactory.getVeniceController(clusterName, kafkaBrokerWrapper);
    VeniceServerWrapper serverWrapper = null;
    try {
      serverWrapper = ServiceFactory.getVeniceServer(clusterName, kafkaBrokerWrapper, true, true);
      Assert.assertTrue(serverWrapper.getVeniceServer().isStarted());
    } finally {
      if (serverWrapper != null) {
        serverWrapper.close();
      }
      controllerWrapper.close();
      kafkaBrokerWrapper.close();
    }
  }
}
