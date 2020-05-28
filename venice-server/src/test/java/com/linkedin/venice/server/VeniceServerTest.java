package com.linkedin.venice.server;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Properties;

import static com.linkedin.venice.integration.utils.VeniceServerWrapper.*;


public class VeniceServerTest {
  @Test
  public void testStartServerWhenDisableWhitelistChecking() {
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(1, 1, 0)) {
      Assert.assertTrue(cluster.getVeniceServers().get(0).getVeniceServer().isStarted());
    }
  }

  @Test
  public void testStartServerWhenEnableWhitelistCheckingFailed() {
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(1, 0, 0)) {
      Assert.assertThrows(VeniceException.class, () -> {
        Properties featureProperties = new Properties();
        featureProperties.setProperty(SERVER_ENABLE_SERVER_WHITE_LIST, Boolean.toString(true));
        featureProperties.setProperty(SERVER_IS_AUTO_JOIN, Boolean.toString(false));
        cluster.addVeniceServer(featureProperties, new Properties());
      });
      Assert.assertTrue(cluster.getVeniceServers().isEmpty());
    }
  }

  @Test
  public void testStartServerWhenEnableWhitelistCheckingSuccessful() {
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(1, 0, 0)) {
      Properties featureProperties = new Properties();
      featureProperties.setProperty(SERVER_ENABLE_SERVER_WHITE_LIST, Boolean.toString(true));
      featureProperties.setProperty(SERVER_IS_AUTO_JOIN, Boolean.toString(true));
      cluster.addVeniceServer(featureProperties, new Properties());
      Assert.assertTrue(cluster.getVeniceServers().get(0).getVeniceServer().isStarted());
    }
  }

  @Test
  public void testCheckBeforeJoinCluster() {
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(1, 1, 0)) {
      VeniceServerWrapper server = cluster.getVeniceServers().get(0);
      StorageEngineRepository repository = server.getVeniceServer().getStorageService().getStorageEngineRepository();
      Assert.assertTrue(repository.getAllLocalStorageEngines().isEmpty(), "New node should not have any storage engine.");

      // Create a storage engine.
      String storeName = TestUtils.getUniqueString("testCheckBeforeJoinCluster");
      server.getVeniceServer().getStorageService().openStoreForNewPartition(server.getVeniceServer().getConfigLoader().getStoreConfig(storeName), 1);
      Assert.assertEquals(repository.getAllLocalStorageEngines().size(), 1, "We have created one storage engine for store: " + storeName);

      // Restart server, as server's info leave in Helix cluster, so we expect that all local storage would NOT be deleted
      // once the server join again.
      cluster.stopVeniceServer(server.getPort());
      cluster.restartVeniceServer(server.getPort());
      repository = server.getVeniceServer().getStorageService().getStorageEngineRepository();
      Assert.assertEquals(repository.getAllLocalStorageEngines().size(), 1, "We should not cleanup the local storage");

      // Stop server, remove it from the cluster then restart. We expect that all local storage would be deleted. Once
      // the server join again.
      cluster.stopVeniceServer(server.getPort());
      try (ControllerClient client = new ControllerClient(cluster.getClusterName(), cluster.getAllControllersURLs())) {
        client.removeNodeFromCluster(Utils.getHelixNodeIdentifier(server.getPort()));
      }

      cluster.restartVeniceServer(server.getPort());
      Assert.assertTrue(server.getVeniceServer().getStorageService().getStorageEngineRepository().getAllLocalStorageEngines().isEmpty(),
          "After removing the node from cluster, local storage should be cleaned up once the server join the cluster again.");
    }
  }

  @Test
  public void testCheckBeforeJointClusterBeforeHelixInitializingCluster() {
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(0, 0, 0)) {
      Thread t = new Thread(() -> {
        Properties featureProperties = new Properties();
        featureProperties.setProperty(SERVER_ENABLE_SERVER_WHITE_LIST, Boolean.toString(false));
        featureProperties.setProperty(SERVER_IS_AUTO_JOIN, Boolean.toString(true));
        cluster.addVeniceServer(featureProperties, new Properties());
      });
      t.start();

      Utils.sleep(Time.MS_PER_SECOND);
      Assert.assertTrue(cluster.getVeniceServers().isEmpty() || !cluster.getVeniceServers().get(0).getVeniceServer().isStarted());
      cluster.addVeniceController(new Properties());

      t.join(30 * Time.MS_PER_SECOND);
      Assert.assertTrue(!t.isAlive(), "Server should be added by now");
      Assert.assertTrue(cluster.getVeniceServers().get(0).getVeniceServer().isStarted());

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      Assert.fail("Test is interrupted");
    }
  }
}
