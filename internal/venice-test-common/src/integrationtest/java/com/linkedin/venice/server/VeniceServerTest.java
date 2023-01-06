package com.linkedin.venice.server;

import static com.linkedin.venice.integration.utils.VeniceServerWrapper.SERVER_ENABLE_SERVER_ALLOW_LIST;
import static com.linkedin.venice.integration.utils.VeniceServerWrapper.SERVER_IS_AUTO_JOIN;

import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.TestVeniceServer;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.lang.reflect.Field;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.Test;


public class VeniceServerTest {
  @Test
  public void testStartServerWithDefaultConfigForTests() throws NoSuchFieldException, IllegalAccessException {
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(1, 1, 0)) {
      TestVeniceServer server = cluster.getVeniceServers().get(0).getVeniceServer();
      Assert.assertTrue(server.isStarted());

      Field liveClusterConfigRepoField = server.getClass().getSuperclass().getDeclaredField("liveClusterConfigRepo");
      liveClusterConfigRepoField.setAccessible(true);
      Assert.assertNotNull(liveClusterConfigRepoField.get(server));
    }
  }

  @Test
  public void testStartServerWhenEnableAllowlistCheckingFailed() {
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(1, 0, 0)) {
      Assert.assertThrows(VeniceException.class, () -> {
        Properties featureProperties = new Properties();
        featureProperties.setProperty(SERVER_ENABLE_SERVER_ALLOW_LIST, Boolean.toString(true));
        featureProperties.setProperty(SERVER_IS_AUTO_JOIN, Boolean.toString(false));
        cluster.addVeniceServer(featureProperties, new Properties());
      });
      Assert.assertTrue(cluster.getVeniceServers().isEmpty());
    }
  }

  @Test
  public void testStartServerWhenEnableAllowlistCheckingSuccessful() {
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(1, 0, 0)) {
      Properties featureProperties = new Properties();
      featureProperties.setProperty(SERVER_ENABLE_SERVER_ALLOW_LIST, Boolean.toString(true));
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
      Assert
          .assertTrue(repository.getAllLocalStorageEngines().isEmpty(), "New node should not have any storage engine.");

      // Create a storage engine.
      String storeName = Utils.getUniqueString("testCheckBeforeJoinCluster");
      server.getVeniceServer()
          .getStorageService()
          .openStoreForNewPartition(
              server.getVeniceServer().getConfigLoader().getStoreConfig(storeName),
              1,
              () -> null);
      Assert.assertEquals(
          repository.getAllLocalStorageEngines().size(),
          1,
          "We have created one storage engine for store: " + storeName);

      // Restart server, as server's info leave in Helix cluster, so we expect that all local storage would NOT be
      // deleted
      // once the server join again.
      cluster.stopVeniceServer(server.getPort());
      cluster.restartVeniceServer(server.getPort());
      repository = server.getVeniceServer().getStorageService().getStorageEngineRepository();
      Assert.assertEquals(repository.getAllLocalStorageEngines().size(), 1, "We should not cleanup the local storage");

      // Stop server, remove it from the cluster then restart. We expect that all local storage would be deleted. Once
      // the server join again.
      cluster.stopVeniceServer(server.getPort());
      try (ControllerClient client = ControllerClient
          .constructClusterControllerClient(cluster.getClusterName(), cluster.getAllControllersURLs())) {
        client.removeNodeFromCluster(Utils.getHelixNodeIdentifier(server.getPort()));
      }

      cluster.restartVeniceServer(server.getPort());
      Assert.assertTrue(
          server.getVeniceServer()
              .getStorageService()
              .getStorageEngineRepository()
              .getAllLocalStorageEngines()
              .isEmpty(),
          "After removing the node from cluster, local storage should be cleaned up once the server join the cluster again.");
    }
  }

  @Test
  public void testCheckBeforeJointClusterBeforeHelixInitializingCluster() throws Exception {
    Thread serverAddingThread = null;
    try (VeniceClusterWrapper cluster = ServiceFactory.getVeniceCluster(0, 0, 0)) {
      serverAddingThread = new Thread(() -> {
        Properties featureProperties = new Properties();
        featureProperties.setProperty(SERVER_ENABLE_SERVER_ALLOW_LIST, Boolean.toString(false));
        featureProperties.setProperty(SERVER_IS_AUTO_JOIN, Boolean.toString(true));
        cluster.addVeniceServer(featureProperties, new Properties());
      });
      serverAddingThread.start();

      Utils.sleep(Time.MS_PER_SECOND);
      Assert.assertTrue(
          cluster.getVeniceServers().isEmpty() || !cluster.getVeniceServers().get(0).getVeniceServer().isStarted());
      cluster.addVeniceController(new Properties());

      serverAddingThread.join(30 * Time.MS_PER_SECOND);
      Assert.assertTrue(!serverAddingThread.isAlive(), "Server should be added by now");
      Assert.assertTrue(cluster.getVeniceServers().get(0).getVeniceServer().isStarted());
    } finally {
      TestUtils.shutdownThread(serverAddingThread);
    }
  }
}
