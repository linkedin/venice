package com.linkedin.venice.server;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.utils.FlakyTestRetryAnalyzer;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.integration.utils.VeniceServerWrapper.*;


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
      //expected.
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

  @Test
  public void testPathCheck()
      throws IOException {

    // setup
    String tmpDirectory = System.getProperty("java.io.tmpdir");
    String directoryName = TestUtils.getUniqueString("testdir");
    String fileName = TestUtils.getUniqueString("testfile");
    Path dirPath = Paths.get(tmpDirectory, directoryName);
    Path filePath = Paths.get(tmpDirectory, fileName);
    Files.createDirectory(dirPath);
    Files.createFile(filePath);
    File dir = new File(tmpDirectory, directoryName).getAbsoluteFile();
    Assert.assertFalse(dir.isFile(), "must not be a file");
    Assert.assertTrue(dir.isDirectory(), "must be a directory");

    //test

    Assert.assertTrue(VeniceServer.directoryExists(dirPath.toString()),
        "directory: " + dirPath.toString() + " should exist");
    Assert.assertFalse(VeniceServer.directoryExists(filePath.toString()),
        "file: " + filePath.toString() + " should not count as a directory that exists");

    String pathDoesNotExist = Paths.get(tmpDirectory, TestUtils.getUniqueString("no-directory")).toString();
    Assert.assertFalse(VeniceServer.directoryExists(pathDoesNotExist), "Path that doesn't exist should not exist");
  }

  @Test(retryAnalyzer = FlakyTestRetryAnalyzer.class)
  public void testCheckBeforeJoinCluster()
      throws IOException {
    VeniceClusterWrapper clusterWrapper = ServiceFactory.getVeniceCluster();
    try {
      VeniceServerWrapper server = clusterWrapper.getVeniceServers().get(0);
      StoreRepository repository = server.getVeniceServer().getStorageService().getStoreRepository();
      Assert.assertEquals(repository.getAllLocalStorageEngines().size(), 0,
          "New node should not have any storage engine.");
      // Create a storage engine.
      String testStore = TestUtils.getUniqueString("testCheckBeforeJoinCluster");
      server.getVeniceServer()
          .getStorageService()
          .openStoreForNewPartition(server.getVeniceServer().getConfigLoader().getStoreConfig(testStore), 1);
      Assert.assertEquals(repository.getAllLocalStorageEngines().size(), 1,
          "We have created one storage engine for store: " + testStore);

      // Restart server, as server's info leave in Helix cluster, so we expect that all local storage would NOT be deleted
      // once the server join again.
      clusterWrapper.stopVeniceServer(server.getPort());
      clusterWrapper.restartVeniceServer(server.getPort());
      repository = server.getVeniceServer().getStorageService().getStoreRepository();
      Assert.assertEquals(repository.getAllLocalStorageEngines().size(), 1, "We should not cleanup the local storage");

      // Stop server, remove it from the cluster then restart. We expect that all local storage would be deleted. Once
      // the server join again.
      clusterWrapper.stopVeniceServer(server.getPort());
      clusterWrapper.getMasterVeniceController()
          .getVeniceAdmin()
          .removeStorageNode(clusterWrapper.getClusterName(), Utils.getHelixNodeIdentifier(server.getPort()));
      clusterWrapper.restartVeniceServer(server.getPort());

      repository = server.getVeniceServer().getStorageService().getStoreRepository();
      Assert.assertEquals(repository.getAllLocalStorageEngines().size(), 0,
          "After removing the node from cluster, local storage should be cleaned up once the server join the cluster again.");
    }finally {
      clusterWrapper.close();
    }
  }

  @Test
  public void testCheckBeforeJointClusterBeforeHelixInitializingCluster() {
    String clusterName = TestUtils.getUniqueString("testCheckBeforeJointClusterBeforeHelixInitializingCluster");
    KafkaBrokerWrapper kafka = ServiceFactory.getKafkaBroker();
    final VeniceServerWrapper[] servers = new VeniceServerWrapper[1];
    new Thread(new Runnable() {
      @Override
      public void run() {
        Properties featureProperties = new Properties();
        featureProperties.setProperty(SERVER_ENABLE_SERVER_WHITE_LIST, Boolean.toString(false));
        featureProperties.setProperty(SERVER_IS_AUTO_JOIN, Boolean.toString(true));
        featureProperties.setProperty(SERVER_ENABLE_SSL, Boolean.toString(false));
        featureProperties.setProperty(SERVER_SSL_TO_KAFKA, Boolean.toString(false));
        servers[0] = ServiceFactory.getVeniceServer(clusterName, kafka, featureProperties, new Properties());
      }
    }).start();
    Utils.sleep(1000);
    // Controller is started after server.
    ServiceFactory.getVeniceController(clusterName, kafka);
    TestUtils.waitForNonDeterministicCompletion(3000, TimeUnit.MILLISECONDS, () -> servers[0] != null);
    Assert.assertTrue(servers[0].isRunning(), "Server could start before controller correctly.");
  }
}