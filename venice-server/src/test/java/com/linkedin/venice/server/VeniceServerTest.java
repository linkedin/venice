package com.linkedin.venice.server;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.utils.TestUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
}
