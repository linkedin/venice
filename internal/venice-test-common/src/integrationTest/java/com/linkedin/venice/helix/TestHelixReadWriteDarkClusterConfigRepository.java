package com.linkedin.venice.helix;

import static com.linkedin.venice.zk.VeniceZkPaths.DARK_CLUSTER_CONFIG;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.DarkClusterConfig;
import com.linkedin.venice.utils.TestUtils;
import java.util.concurrent.TimeUnit;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestHelixReadWriteDarkClusterConfigRepository {
  private ZkClient zkClient;
  private String cluster = "test-cluster";
  private String clusterPath = "/test-cluster";
  private String clusterConfigPath = "/" + DARK_CLUSTER_CONFIG;
  private ZkServerWrapper zkServerWrapper;
  private HelixAdapterSerializer adapter = new HelixAdapterSerializer();

  HelixReadWriteDarkClusterConfigRepository darkClusterConfigRWRepo;

  @BeforeClass
  public void zkSetup() {
    zkServerWrapper = ServiceFactory.getZkServer();
  }

  @AfterClass
  public void zkCleanup() {
    zkServerWrapper.close();
  }

  @BeforeMethod
  public void configureTest() {
    String zkAddress = zkServerWrapper.getAddress();
    zkClient = ZkClientFactory.newZkClient(zkAddress);
    zkClient.setZkSerializer(adapter);

    darkClusterConfigRWRepo = new HelixReadWriteDarkClusterConfigRepository(zkClient, adapter, cluster);
    darkClusterConfigRWRepo.refresh();
  }

  @AfterMethod
  public void teardownTest() {
    zkClient.deleteRecursively(clusterPath);
    zkClient.close();
  }

  @Test
  public void testDarkClusterConfigGetsUpdated() {
    DarkClusterConfig darkClusterConfig = new DarkClusterConfig();
    darkClusterConfig.setStoresToReplicate(java.util.Arrays.asList("store1", "store2"));

    darkClusterConfigRWRepo.updateConfigs(darkClusterConfig);

    TestUtils.waitForNonDeterministicCompletion(3, TimeUnit.SECONDS, () -> {
      Object zkData = zkClient.readData(clusterPath + clusterConfigPath);
      if (zkData == null || !(zkData instanceof DarkClusterConfig)) {
        return false;
      }

      DarkClusterConfig clusterConfig = (DarkClusterConfig) zkData;
      return clusterConfig.getStoresToReplicate().equals(darkClusterConfig.getStoresToReplicate());
    });
  }

  @Test
  public void testReadWriteDarkClusterConfigRepoCanReadAndWrite() {
    HelixReadWriteDarkClusterConfigRepository localDarkClusterConfigRWRepo =
        new HelixReadWriteDarkClusterConfigRepository(zkClient, adapter, cluster);
    localDarkClusterConfigRWRepo.refresh();

    DarkClusterConfig darkClusterConfig = new DarkClusterConfig();
    darkClusterConfig.setStoresToReplicate(java.util.Arrays.asList("storeA", "storeB"));

    // Update dark cluster config
    darkClusterConfigRWRepo.updateConfigs(darkClusterConfig);

    // Verify the update is visible to the local repo
    TestUtils.waitForNonDeterministicCompletion(
        3,
        TimeUnit.SECONDS,
        () -> localDarkClusterConfigRWRepo.getConfigs() != null && localDarkClusterConfigRWRepo.getConfigs()
            .getStoresToReplicate()
            .equals(darkClusterConfig.getStoresToReplicate()));
  }

  @Test
  public void testDarkClusterConfigCanDeleteZNode() {
    DarkClusterConfig darkClusterConfig = new DarkClusterConfig();
    darkClusterConfig.setStoresToReplicate(java.util.Arrays.asList("store1", "store2"));

    darkClusterConfigRWRepo.updateConfigs(darkClusterConfig);

    TestUtils.waitForNonDeterministicCompletion(3, TimeUnit.SECONDS, () -> {
      Object zkData = zkClient.readData(clusterPath + clusterConfigPath);
      if (zkData == null || !(zkData instanceof DarkClusterConfig)) {
        return false;
      }

      DarkClusterConfig clusterConfig = (DarkClusterConfig) zkData;
      return clusterConfig.getStoresToReplicate().equals(darkClusterConfig.getStoresToReplicate());
    });

    darkClusterConfigRWRepo.deleteConfigs();

    TestUtils.waitForNonDeterministicCompletion(
        3,
        TimeUnit.SECONDS,
        () -> !zkClient.exists(clusterPath + clusterConfigPath));
  }
}
