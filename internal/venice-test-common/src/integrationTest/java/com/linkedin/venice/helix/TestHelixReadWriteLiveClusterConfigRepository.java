package com.linkedin.venice.helix;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.LiveClusterConfig;
import com.linkedin.venice.utils.TestUtils;
import java.util.concurrent.TimeUnit;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestHelixReadWriteLiveClusterConfigRepository {
  private ZkClient zkClient;
  private String cluster = "test-metadata-cluster";
  private String clusterPath = "/test-metadata-cluster";
  private String clusterConfigPath = "/ClusterConfig";
  private ZkServerWrapper zkServerWrapper;
  private HelixAdapterSerializer adapter = new HelixAdapterSerializer();

  private static final String CONFIGURED_REGION = "ConfiguredRegion";
  private static final String NON_CONFIGURED_REGION = "NonConfiguredRegion";

  HelixReadWriteLiveClusterConfigRepository liveClusterConfigRWRepo;

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

    liveClusterConfigRWRepo = new HelixReadWriteLiveClusterConfigRepository(zkClient, adapter, cluster);
    liveClusterConfigRWRepo.refresh();
  }

  @AfterMethod
  public void teardownTest() {
    zkClient.deleteRecursively(clusterPath);
    zkClient.close();
  }

  @Test
  public void testLiveClusterConfigGetsUpdated() {
    LiveClusterConfig liveClusterConfig = new LiveClusterConfig();
    liveClusterConfig.setServerKafkaFetchQuotaRecordsPerSecondForRegion(CONFIGURED_REGION, 100);

    liveClusterConfigRWRepo.updateConfigs(liveClusterConfig);
    TestUtils.waitForNonDeterministicCompletion(3, TimeUnit.SECONDS, () -> {
      Object zkData = zkClient.readData(clusterPath + clusterConfigPath);
      if (zkData == null || !(zkData instanceof LiveClusterConfig)) {
        return false;
      }

      LiveClusterConfig clusterConfig = (LiveClusterConfig) zkData;
      return clusterConfig.getServerKafkaFetchQuotaRecordsPerSecondForRegion(CONFIGURED_REGION) == 100
          && clusterConfig.getServerKafkaFetchQuotaRecordsPerSecondForRegion(
              NON_CONFIGURED_REGION) == LiveClusterConfig.DEFAULT_SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND;
    });
  }

  @Test
  public void testReadWriteLiveClusterConfigRepoCanReadAndWrite() {
    // Create a HelixReadWriteLiveClusterConfigRepository for reading from Zk
    HelixReadWriteLiveClusterConfigRepository liveClusterConfigRWRepo2 =
        new HelixReadWriteLiveClusterConfigRepository(zkClient, adapter, cluster);
    liveClusterConfigRWRepo2.refresh();

    LiveClusterConfig liveClusterConfig = new LiveClusterConfig();
    liveClusterConfig.setServerKafkaFetchQuotaRecordsPerSecondForRegion(CONFIGURED_REGION, 100);

    // Update live cluster configs in Zk
    liveClusterConfigRWRepo.updateConfigs(liveClusterConfig);

    // Verify ReadWriteLiveClusterConfigRepository can read the updated configs
    TestUtils.waitForNonDeterministicCompletion(
        3,
        TimeUnit.SECONDS,
        () -> liveClusterConfigRWRepo2.getConfigs() != null
            && liveClusterConfigRWRepo2.getConfigs()
                .getServerKafkaFetchQuotaRecordsPerSecondForRegion(CONFIGURED_REGION) == 100
            && liveClusterConfigRWRepo2.getConfigs()
                .getServerKafkaFetchQuotaRecordsPerSecondForRegion(
                    NON_CONFIGURED_REGION) == LiveClusterConfig.DEFAULT_SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND);
  }

  @Test
  public void testLiveClusterConfigCanDeleteZNode() {
    LiveClusterConfig liveClusterConfig = new LiveClusterConfig();
    liveClusterConfig.setServerKafkaFetchQuotaRecordsPerSecondForRegion(CONFIGURED_REGION, 100);

    // Update live cluster configs in Zk
    liveClusterConfigRWRepo.updateConfigs(liveClusterConfig);

    // Verify that the config got updated in Zk
    TestUtils.waitForNonDeterministicCompletion(3, TimeUnit.SECONDS, () -> {
      Object zkData = zkClient.readData(clusterPath + clusterConfigPath);
      if (zkData == null || !(zkData instanceof LiveClusterConfig)) {
        return false;
      }

      LiveClusterConfig clusterConfig = (LiveClusterConfig) zkData;
      return clusterConfig.getServerKafkaFetchQuotaRecordsPerSecondForRegion(CONFIGURED_REGION) == 100
          && clusterConfig.getServerKafkaFetchQuotaRecordsPerSecondForRegion(
              NON_CONFIGURED_REGION) == LiveClusterConfig.DEFAULT_SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND;
    });

    // Trigger a deletion of all configs
    liveClusterConfigRWRepo.deleteConfigs();

    // Verify that the ZNode gets deleted
    TestUtils.waitForNonDeterministicCompletion(
        3,
        TimeUnit.SECONDS,
        () -> !zkClient.exists(clusterPath + clusterConfigPath));
  }
}
