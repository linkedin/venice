package com.linkedin.venice.helix;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.LiveClusterConfig;
import com.linkedin.venice.utils.TestUtils;
import java.util.concurrent.TimeUnit;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestHelixReadOnlyLiveClusterConfigRepository {
  private ZkClient zkClient;
  private String cluster = "test-metadata-cluster";
  private String clusterPath = "/test-metadata-cluster";
  private String clusterConfigPath = "/ClusterConfig";
  private ZkServerWrapper zkServerWrapper;
  private HelixAdapterSerializer adapter = new HelixAdapterSerializer();

  private static final String CONFIGURED_REGION = "ConfiguredRegion";
  private static final String NON_CONFIGURED_REGION = "NonConfiguredRegion";

  HelixReadOnlyLiveClusterConfigRepository liveClusterConfigRORepo;

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
    zkClient.create(clusterPath, null, CreateMode.PERSISTENT);
    zkClient.create(clusterPath + clusterConfigPath, null, CreateMode.PERSISTENT);

    liveClusterConfigRORepo = new HelixReadOnlyLiveClusterConfigRepository(zkClient, adapter, cluster);
    liveClusterConfigRORepo.refresh();
  }

  @AfterMethod
  public void teardownTest() {
    zkClient.deleteRecursively(clusterPath);
    zkClient.close();
  }

  @Test
  public void testLiveClusterConfigGetsPropagated() {
    LiveClusterConfig liveClusterConfig = new LiveClusterConfig();
    liveClusterConfig.setServerKafkaFetchQuotaRecordsPerSecondForRegion(CONFIGURED_REGION, 100);

    // Serialize Live configs and store in Zk
    zkClient.writeData(clusterPath + clusterConfigPath, liveClusterConfig);

    // Verify the data gets read by in HelixReadOnlyLiveClusterConfigRepository
    TestUtils.waitForNonDeterministicCompletion(
        3,
        TimeUnit.SECONDS,
        () -> liveClusterConfigRORepo.getConfigs() != null
            && liveClusterConfigRORepo.getConfigs()
                .getServerKafkaFetchQuotaRecordsPerSecondForRegion(CONFIGURED_REGION) == 100
            && liveClusterConfigRORepo.getConfigs()
                .getServerKafkaFetchQuotaRecordsPerSecondForRegion(
                    NON_CONFIGURED_REGION) == LiveClusterConfig.DEFAULT_SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND);
  }

  @Test
  public void testDeletedZNodeReturnsDefaultLiveClusterConfig() {
    LiveClusterConfig liveClusterConfig = new LiveClusterConfig();
    liveClusterConfig.setServerKafkaFetchQuotaRecordsPerSecondForRegion(CONFIGURED_REGION, 100);

    // Serialize Live configs and store in Zk
    zkClient.writeData(clusterPath + clusterConfigPath, liveClusterConfig);

    // Verify the data got persisted in Zk
    TestUtils.waitForNonDeterministicCompletion(
        3,
        TimeUnit.SECONDS,
        () -> liveClusterConfigRORepo.getConfigs() != null
            && liveClusterConfigRORepo.getConfigs()
                .getServerKafkaFetchQuotaRecordsPerSecondForRegion(CONFIGURED_REGION) == 100
            && liveClusterConfigRORepo.getConfigs()
                .getServerKafkaFetchQuotaRecordsPerSecondForRegion(
                    NON_CONFIGURED_REGION) == LiveClusterConfig.DEFAULT_SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND);

    // Trigger a deletion of the live config ZNode
    zkClient.delete(clusterPath + clusterConfigPath);

    // Verify that the repository returns default values for the configs
    TestUtils.waitForNonDeterministicCompletion(
        3,
        TimeUnit.SECONDS,
        () -> liveClusterConfigRORepo.getConfigs() != null
            && liveClusterConfigRORepo.getConfigs()
                .getServerKafkaFetchQuotaRecordsPerSecondForRegion(
                    CONFIGURED_REGION) == LiveClusterConfig.DEFAULT_SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND
            && liveClusterConfigRORepo.getConfigs()
                .getServerKafkaFetchQuotaRecordsPerSecondForRegion(
                    NON_CONFIGURED_REGION) == LiveClusterConfig.DEFAULT_SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND);
  }

  @Test
  public void testLiveClusterConfigGetsPropagatedOnServiceStart() {
    LiveClusterConfig liveClusterConfig = new LiveClusterConfig();
    liveClusterConfig.setServerKafkaFetchQuotaRecordsPerSecondForRegion(CONFIGURED_REGION, 100);

    // Serialize Live configs and store in Zk
    zkClient.writeData(clusterPath + clusterConfigPath, liveClusterConfig);

    // Verify the data gets read by in HelixReadOnlyLiveClusterConfigRepository
    TestUtils.waitForNonDeterministicCompletion(
        3,
        TimeUnit.SECONDS,
        () -> liveClusterConfigRORepo.getConfigs() != null
            && liveClusterConfigRORepo.getConfigs()
                .getServerKafkaFetchQuotaRecordsPerSecondForRegion(CONFIGURED_REGION) == 100
            && liveClusterConfigRORepo.getConfigs()
                .getServerKafkaFetchQuotaRecordsPerSecondForRegion(
                    NON_CONFIGURED_REGION) == LiveClusterConfig.DEFAULT_SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND);

    // Create a HelixReadOnlyLiveClusterConfigRepository to simulate service start up
    HelixReadOnlyLiveClusterConfigRepository liveClusterConfigRORepo2 =
        new HelixReadOnlyLiveClusterConfigRepository(zkClient, adapter, cluster);
    liveClusterConfigRORepo2.refresh();

    // Verify the data gets pulled in on service start up
    TestUtils.waitForNonDeterministicCompletion(
        30,
        TimeUnit.SECONDS,
        () -> liveClusterConfigRORepo2.getConfigs() != null
            && liveClusterConfigRORepo2.getConfigs()
                .getServerKafkaFetchQuotaRecordsPerSecondForRegion(CONFIGURED_REGION) == 100
            && liveClusterConfigRORepo2.getConfigs()
                .getServerKafkaFetchQuotaRecordsPerSecondForRegion(
                    NON_CONFIGURED_REGION) == LiveClusterConfig.DEFAULT_SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND);
  }
}
