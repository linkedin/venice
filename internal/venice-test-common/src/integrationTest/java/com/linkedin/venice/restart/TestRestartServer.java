package com.linkedin.venice.restart;

import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;

import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.tehuti.Metric;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestRestartServer {
  private static final int REPLICATION_FACTOR = 2;
  private VeniceClusterWrapper cluster;

  @BeforeClass
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties clusterConfig = new Properties();
    clusterConfig.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfServers(REPLICATION_FACTOR)
        .replicationFactor(REPLICATION_FACTOR)
        .extraProperties(clusterConfig)
        .build();
    cluster = ServiceFactory.getVeniceCluster(options);
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(cluster);
  }

  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testRestartServerAfterPushCompleted() {
    int keyCount = REPLICATION_FACTOR * VeniceClusterWrapperConstants.DEFAULT_PARTITION_SIZE_BYTES;
    String storeName = cluster.createStore(keyCount);
    String kafkaTopic = Version.composeKafkaTopic(storeName, 1);
    String quotaUsedMetricName = String.format(".%s--storage_quota_used.Gauge", storeName);

    RoutingDataRepository routingDataRepository = cluster.getRandomVeniceRouter().getRoutingDataRepository();
    Assert.assertTrue(routingDataRepository.containsKafkaTopic(kafkaTopic));
    int partitionCount = routingDataRepository.getNumberOfPartitions(kafkaTopic);
    Assert.assertTrue(partitionCount > 1);

    cluster.updateStore(storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      for (VeniceServerWrapper server: cluster.getVeniceServers()) {
        Metric quotaUsedMetric = server.getMetricsRepository().getMetric(quotaUsedMetricName);
        Assert.assertNotNull(quotaUsedMetric);
        Assert.assertEquals(quotaUsedMetric.value(), -1., .0);
      }
    });

    cluster.updateStore(storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(1));
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      for (VeniceServerWrapper server: cluster.getVeniceServers()) {
        Metric quotaUsedMetric = server.getMetricsRepository().getMetric(quotaUsedMetricName);
        Assert.assertTrue(quotaUsedMetric.value() > keyCount);
      }
    });

    for (VeniceServerWrapper server: cluster.getVeniceServers()) {
      cluster.stopVeniceServer(server.getPort());
    }

    // Check no partitions are assigned after all the servers were shutdown.
    TestUtils.waitForNonDeterministicAssertion(
        20,
        TimeUnit.SECONDS,
        () -> Assert.assertFalse(routingDataRepository.containsKafkaTopic(kafkaTopic)));

    for (VeniceServerWrapper server: cluster.getVeniceServers()) {
      cluster.restartVeniceServer(server.getPort());
    }

    // After restart, all partitions should become ready to serve again.
    TestUtils.waitForNonDeterministicAssertion(20, TimeUnit.SECONDS, () -> {
      Assert.assertTrue(routingDataRepository.containsKafkaTopic(kafkaTopic));
      for (int i = 0; i < partitionCount; ++i) {
        Assert.assertEquals(routingDataRepository.getReadyToServeInstances(kafkaTopic, i).size(), REPLICATION_FACTOR);
      }
    });

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      for (VeniceServerWrapper server: cluster.getVeniceServers()) {
        Metric quotaUsedMetric = server.getMetricsRepository().getMetric(quotaUsedMetricName);
        Assert.assertTrue(quotaUsedMetric.value() > keyCount);
      }
    });
  }
}
