package com.linkedin.venice.restart;

import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.DISK_QUOTA_USED;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.INGESTION_BYTES_CONSUMED;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.INGESTION_RECORDS_CONSUMED;
import static com.linkedin.venice.integration.utils.VeniceServerWrapper.SERVICE_METRIC_PREFIX;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateAnyGaugeDataPointAtLeast;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateAnySumDataPointAtLeast;

import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.Metric;
import java.util.Collection;
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
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfServers(REPLICATION_FACTOR)
        .replicationFactor(REPLICATION_FACTOR)
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

    // Validate OTel ingestion metrics are reported after push
    validateOtelIngestionMetricsAfterPush();

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

    // Validate OTel DISK_QUOTA_USED metric alongside Tehuti storage_quota_used
    validateOtelDiskQuotaUsed(keyCount);

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

  private static InMemoryMetricReader getOtelReader(VeniceServerWrapper server) {
    VeniceMetricsRepository metricsRepo = (VeniceMetricsRepository) server.getMetricsRepository();
    return (InMemoryMetricReader) metricsRepo.getVeniceMetricsConfig().getOtelAdditionalMetricsReader();
  }

  private void validateOtelIngestionMetricsAfterPush() {
    for (VeniceServerWrapper server: cluster.getVeniceServers()) {
      InMemoryMetricReader reader = getOtelReader(server);
      Assert.assertNotNull(reader, "InMemoryMetricReader should be registered for server");
      // Collect once to avoid draining async counter adders between validations
      Collection<MetricData> metrics = reader.collectAllMetrics();
      validateAnySumDataPointAtLeast(
          metrics,
          1,
          INGESTION_RECORDS_CONSUMED.getMetricEntity().getMetricName(),
          SERVICE_METRIC_PREFIX);
      validateAnySumDataPointAtLeast(
          metrics,
          1,
          INGESTION_BYTES_CONSUMED.getMetricEntity().getMetricName(),
          SERVICE_METRIC_PREFIX);
    }
  }

  private void validateOtelDiskQuotaUsed(int expectedMinValue) {
    for (VeniceServerWrapper server: cluster.getVeniceServers()) {
      InMemoryMetricReader reader = getOtelReader(server);
      validateAnyGaugeDataPointAtLeast(
          reader,
          expectedMinValue,
          DISK_QUOTA_USED.getMetricEntity().getMetricName(),
          SERVICE_METRIC_PREFIX);
    }
  }
}
