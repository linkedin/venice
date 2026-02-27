package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.ROUTER_CLIENT_DECOMPRESSION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class TestDictionaryRetrievalServiceMetrics {
  private static final int TEST_TIMEOUT = 120 * Time.MS_PER_SECOND;
  private VeniceClusterWrapper veniceCluster;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(0)
        .numberOfRouters(0)
        .replicationFactor(2)
        .build();
    veniceCluster = ServiceFactory.getVeniceCluster(options);

    Properties serverProperties = new Properties();
    serverProperties.put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB);
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    veniceCluster.addVeniceServer(new Properties(), serverProperties);
    veniceCluster.addVeniceServer(new Properties(), serverProperties);

    Properties routerProperties = new Properties();
    routerProperties.put(ROUTER_CLIENT_DECOMPRESSION_ENABLED, "true");
    veniceCluster.addVeniceRouter(routerProperties);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testDictionaryRetrievalMetricsAfterPush() throws Exception {
    // Create a store with ZSTD_WITH_DICT compression, which triggers dictionary download on the router
    veniceCluster.createStoreWithZstdDictionary(100);

    VeniceRouterWrapper routerWrapper = veniceCluster.getRandomVeniceRouter();
    MetricsRepository routerMetrics = routerWrapper.getMetricsRepository();

    // The metric name format follows Venice convention: ".{stats_name}--{metric_name}.{stat_type}"
    // Stats are registered with name "dictionary_retrieval_service" in DictionaryRetrievalService
    String metricPrefix = ".dictionary_retrieval_service--";

    // Wait for dictionary download to complete and metrics to be populated
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      // Verify download success counter is > 0 (at least one dictionary was downloaded)
      double downloadSuccess = routerMetrics.getMetric(metricPrefix + "dictionary_download_success.Count").value();
      Assert.assertTrue(downloadSuccess > 0, "Expected dictionary_download_success > 0, got " + downloadSuccess);

      // Verify consumer poll counter is > 0 (consumer thread processed at least one item)
      double consumerPoll = routerMetrics.getMetric(metricPrefix + "dictionary_consumer_poll.Count").value();
      Assert.assertTrue(consumerPoll > 0, "Expected dictionary_consumer_poll > 0, got " + consumerPoll);
    });

    // Verify the retriever thread is alive (gauge should be 1)
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      double threadAlive = routerMetrics.getMetric(metricPrefix + "dictionary_retriever_thread_alive.Gauge").value();
      Assert.assertEquals(threadAlive, 1.0, "Expected dictionary_retriever_thread_alive to be 1");
    });

    // Verify the downloading futures count gauge is >= 0 (futures may have already completed)
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      double futuresCount =
          routerMetrics.getMetric(metricPrefix + "dictionary_downloading_futures_count.Gauge").value();
      Assert.assertTrue(futuresCount >= 0, "Expected dictionary_downloading_futures_count >= 0, got " + futuresCount);
    });

    // Verify version queued counter > 0
    double versionQueued = routerMetrics.getMetric(metricPrefix + "dictionary_version_queued.Count").value();
    Assert.assertTrue(versionQueued > 0, "Expected dictionary_version_queued > 0, got " + versionQueued);
  }
}
