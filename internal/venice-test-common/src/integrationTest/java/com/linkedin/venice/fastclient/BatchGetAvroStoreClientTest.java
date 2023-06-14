package com.linkedin.venice.fastclient;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.streaming.VeniceResponseMap;
import com.linkedin.venice.fastclient.meta.StoreMetadataFetchMode;
import com.linkedin.venice.fastclient.schema.TestValueSchema;
import com.linkedin.venice.fastclient.stats.FastClientStats;
import com.linkedin.venice.fastclient.utils.AbstractClientEndToEndSetup;
import com.linkedin.venice.read.RequestType;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.Test;


/**
 * TODO :
 * Metrics . Test if metrics are what we expect
 * Error handling. How can we simulate synchronization issues
 * Da Vinci metadata. Do we need to test with that? What is the use case when we use da vinci metadata
 */
public class BatchGetAvroStoreClientTest extends AbstractClientEndToEndSetup {
  // Every test will print all stats if set to true. Should only be used locally
  private static boolean PRINT_STATS = false;
  private static final Logger LOGGER = LogManager.getLogger(BatchGetAvroStoreClientTest.class);
  private static final long TIME_OUT_IN_SECONDS = 60;

  private void printAllStats() {
    /* The print_stats flag controls if all stats are printed. Usually it will be too much info but while running
     * locally this can help understand what metrics are used */
    if (!PRINT_STATS || clientConfig == null)
      return;
    // Prints all available stats. Useful for debugging
    Map<String, List<Metric>> metricsSan = new HashMap<>();
    FastClientStats stats = clientConfig.getStats(RequestType.MULTI_GET);
    MetricsRepository metricsRepository = stats.getMetricsRepository();
    Map<String, ? extends Metric> metrics = metricsRepository.metrics();
    for (Map.Entry<String, ? extends Metric> metricName: metrics.entrySet()) {
      String metricNameSan = StringUtils.substringBetween(metricName.getKey(), "--", ".");
      if (StringUtils.startsWith(metricNameSan, "multiget_")) {
        metricsSan.computeIfAbsent(metricNameSan, (k) -> new ArrayList<>()).add(metricName.getValue());
      }
    }
    List<String> valMetrics = new ArrayList<>();
    List<String> noValMetrics = new ArrayList<>();
    for (Map.Entry<String, List<Metric>> metricNameSan: metricsSan.entrySet()) {
      boolean hasVal = false;
      for (Metric metric: metricNameSan.getValue()) {
        if (metric != null) {
          double value = metric.value();
          if (Double.isFinite(value) && value != 0f) {
            hasVal = true;
          }
        }
      }
      if (!hasVal) {
        noValMetrics.add(
            metricNameSan + ":"
                + metricsSan.get(metricNameSan.getKey())
                    .stream()
                    .map((m) -> StringUtils.substringAfterLast(m.name(), "."))
                    .collect(Collectors.joining(",")));
      } else {
        valMetrics.add(
            metricNameSan + ":"
                + metricsSan.get(metricNameSan.getKey())
                    .stream()
                    .map((m) -> StringUtils.substringAfterLast(m.name(), ".") + "=" + m.value())
                    .collect(Collectors.joining(",")));
      }
    }
    valMetrics.sort(Comparator.naturalOrder());
    noValMetrics.sort(Comparator.naturalOrder());
    LOGGER.info("STATS: Metrics with values -> \n    {}", String.join("\n    ", valMetrics));
    LOGGER.info("STATS: Metrics with noValues -> \n    {}", String.join("\n    ", noValMetrics));
  }

  /* Interpretation of available stats when printstats is enabled
  * multiget_request:OccurrenceRate=0.03329892444474044 -> no. of total requests per second
  * multiget_healthy_request:OccurrenceRate=0.03331556503198294 -> No. of healthy request per second. This is over a
    30 second window so the numbers won't make sense for a unit test
  * multiget_healthy_request_latency:99_9thPercentile=895.5951595159517,90thPercentile=895.5951595159517,
    77thPercentile=895.5951595159517,Avg=895.929393,99thPercentile=895.5951595159517,
    95thPercentile=895.5951595159517,50thPercentile=895.5951595159517
    -> latency %iles for the whole request. This measures time until onCompletion is called
  * multiget_request_key_count:Max=101.0,Rate=3.2674452460289216,Avg=101.0 -> no. of keys processed and rate per sec
  * multiget_success_request_key_count:Max=100.0,Rate=3.3315565031982945,Avg=100.0
      -> same as above but for successful requests.
  * multiget_success_request_key_ratio:SimpleRatioStat=1.0196212185184406 (number of success keys / no. of total keys)
  * multiget_success_request_ratio:SimpleRatioStat=1.0 -> no. of successful / total requests
  */

  /**
   * Creates a batchget request which uses scatter gather to fetch all keys from different replicas.
   */
  @Test(dataProvider = "FastClient-One-Boolean-Store-Metadata-Fetch-Mode", timeOut = TIME_OUT)
  public void testBatchGetGenericClient(
      boolean useStreamingBatchGetAsDefault,
      StoreMetadataFetchMode storeMetadataFetchMode) throws Exception {
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setSpeculativeQueryEnabled(true)
            .setDualReadEnabled(false)
            .setMaxAllowedKeyCntInBatchGetReq(recordCnt + 1) // +1 for nonExistingKey
            // TODO: this needs to be revisited to see how much this should be set. Current default is 50.
            .setRoutingPendingRequestCounterInstanceBlockThreshold(recordCnt + 1)
            .setUseStreamingBatchGetAsDefault(useStreamingBatchGetAsDefault);

    MetricsRepository metricsRepository = new MetricsRepository();
    AvroGenericStoreClient<String, GenericRecord> genericFastClient =
        getGenericFastClient(clientConfigBuilder, metricsRepository, storeMetadataFetchMode);

    Set<String> keys = new HashSet<>();
    for (int i = 0; i < recordCnt; ++i) {
      keys.add(keyPrefix + i);
    }
    keys.add("nonExistingKey");
    Map<String, GenericRecord> results = genericFastClient.batchGet(keys).get();
    // Assertions
    for (int i = 0; i < recordCnt; ++i) {
      String key = keyPrefix + i;
      GenericRecord value = results.get(key);
      assertEquals(value.get(VALUE_FIELD_NAME), i);
    }

    validateMetrics(metricsRepository, useStreamingBatchGetAsDefault, recordCnt + 1, recordCnt);

    FastClientStats stats = clientConfig.getStats(RequestType.MULTI_GET);
    LOGGER.info("STATS: {}", stats.buildSensorStatSummary("multiget_healthy_request_latency"));
    printAllStats();
  }

  @Test(dataProvider = "FastClient-One-Boolean-Store-Metadata-Fetch-Mode", timeOut = TIME_OUT)
  public void testBatchGetSpecificClient(
      boolean useStreamingBatchGetAsDefault,
      StoreMetadataFetchMode storeMetadataFetchMode) throws Exception {
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setSpeculativeQueryEnabled(true)
            .setDualReadEnabled(false)
            .setMaxAllowedKeyCntInBatchGetReq(recordCnt)
            // TODO: this needs to be revisited to see how much this should be set. Current default is 50.
            .setRoutingPendingRequestCounterInstanceBlockThreshold(recordCnt)
            .setUseStreamingBatchGetAsDefault(useStreamingBatchGetAsDefault);

    MetricsRepository metricsRepository = new MetricsRepository();
    AvroSpecificStoreClient<String, TestValueSchema> specificFastClient =
        getSpecificFastClient(clientConfigBuilder, metricsRepository, TestValueSchema.class, storeMetadataFetchMode);

    Set<String> keys = new HashSet<>();
    for (int i = 0; i < recordCnt; ++i) {
      keys.add(keyPrefix + i);
    }
    Map<String, TestValueSchema> results = specificFastClient.batchGet(keys).get();
    // Assertions
    for (int i = 0; i < recordCnt; ++i) {
      String key = keyPrefix + i;
      GenericRecord value = results.get(key);
      assertEquals(value.get(VALUE_FIELD_NAME), i);
    }

    validateMetrics(metricsRepository, useStreamingBatchGetAsDefault, recordCnt, recordCnt);

    specificFastClient.close();
    printAllStats();
  }

  @Test(dataProvider = "StoreMetadataFetchModes", timeOut = TIME_OUT)
  public void testStreamingBatchGetGenericClient(StoreMetadataFetchMode storeMetadataFetchMode) throws Exception {
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setSpeculativeQueryEnabled(true)
            .setDualReadEnabled(false);

    MetricsRepository metricsRepository = new MetricsRepository();
    AvroGenericStoreClient<String, GenericRecord> genericFastClient =
        getGenericFastClient(clientConfigBuilder, metricsRepository, storeMetadataFetchMode);

    Set<String> keys = new HashSet<>();
    for (int i = 0; i < recordCnt; ++i) {
      keys.add(keyPrefix + i);
    }
    keys.add("nonExisting");

    VeniceResponseMap<String, GenericRecord> veniceResponseMap =
        genericFastClient.streamingBatchGet(keys).get(5, TimeUnit.SECONDS);
    assertEquals(veniceResponseMap.getNonExistingKeys().size(), 1);
    assertTrue(veniceResponseMap.isFullResponse());
    assertEquals(veniceResponseMap.getTotalEntryCount(), recordCnt + 1); // 1 for nonExisting key

    for (int i = 0; i < recordCnt; ++i) {
      String key = keyPrefix + i;
      GenericRecord value = veniceResponseMap.get(key);
      assertNotNull(value, "Expected non null value but got  null for key " + key);
      assertEquals(
          value.get(VALUE_FIELD_NAME),
          i,
          "Expected value " + i + " for key " + key + " but got " + value.get(VALUE_FIELD_NAME));
    }
    assertEquals(
        veniceResponseMap.size(),
        recordCnt,
        "Incorrect record count . Expected " + recordCnt + " actual " + veniceResponseMap.size());
    assertFalse(
        veniceResponseMap.containsKey("nonExisting"),
        " Results contained nonExisting key with value " + veniceResponseMap.get("nonExisting"));
    assertNotNull(veniceResponseMap.getNonExistingKeys(), " Expected non existing keys to be not null");
    assertEquals(
        veniceResponseMap.getNonExistingKeys().size(),
        1,
        "Incorrect non existing key size . Expected  1 got " + veniceResponseMap.getNonExistingKeys().size());

    validateMetrics(metricsRepository, true, recordCnt + 1, recordCnt);
  }

  @Test(dataProvider = "StoreMetadataFetchModes", timeOut = TIME_OUT)
  public void testStreamingBatchGetWithCallbackGenericClient(StoreMetadataFetchMode storeMetadataFetchMode)
      throws Exception {
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setSpeculativeQueryEnabled(true)
            .setDualReadEnabled(false);

    MetricsRepository metricsRepository = new MetricsRepository();
    AvroGenericStoreClient<String, GenericRecord> genericFastClient =
        getGenericFastClient(clientConfigBuilder, metricsRepository, storeMetadataFetchMode);
    Set<String> keys = new HashSet<>();
    for (int i = 0; i < recordCnt; ++i) {
      keys.add(keyPrefix + i);
    }
    keys.add("nonExisting");
    Map<String, GenericRecord> results = new ConcurrentHashMap<>();
    AtomicBoolean isComplete = new AtomicBoolean();
    AtomicBoolean isDuplicate = new AtomicBoolean();
    CountDownLatch completeLatch = new CountDownLatch(1);
    genericFastClient.streamingBatchGet(keys, new StreamingCallback<String, GenericRecord>() {
      @Override
      public void onRecordReceived(String key, GenericRecord value) {
        LOGGER.info("Record received {}:{}", key, value);
        if ("nonExisting".equals(key)) {
          assertNull(value);
        } else {
          if (results.containsKey(key)) {
            isDuplicate.set(true);
          } else {
            results.put(key, value);
          }
        }
      }

      @Override
      public void onCompletion(Optional<Exception> exception) {
        LOGGER.info("Exception received {}", exception);
        assertEquals(exception, Optional.empty());
        isComplete.set(true);
        completeLatch.countDown();
      }
    });

    // Wait until isComplete is true or timeout
    if (!completeLatch.await(TIME_OUT_IN_SECONDS, TimeUnit.SECONDS)) {
      fail("Test did not complete within timeout");
    }

    assertFalse(isDuplicate.get(), "Duplicate records received");
    for (int i = 0; i < recordCnt; ++i) {
      String key = keyPrefix + i;
      GenericRecord value = results.get(key);
      assertNotNull(value, "Expected non null value but got null for key " + key);
      assertEquals(
          value.get(VALUE_FIELD_NAME),
          i,
          "Expected value " + i + " for key " + key + " but got " + value.get(VALUE_FIELD_NAME));
    }
    assertEquals(
        results.size(),
        recordCnt,
        "Incorrect record count . Expected " + recordCnt + " actual " + results.size());
    assertFalse(
        results.containsKey("nonExisting"),
        " Results contained nonExisting key with value " + results.get("nonExisting"));
    FastClientStats stats = clientConfig.getStats(RequestType.MULTI_GET);

    LOGGER.info(
        "STATS: latency -> {}",
        stats.buildSensorStatSummary("multiget_healthy_request_latency", "99thPercentile"));

    validateMetrics(metricsRepository, true, recordCnt + 1, recordCnt);
    printAllStats();
  }
}
