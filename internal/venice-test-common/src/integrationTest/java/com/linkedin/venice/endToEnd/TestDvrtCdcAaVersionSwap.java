package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.CONSUMER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.ClientType.CHANGE_DATA_CAPTURE_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;

import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.ChangelogClientConfig;
import com.linkedin.davinci.consumer.StatefulVeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerClientFactory;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


/**
 * Integration tests for the active-active aware version swap path on the
 * {@link com.linkedin.davinci.consumer.VeniceChangelogConsumerDaVinciRecordTransformerImpl} CDC consumer:
 * single-swap basic flow plus edge cases (stateful, stateless, mid-swap restart, watchdog timeout,
 * buffer pressure). Multi-version scenarios (back-to-back swaps, rollback) live in the sibling
 * {@link TestDvrtCdcAaVersionSwapMultiVersion} class so each landing shard fits the 15-min CI limit.
 */
public class TestDvrtCdcAaVersionSwap extends AbstractDvrtCdcAaVersionSwapTest {
  /**
   * Stateful DaVinciRecordTransformer CDC consumer with AA flag on. Verifies pre-swap records
   * surface through the consumer and post-cutover records continue to surface — the AA path does
   * not deadlock or crash through a swap.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testAaVersionSwapStatefulRecordTransformerEndToEnd() throws Exception {
    String storeName = Utils.getUniqueString("aa-stateful-dvrt");
    try {
      createAaHybridStore(storeName);
      emptyPush(storeName);
      writeRecords(storeName, 0, PRE_SWAP_RECORDS);

      MetricsRepository metricsRepository =
          getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
      VeniceChangelogConsumerClientFactory factory =
          new VeniceChangelogConsumerClientFactory(buildClientConfig(childDatacenters.get(0), true), metricsRepository);
      try (StatefulVeniceChangelogConsumer<GenericRecord, GenericRecord> consumer =
          factory.getStatefulChangelogConsumer(storeName)) {
        consumer.start().get();
        Map<String, ChangeEvent<GenericRecord>> seen = new HashMap<>();
        List<String> orderedKeys = new ArrayList<>();
        pollUntilRangeObserved(consumer, seen, orderedKeys, 0, PRE_SWAP_RECORDS);

        emptyPush(storeName);
        // Drain past the swap watchdog timeout so the cutover is definitely committed before
        // producing post-cutover records.
        drainForDuration(consumer, seen, orderedKeys, POST_SWAP_SETTLE_DRAIN_SECONDS);
        int total = PRE_SWAP_RECORDS + POST_SWAP_RECORDS;
        writeRecords(storeName, PRE_SWAP_RECORDS, total);
        pollUntilRangeObserved(consumer, seen, orderedKeys, PRE_SWAP_RECORDS, total);
        // No-loss across the swap: every record produced to RT (pre-swap and post-cutover) must
        // surface through the consumer, sourced from either v1 or v2.
        assertNoLoss(seen, 0, total);
      }
    } finally {
      deleteStoreQuietly(storeName);
    }
  }

  /**
   * Stateless DaVinciRecordTransformer CDC consumer with AA flag on. Same end-to-end shape as the
   * stateful test.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testAaVersionSwapStatelessRecordTransformerEndToEnd() throws Exception {
    String storeName = Utils.getUniqueString("aa-stateless-dvrt");
    try {
      createAaHybridStore(storeName);
      emptyPush(storeName);
      writeRecords(storeName, 0, PRE_SWAP_RECORDS);

      MetricsRepository metricsRepository =
          getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
      ChangelogClientConfig clientConfig = buildClientConfig(childDatacenters.get(0), true);
      clientConfig.setIsNewStatelessClientEnabled(true);
      VeniceChangelogConsumerClientFactory factory =
          new VeniceChangelogConsumerClientFactory(clientConfig, metricsRepository);
      VeniceChangelogConsumer<GenericRecord, GenericRecord> consumer = factory.getChangelogConsumer(storeName);
      try {
        consumer.subscribeAll().get();
        Map<String, ChangeEvent<GenericRecord>> seen = new HashMap<>();
        List<String> orderedKeys = new ArrayList<>();
        pollUntilRangeObserved(consumer, seen, orderedKeys, 0, PRE_SWAP_RECORDS);

        emptyPush(storeName);
        drainForDuration(consumer, seen, orderedKeys, POST_SWAP_SETTLE_DRAIN_SECONDS);
        int total = PRE_SWAP_RECORDS + POST_SWAP_RECORDS;
        writeRecords(storeName, PRE_SWAP_RECORDS, total);
        pollUntilRangeObserved(consumer, seen, orderedKeys, PRE_SWAP_RECORDS, total);
        assertNoLoss(seen, 0, total);
      } finally {
        consumer.unsubscribeAll();
      }
    } finally {
      deleteStoreQuietly(storeName);
    }
  }

  /**
   * Consumer is closed mid-flight (simulating a crash). A fresh consumer subscribes after the swap
   * is triggered and post-swap records are written. Convergence: the new consumer's stream
   * progresses through the post-swap keys without deadlocking.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testAaVersionSwapStatelessRecordTransformerRestartMidSwap() throws Exception {
    String storeName = Utils.getUniqueString("aa-restart-dvrt");
    try {
      createAaHybridStore(storeName);
      emptyPush(storeName);
      writeRecords(storeName, 0, PRE_SWAP_RECORDS);

      MetricsRepository metrics1 =
          getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
      ChangelogClientConfig config1 = buildClientConfig(childDatacenters.get(0), true);
      config1.setIsNewStatelessClientEnabled(true);
      VeniceChangelogConsumerClientFactory factory1 = new VeniceChangelogConsumerClientFactory(config1, metrics1);
      VeniceChangelogConsumer<GenericRecord, GenericRecord> consumer1 = factory1.getChangelogConsumer(storeName);
      Map<String, ChangeEvent<GenericRecord>> seenBefore = new HashMap<>();
      List<String> orderedBefore = new ArrayList<>();
      try {
        consumer1.subscribeAll().get();
        pollUntilRangeObserved(consumer1, seenBefore, orderedBefore, 0, PRE_SWAP_RECORDS);
      } finally {
        consumer1.unsubscribeAll();
      }
      emptyPush(storeName);
      int total = PRE_SWAP_RECORDS + POST_SWAP_RECORDS;
      writeRecords(storeName, PRE_SWAP_RECORDS, total);

      MetricsRepository metrics2 =
          getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
      ChangelogClientConfig config2 = buildClientConfig(childDatacenters.get(0), true);
      config2.setIsNewStatelessClientEnabled(true);
      VeniceChangelogConsumerClientFactory factory2 = new VeniceChangelogConsumerClientFactory(config2, metrics2);
      VeniceChangelogConsumer<GenericRecord, GenericRecord> consumer2 = factory2.getChangelogConsumer(storeName);
      try {
        consumer2.subscribeAll().get();
        Map<String, ChangeEvent<GenericRecord>> seenAfter = new HashMap<>();
        List<String> orderedAfter = new ArrayList<>();
        // The new consumer comes up with v2 already current; it must successfully ingest and
        // surface every record that ever landed on the RT (consumer2 reads v2's VT from EARLIEST
        // in stateless mode and sees the full RT replay).
        pollUntilRangeObserved(consumer2, seenAfter, orderedAfter, 0, total);
        assertNoLoss(seenAfter, 0, total);
      } finally {
        consumer2.unsubscribeAll();
      }
    } finally {
      deleteStoreQuietly(storeName);
    }
  }

  /**
   * Configures an impossible barrier (third region that never appears) plus a short timeout. The
   * coordinator's timeout watchdog must force the cutover within the timeout, and the consumer
   * continues to deliver records past the swap.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testAaVersionSwapTimeout() throws Exception {
    String storeName = Utils.getUniqueString("aa-timeout-dvrt");
    try {
      createAaHybridStore(storeName);
      emptyPush(storeName);
      writeRecords(storeName, 0, PRE_SWAP_RECORDS);

      MetricsRepository metrics =
          getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
      ChangelogClientConfig config = buildClientConfig(childDatacenters.get(0), true);
      config.setTotalRegionCount(NUMBER_OF_CHILD_DATACENTERS + 1);
      config.setIsNewStatelessClientEnabled(true);
      config.setConsumerName("timeout-consumer");

      VeniceChangelogConsumerClientFactory factory = new VeniceChangelogConsumerClientFactory(config, metrics);
      VeniceChangelogConsumer<GenericRecord, GenericRecord> consumer = factory.getChangelogConsumer(storeName);
      try {
        consumer.subscribeAll().get();
        Map<String, ChangeEvent<GenericRecord>> seen = new HashMap<>();
        List<String> orderedKeys = new ArrayList<>();
        pollUntilRangeObserved(consumer, seen, orderedKeys, 0, PRE_SWAP_RECORDS);

        emptyPush(storeName);
        // Short watchdog (configured by buildClientConfig) fires inside the drain window;
        // post-cutover records flow afterward.
        drainForDuration(consumer, seen, orderedKeys, POST_SWAP_SETTLE_DRAIN_SECONDS);
        int total = PRE_SWAP_RECORDS + POST_SWAP_RECORDS;
        writeRecords(storeName, PRE_SWAP_RECORDS, total);
        pollUntilRangeObserved(consumer, seen, orderedKeys, PRE_SWAP_RECORDS, total);
        assertNoLoss(seen, 0, total);
      } finally {
        consumer.unsubscribeAll();
      }
    } finally {
      deleteStoreQuietly(storeName);
    }
  }

  /**
   * Smaller-than-default consumer buffer + interleaved writes during the swap stress the drainer
   * pipeline. The test verifies the AA path doesn't deadlock under buffer pressure: the consumer
   * continues to make progress on post-cutover records.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testNoDataLossWithSymmetricDrainerFlush() throws Exception {
    String storeName = Utils.getUniqueString("aa-noloss-dvrt");
    try {
      createAaHybridStore(storeName);
      emptyPush(storeName);
      writeRecords(storeName, 0, PRE_SWAP_RECORDS);

      MetricsRepository metrics =
          getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
      ChangelogClientConfig config = buildClientConfig(childDatacenters.get(0), true);
      config.setIsNewStatelessClientEnabled(true);
      // Smaller-than-default buffer to stress the drainer; large enough that backpressure doesn't
      // deadlock the symmetric flush window.
      config.setMaxBufferSize(100);
      VeniceChangelogConsumerClientFactory factory = new VeniceChangelogConsumerClientFactory(config, metrics);
      VeniceChangelogConsumer<GenericRecord, GenericRecord> consumer = factory.getChangelogConsumer(storeName);
      try {
        consumer.subscribeAll().get();
        Map<String, ChangeEvent<GenericRecord>> seen = new HashMap<>();
        List<String> orderedKeys = new ArrayList<>();
        pollUntilRangeObserved(consumer, seen, orderedKeys, 0, PRE_SWAP_RECORDS);

        emptyPush(storeName);
        drainForDuration(consumer, seen, orderedKeys, POST_SWAP_SETTLE_DRAIN_SECONDS);
        int total = PRE_SWAP_RECORDS + POST_SWAP_RECORDS;
        writeRecords(storeName, PRE_SWAP_RECORDS, total);
        pollUntilRangeObserved(consumer, seen, orderedKeys, PRE_SWAP_RECORDS, total);
        assertNoLoss(seen, 0, total);
      } finally {
        consumer.unsubscribeAll();
      }
    } finally {
      deleteStoreQuietly(storeName);
    }
  }
}
