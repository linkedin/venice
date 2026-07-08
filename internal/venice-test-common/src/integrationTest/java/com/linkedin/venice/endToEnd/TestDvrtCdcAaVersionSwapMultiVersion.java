package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.CONSUMER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.ClientType.CHANGE_DATA_CAPTURE_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static org.testng.Assert.assertEquals;

import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.ChangelogClientConfig;
import com.linkedin.davinci.consumer.VeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerClientFactory;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


/**
 * Multi-version scenarios for the active-active aware version swap path on the
 * {@link com.linkedin.davinci.consumer.VeniceChangelogConsumerDaVinciRecordTransformerImpl} CDC
 * consumer: back-to-back swaps and rollback. These are the heaviest tests in the suite and live
 * in their own class so they land in a dedicated CI shard. Single-swap basic + edge scenarios are
 * in the sibling {@link TestDvrtCdcAaVersionSwap} class.
 */
public class TestDvrtCdcAaVersionSwapMultiVersion extends AbstractDvrtCdcAaVersionSwapTest {
  /**
   * Two consecutive swaps (v1 → v2 → v3). After v2 commits, the coordinator must reset cleanly so
   * the v3 swap can arm via {@code armIfNeeded}. The {@code maxServedVersion} guard ensures stale
   * v2 VSMs don't trigger spurious flips.
   */
  @Test(timeOut = TEST_TIMEOUT * 2)
  public void testAaVersionSwapBackToBackSwaps() throws Exception {
    String storeName = Utils.getUniqueString("aa-backtoback-dvrt");
    try {
      createAaHybridStore(storeName);
      emptyPush(storeName);
      writeRecords(storeName, 0, PRE_SWAP_RECORDS);

      MetricsRepository metrics =
          getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
      ChangelogClientConfig config = buildClientConfig(childDatacenters.get(0), true);
      config.setIsNewStatelessClientEnabled(true);
      VeniceChangelogConsumerClientFactory factory = new VeniceChangelogConsumerClientFactory(config, metrics);
      VeniceChangelogConsumer<GenericRecord, GenericRecord> consumer = factory.getChangelogConsumer(storeName);
      try {
        consumer.subscribeAll().get();
        Map<String, ChangeEvent<GenericRecord>> seen = new HashMap<>();
        List<String> orderedKeys = new ArrayList<>();
        pollUntilRangeObserved(consumer, seen, orderedKeys, 0, PRE_SWAP_RECORDS);

        int afterV2 = PRE_SWAP_RECORDS + POST_SWAP_RECORDS;
        int afterV3 = afterV2 + POST_SWAP_RECORDS;

        emptyPush(storeName); // v2
        drainForDuration(consumer, seen, orderedKeys, POST_SWAP_SETTLE_DRAIN_SECONDS);
        writeRecords(storeName, PRE_SWAP_RECORDS, afterV2);
        pollUntilRangeObserved(consumer, seen, orderedKeys, PRE_SWAP_RECORDS, afterV2);

        emptyPush(storeName); // v3
        drainForDuration(consumer, seen, orderedKeys, POST_SWAP_SETTLE_DRAIN_SECONDS);
        writeRecords(storeName, afterV2, afterV3);
        pollUntilRangeObserved(consumer, seen, orderedKeys, afterV2, afterV3);
        assertNoLoss(seen, 0, afterV3);
      } finally {
        consumer.unsubscribeAll();
      }
    } finally {
      deleteStoreQuietly(storeName);
    }
  }

  /**
   * Real rollback: v1 → v2 swap → v3 swap → rollback to v2 → v4 swap. The {@code maxServedVersion}
   * guard must prevent stale VSMs (e.g., the v2→v3 ones replayed in v4's VT) from triggering a
   * downgrade.
   */
  @Test(timeOut = TEST_TIMEOUT * 2)
  public void testAaVersionSwapRollback() throws Exception {
    String storeName = Utils.getUniqueString("aa-rollback-dvrt");
    try {
      createAaHybridStore(storeName);
      emptyPush(storeName); // v1
      writeRecords(storeName, 0, PRE_SWAP_RECORDS);

      MetricsRepository metrics =
          getVeniceMetricsRepository(CHANGE_DATA_CAPTURE_CLIENT, CONSUMER_METRIC_ENTITIES, true);
      ChangelogClientConfig config = buildClientConfig(childDatacenters.get(0), true);
      config.setIsNewStatelessClientEnabled(true);
      VeniceChangelogConsumerClientFactory factory = new VeniceChangelogConsumerClientFactory(config, metrics);
      VeniceChangelogConsumer<GenericRecord, GenericRecord> consumer = factory.getChangelogConsumer(storeName);
      try {
        consumer.subscribeAll().get();
        Map<String, ChangeEvent<GenericRecord>> seen = new HashMap<>();
        List<String> orderedKeys = new ArrayList<>();
        pollUntilRangeObserved(consumer, seen, orderedKeys, 0, PRE_SWAP_RECORDS);

        int afterV2 = PRE_SWAP_RECORDS + POST_SWAP_RECORDS;

        emptyPush(storeName); // v2
        drainForDuration(consumer, seen, orderedKeys, POST_SWAP_SETTLE_DRAIN_SECONDS);
        writeRecords(storeName, PRE_SWAP_RECORDS, afterV2);
        pollUntilRangeObserved(consumer, seen, orderedKeys, PRE_SWAP_RECORDS, afterV2);

        emptyPush(storeName); // v3
        drainForDuration(consumer, seen, orderedKeys, POST_SWAP_SETTLE_DRAIN_SECONDS);
        // Intentionally do not produce records during v3's transient lifetime — v3 will be rolled
        // back below, and records produced here would only surface on v3's VT, which is torn down
        // by the rollback (records exist neither on v2's VT nor on v4's VT after rollback).

        assertCommand(parentControllerClient.rollbackToBackupVersion(storeName));
        // Assert per-colo currentVersion rather than parent.currentVersion: a bug in PR #2785
        // (VeniceParentHelixAdmin.updateParentVersionStatusAfterRollback) double-decrements parent's
        // currentVersion when 3+ versions exist — the admin-task handler decrements 3->2, then
        // updateParentVersionStatusAfterRollback decrements again to 1. Child controllers decrement
        // correctly to 2, so the colo map reflects the correct rolled-back-to state.
        TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
          for (int v: parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions().values()) {
            assertEquals(v, 2);
          }
        });

        int afterV4 = afterV2 + POST_SWAP_RECORDS;
        emptyPush(storeName); // v4
        drainForDuration(consumer, seen, orderedKeys, POST_SWAP_SETTLE_DRAIN_SECONDS);
        writeRecords(storeName, afterV2, afterV4);
        pollUntilRangeObserved(consumer, seen, orderedKeys, afterV2, afterV4);
        assertNoLoss(seen, 0, afterV4);
      } finally {
        consumer.unsubscribeAll();
      }
    } finally {
      deleteStoreQuietly(storeName);
    }
  }
}
