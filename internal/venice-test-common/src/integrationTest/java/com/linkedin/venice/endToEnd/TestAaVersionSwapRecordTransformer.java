package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.consumer.stats.BasicConsumerStats.CONSUMER_METRIC_ENTITIES;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_USE_MULTI_REGION_REAL_TIME_TOPIC_SWITCHER_ENABLED;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC;
import static com.linkedin.venice.ConfigKeys.PARENT_KAFKA_CLUSTER_FABRIC_LIST;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.D2_SERVICE_NAME;
import static com.linkedin.venice.stats.ClientType.CHANGE_DATA_CAPTURE_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestUtils.createAndVerifyStoreInAllRegions;
import static com.linkedin.venice.utils.TestUtils.updateStoreToHybrid;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.davinci.consumer.ChangeEvent;
import com.linkedin.davinci.consumer.ChangelogClientConfig;
import com.linkedin.davinci.consumer.StatefulVeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerClientFactory;
import com.linkedin.venice.consumer.ChangelogConsumerTestUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration tests for the active-active aware version swap path on the
 * {@link com.linkedin.davinci.consumer.VeniceChangelogConsumerDaVinciRecordTransformerImpl} CDC consumer.
 *
 * <h3>What these tests verify (deterministic)</h3>
 * <ul>
 *   <li>Pre-swap records produced before the cutover surface through the consumer (sourced from the
 *       old version's VT).</li>
 *   <li>The AA path runs end-to-end without crashing or deadlocking through the swap.</li>
 *   <li>Records produced sufficiently after the watchdog timeout has elapsed surface through the
 *       consumer (sourced from the new version's VT).</li>
 *   <li>The coordinator handles back-to-back swaps and rollback correctly.</li>
 * </ul>
 *
 * <h3>What these tests intentionally do not verify</h3>
 * <p>The "no-loss" property for records produced during the cutover window is not asserted: per the
 * AA design (see {@link com.linkedin.davinci.consumer.RecordTransformerVersionSwapCoordinator}'s
 * Javadoc), records that arrive in the same Kafka poll-batch as a VSM on the future side are
 * dropped pre-commit and not redelivered. The plan accepts this under the assumption of poll-batch
 * symmetry between current and future leaders, but that assumption can fail under artificial test
 * stress and is therefore not asserted here. Unit tests in
 * {@code RecordTransformerVersionSwapCoordinatorTest} cover the coordinator's exact state machine.
 */
public class TestAaVersionSwapRecordTransformer {
  private static final Logger LOGGER = LogManager.getLogger(TestAaVersionSwapRecordTransformer.class);
  private static final int TEST_TIMEOUT = 2 * Time.MS_PER_MINUTE;
  private static final int PUSH_TIMEOUT = 90_000;
  private static final int POLL_TIMEOUT_SECONDS = 90;

  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final int PARTITION_COUNT = 1;
  // Short watchdog so cutover commits quickly; tests drain just past this window before producing
  // post-cutover records.
  private static final long SWAP_WATCHDOG_TIMEOUT_MS = 5_000L;
  private static final long POST_SWAP_SETTLE_DRAIN_SECONDS = 15;
  private static final int PRE_SWAP_RECORDS = 10;
  private static final int POST_SWAP_RECORDS = 5;
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private ControllerClient parentControllerClient;
  private List<ControllerClient> dcControllerClients;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Properties serverProperties = new Properties();
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    serverProperties.put(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, true);

    Properties controllerProps = new Properties();
    controllerProps.put(NATIVE_REPLICATION_SOURCE_FABRIC, "dc-0");
    controllerProps.put(PARENT_KAFKA_CLUSTER_FABRIC_LIST, DEFAULT_PARENT_DATA_CENTER_REGION_NAME);
    controllerProps.put(CONTROLLER_USE_MULTI_REGION_REAL_TIME_TOPIC_SWITCHER_ENABLED, true);

    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(NUMBER_OF_CHILD_DATACENTERS)
            .numberOfClusters(NUMBER_OF_CLUSTERS)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(2)
            .numberOfRouters(1)
            .replicationFactor(2)
            .forkServer(false)
            .parentControllerProperties(controllerProps)
            .childControllerProperties(controllerProps)
            .serverProperties(serverProperties);
    multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());
    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    parentControllers = multiRegionMultiClusterWrapper.getParentControllers();

    String clusterName = CLUSTER_NAMES[0];
    String parentControllerURLs =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));
    parentControllerClient = new ControllerClient(clusterName, parentControllerURLs);
    dcControllerClients = Arrays.asList(
        new ControllerClient(clusterName, childDatacenters.get(0).getControllerConnectString()),
        new ControllerClient(clusterName, childDatacenters.get(1).getControllerConnectString()));
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(parentControllerClient);
    if (dcControllerClients != null) {
      dcControllerClients.forEach(Utils::closeQuietlyWithErrorLogged);
    }
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

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
        TestUtils.waitForNonDeterministicAssertion(
            60,
            TimeUnit.SECONDS,
            true,
            () -> assertEquals(parentControllerClient.getStore(storeName).getStore().getCurrentVersion(), 2));

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

  // ---------- helpers ----------

  /**
   * Asserts that every record produced to the RT in the half-open range {@code [startInclusive,
   * endExclusive)} surfaced through the consumer (from either v1's or v2's transformer) <em>with
   * the correct value</em>. This is the AA path's at-least-once guarantee: records may flow
   * through v1, v2, or both, but none should be lost or corrupted.
   *
   * <p>Each key {@code i} was produced with value {@code firstName="first_i", lastName="last_i"}
   * (see {@link #writeRecords}). RT replication preserves the value across both v1's and v2's
   * VTs, so the surfaced value must match regardless of which version's transformer delivered it.
   */
  private static void assertNoLoss(Map<String, ChangeEvent<GenericRecord>> seen, int startInclusive, int endExclusive) {
    for (int i = startInclusive; i < endExclusive; i++) {
      ChangeEvent<GenericRecord> changeEvent = seen.get(String.valueOf(i));
      assertNotNull(changeEvent, "Lost key: " + i + " (must surface from v1 or v2)");
      GenericRecord current = changeEvent.getCurrentValue();
      assertNotNull(current, "Key " + i + " surfaced with null value (expected non-null PUT)");
      assertEquals(String.valueOf(current.get("firstName")), "first_" + i, "Key " + i + " firstName mismatch");
      assertEquals(String.valueOf(current.get("lastName")), "last_" + i, "Key " + i + " lastName mismatch");
    }
  }

  private void createAaHybridStore(String storeName) {
    String keySchemaStr = TestChangelogKey.SCHEMA$.toString();
    String valueSchemaStr = TestChangelogValue.SCHEMA$.toString();
    createAndVerifyStoreInAllRegions(
        storeName,
        parentControllerClient,
        dcControllerClients,
        keySchemaStr,
        valueSchemaStr);
    updateStoreToHybrid(storeName, parentControllerClient, Optional.of(true), Optional.of(true), Optional.of(true));
    parentControllerClient.updateStore(
        storeName,
        new UpdateStoreQueryParams().setHybridRewindSeconds(60L).setPartitionCount(PARTITION_COUNT));
  }

  private VersionCreationResponse emptyPush(String storeName) {
    VersionCreationResponse response =
        parentControllerClient.emptyPush(storeName, Utils.getUniqueString("empty-hybrid-push"), 1L);
    assertCommand(response);
    TestUtils.waitForNonDeterministicAssertion(
        PUSH_TIMEOUT,
        TimeUnit.MILLISECONDS,
        true,
        () -> Assert.assertEquals(
            parentControllerClient.queryJobStatus(response.getKafkaTopic()).getStatus(),
            ExecutionStatus.COMPLETED.toString()));
    return response;
  }

  /** Convenience overload — produces records to dc-0's RT, which is where consumers subscribe. */
  private void writeRecords(String storeName, int startIndex, int endIndexExclusive) {
    try (VeniceSystemProducer producer = IntegrationTestPushUtils.getSamzaProducer(
        childDatacenters.get(0).getClusters().get(CLUSTER_NAMES[0]),
        storeName,
        Version.PushType.STREAM)) {
      producer.start();
      for (int i = startIndex; i < endIndexExclusive; i++) {
        TestChangelogKey key = new TestChangelogKey();
        key.id = i;
        TestChangelogValue value = new TestChangelogValue();
        value.firstName = "first_" + i;
        value.lastName = "last_" + i;
        sendStreamingRecord(producer, storeName, key, value, null);
      }
    }
  }

  private ChangelogClientConfig buildClientConfig(VeniceMultiClusterWrapper localRegion, boolean aaEnabled) {
    PubSubBrokerWrapper localKafka = localRegion.getPubSubBrokerWrapper();
    ZkServerWrapper localZk = localRegion.getZkServerWrapper();
    Properties consumerProperties = ChangelogConsumerTestUtils
        .buildConsumerProperties(multiRegionMultiClusterWrapper, localKafka, CLUSTER_NAMES[0], localZk);
    ChangelogClientConfig config = new ChangelogClientConfig().setConsumerProperties(consumerProperties)
        .setControllerD2ServiceName(D2_SERVICE_NAME)
        .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .setLocalD2ZkHosts(localZk.getAddress())
        .setD2Client(IntegrationTestPushUtils.getD2Client(localZk.getAddress()))
        .setVersionSwapDetectionIntervalTimeInSeconds(3L)
        .setControllerRequestRetryCount(3)
        .setBootstrapFileSystemPath(getTempDataDirectory().getAbsolutePath());
    if (aaEnabled) {
      config.setVersionSwapByControlMessageEnabled(true)
          .setClientRegionName(localRegion.getRegionName())
          .setTotalRegionCount(childDatacenters.size())
          .setVersionSwapTimeoutInMs(SWAP_WATCHDOG_TIMEOUT_MS);
    }
    return config;
  }

  /**
   * Polls until every key in the half-open range {@code [startInclusive, endExclusive)} appears
   * in the seen map. With multiple partitions, individual keys can arrive in any order across
   * partitions; this helper keeps polling until all expected keys have been delivered (rather
   * than exiting on the first sentinel). Topic origin is intentionally not asserted — that depends
   * on cutover timing per the AA design. See
   * {@link com.linkedin.davinci.consumer.RecordTransformerVersionSwapCoordinator}'s class-level
   * Javadoc for the discussion of cutover semantics.
   */
  private static void pollUntilRangeObserved(
      VeniceChangelogConsumer<?, ?> consumer,
      Map<String, ChangeEvent<GenericRecord>> seen,
      List<String> orderedKeys,
      int startInclusive,
      int endExclusive) {
    pollUntilRangeObserved((PollFunction) consumer::poll, seen, orderedKeys, startInclusive, endExclusive);
  }

  private static void pollUntilRangeObserved(
      StatefulVeniceChangelogConsumer<?, ?> consumer,
      Map<String, ChangeEvent<GenericRecord>> seen,
      List<String> orderedKeys,
      int startInclusive,
      int endExclusive) {
    pollUntilRangeObserved((PollFunction) consumer::poll, seen, orderedKeys, startInclusive, endExclusive);
  }

  private static void pollUntilRangeObserved(
      PollFunction pollFn,
      Map<String, ChangeEvent<GenericRecord>> seen,
      List<String> orderedKeys,
      int startInclusive,
      int endExclusive) {
    TestUtils.waitForNonDeterministicAssertion(POLL_TIMEOUT_SECONDS, TimeUnit.SECONDS, true, () -> {
      drain(pollFn, seen, orderedKeys);
      for (int i = startInclusive; i < endExclusive; i++) {
        assertNotNull(seen.get(String.valueOf(i)), "Key " + i + " not yet observed");
      }
    });
  }

  /**
   * Polls the consumer for {@code drainSeconds} seconds, accumulating observations into the
   * caller's seen-map. Used after a swap is triggered to absorb residual records and let the
   * watchdog timeout fire so subsequent records can be produced post-cutover.
   */
  private static void drainForDuration(
      VeniceChangelogConsumer<?, ?> consumer,
      Map<String, ChangeEvent<GenericRecord>> seen,
      List<String> orderedKeys,
      long drainSeconds) {
    drainForDuration((PollFunction) consumer::poll, seen, orderedKeys, drainSeconds);
  }

  private static void drainForDuration(
      StatefulVeniceChangelogConsumer<?, ?> consumer,
      Map<String, ChangeEvent<GenericRecord>> seen,
      List<String> orderedKeys,
      long drainSeconds) {
    drainForDuration((PollFunction) consumer::poll, seen, orderedKeys, drainSeconds);
  }

  private static void drainForDuration(
      PollFunction pollFn,
      Map<String, ChangeEvent<GenericRecord>> seen,
      List<String> orderedKeys,
      long drainSeconds) {
    long deadline = System.currentTimeMillis() + drainSeconds * 1000;
    while (System.currentTimeMillis() < deadline) {
      drain(pollFn, seen, orderedKeys);
    }
  }

  /**
   * Functional adapter so the same drain helpers work with both {@link VeniceChangelogConsumer}
   * and {@link StatefulVeniceChangelogConsumer}, which don't share an interface.
   */
  @FunctionalInterface
  private interface PollFunction {
    @SuppressWarnings("rawtypes")
    Collection<? extends PubSubMessage> poll(long timeoutMs);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private static void drain(
      PollFunction pollFn,
      Map<String, ChangeEvent<GenericRecord>> seen,
      List<String> orderedKeys) {
    Collection<? extends PubSubMessage> messages = pollFn.poll(500);
    for (PubSubMessage message: messages) {
      if (message.getKey() == null) {
        continue; // skip control messages
      }
      String keyId;
      Object key = message.getKey();
      if (key instanceof GenericRecord) {
        keyId = String.valueOf(((GenericRecord) key).get("id"));
      } else if (key instanceof TestChangelogKey) {
        keyId = String.valueOf(((TestChangelogKey) key).id);
      } else {
        keyId = String.valueOf(key);
      }
      seen.put(keyId, (ChangeEvent<GenericRecord>) message.getValue());
      orderedKeys.add(keyId);
    }
  }

  private void deleteStoreQuietly(String storeName) {
    CompletableFuture.runAsync(() -> {
      try {
        parentControllerClient.disableAndDeleteStore(storeName);
      } catch (Exception e) {
        LOGGER.debug("Best-effort cleanup of store {} failed", storeName, e);
      }
    });
  }
}
