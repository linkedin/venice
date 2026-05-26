package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_USE_MULTI_REGION_REAL_TIME_TOPIC_SWITCHER_ENABLED;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC;
import static com.linkedin.venice.ConfigKeys.PARENT_KAFKA_CLUSTER_FABRIC_LIST;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.D2_SERVICE_NAME;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;


/**
 * Shared multi-region cluster setup and helpers for integration tests of the active-active aware
 * version-swap path on the {@link com.linkedin.davinci.consumer.VeniceChangelogConsumerDaVinciRecordTransformerImpl}
 * CDC consumer.
 *
 * <p>Concrete subclasses partition the scenarios across separate test classes so each lands in its
 * own CI shard within the 15-minute per-shard limit.
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
public abstract class AbstractDvrtCdcAaVersionSwapTest {
  private static final Logger LOGGER = LogManager.getLogger(AbstractDvrtCdcAaVersionSwapTest.class);
  protected static final int TEST_TIMEOUT = 2 * Time.MS_PER_MINUTE;
  protected static final int PUSH_TIMEOUT = 90_000;
  protected static final int POLL_TIMEOUT_SECONDS = 90;

  protected static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  protected static final int NUMBER_OF_CLUSTERS = 1;
  protected static final int PARTITION_COUNT = 1;
  // Short watchdog so cutover commits quickly; tests drain just past this window before producing
  // post-cutover records.
  protected static final long SWAP_WATCHDOG_TIMEOUT_MS = 5_000L;
  // VSM detection interval (1s) + watchdog fallback (5s) + ~2s buffer = 8s. The happy path closes
  // the barrier sooner than the watchdog when both regions emit VSMs; the buffer covers controller
  // jitter without leaving the test idle for half its runtime.
  protected static final long POST_SWAP_SETTLE_DRAIN_SECONDS = 8;
  protected static final int PRE_SWAP_RECORDS = 10;
  protected static final int POST_SWAP_RECORDS = 5;
  static final String[] CLUSTER_NAMES =
      IntStream.range(0, NUMBER_OF_CLUSTERS).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);

  protected List<VeniceMultiClusterWrapper> childDatacenters;
  protected List<VeniceControllerWrapper> parentControllers;
  protected VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  protected ControllerClient parentControllerClient;
  protected List<ControllerClient> dcControllerClients;

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
  protected static void assertNoLoss(
      Map<String, ChangeEvent<GenericRecord>> seen,
      int startInclusive,
      int endExclusive) {
    for (int i = startInclusive; i < endExclusive; i++) {
      ChangeEvent<GenericRecord> changeEvent = seen.get(String.valueOf(i));
      assertNotNull(changeEvent, "Lost key: " + i + " (must surface from v1 or v2)");
      GenericRecord current = changeEvent.getCurrentValue();
      assertNotNull(current, "Key " + i + " surfaced with null value (expected non-null PUT)");
      assertEquals(String.valueOf(current.get("firstName")), "first_" + i, "Key " + i + " firstName mismatch");
      assertEquals(String.valueOf(current.get("lastName")), "last_" + i, "Key " + i + " lastName mismatch");
    }
  }

  protected void createAaHybridStore(String storeName) {
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

  protected VersionCreationResponse emptyPush(String storeName) {
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
  protected void writeRecords(String storeName, int startIndex, int endIndexExclusive) {
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

  protected ChangelogClientConfig buildClientConfig(VeniceMultiClusterWrapper localRegion, boolean aaEnabled) {
    PubSubBrokerWrapper localKafka = localRegion.getPubSubBrokerWrapper();
    ZkServerWrapper localZk = localRegion.getZkServerWrapper();
    Properties consumerProperties = ChangelogConsumerTestUtils
        .buildConsumerProperties(multiRegionMultiClusterWrapper, localKafka, CLUSTER_NAMES[0], localZk);
    ChangelogClientConfig config = new ChangelogClientConfig().setConsumerProperties(consumerProperties)
        .setControllerD2ServiceName(D2_SERVICE_NAME)
        .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
        .setLocalD2ZkHosts(localZk.getAddress())
        .setD2Client(IntegrationTestPushUtils.getD2Client(localZk.getAddress()))
        // Tight detection interval — VSMs are observable as soon as the new-version VT begins
        // ingesting. Reduces the test's post-swap-trigger settle window proportionally.
        .setVersionSwapDetectionIntervalTimeInSeconds(1L)
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
  protected static void pollUntilRangeObserved(
      VeniceChangelogConsumer<?, ?> consumer,
      Map<String, ChangeEvent<GenericRecord>> seen,
      List<String> orderedKeys,
      int startInclusive,
      int endExclusive) {
    pollUntilRangeObserved((PollFunction) consumer::poll, seen, orderedKeys, startInclusive, endExclusive);
  }

  protected static void pollUntilRangeObserved(
      StatefulVeniceChangelogConsumer<?, ?> consumer,
      Map<String, ChangeEvent<GenericRecord>> seen,
      List<String> orderedKeys,
      int startInclusive,
      int endExclusive) {
    pollUntilRangeObserved((PollFunction) consumer::poll, seen, orderedKeys, startInclusive, endExclusive);
  }

  protected static void pollUntilRangeObserved(
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
  protected static void drainForDuration(
      VeniceChangelogConsumer<?, ?> consumer,
      Map<String, ChangeEvent<GenericRecord>> seen,
      List<String> orderedKeys,
      long drainSeconds) {
    drainForDuration((PollFunction) consumer::poll, seen, orderedKeys, drainSeconds);
  }

  protected static void drainForDuration(
      StatefulVeniceChangelogConsumer<?, ?> consumer,
      Map<String, ChangeEvent<GenericRecord>> seen,
      List<String> orderedKeys,
      long drainSeconds) {
    drainForDuration((PollFunction) consumer::poll, seen, orderedKeys, drainSeconds);
  }

  protected static void drainForDuration(
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
  protected interface PollFunction {
    @SuppressWarnings("rawtypes")
    Collection<? extends PubSubMessage> poll(long timeoutMs);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  protected static void drain(
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

  protected void deleteStoreQuietly(String storeName) {
    // Synchronous best-effort: run on the caller's thread (typically @AfterMethod / finally) so
    // that the next test isn't racing with this cleanup on the controller. Errors are swallowed
    // because cleanup must not mask the real test result.
    try {
      parentControllerClient.disableAndDeleteStore(storeName);
    } catch (Exception e) {
      LOGGER.debug("Best-effort cleanup of store {} failed", storeName, e);
    }
  }
}
