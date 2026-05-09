package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CHUNKING_STATUS;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REGION_LOCALITY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REPLICA_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_WRITE_TYPE;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecordWithKeyPrefix;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;

import com.linkedin.davinci.stats.IngestionStats;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.server.VeniceServer;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.ReplicaType;
import com.linkedin.venice.stats.dimensions.VeniceChunkingStatus;
import com.linkedin.venice.stats.dimensions.VeniceRegionLocality;
import com.linkedin.venice.stats.dimensions.VeniceStoreWriteType;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.metrics.data.ExponentialHistogramData;
import io.opentelemetry.sdk.metrics.data.ExponentialHistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class NearlineE2ELatencyTest extends AbstractMultiRegionTest {
  private static final Logger LOGGER = LogManager.getLogger(NearlineE2ELatencyTest.class);
  private static final long TEST_TIMEOUT = 120_000;
  // Worst-case budget: 60s push + 30s read availability + 90s heartbeat reporter cycle.
  private static final long SLO_DIMS_TEST_TIMEOUT = 240_000;

  private PubSubPositionTypeRegistry pubSubPositionTypeRegistry;

  @Override
  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Utils.thisIsLocalhost();
    super.setUp();
    pubSubPositionTypeRegistry =
        multiRegionMultiClusterWrapper.getParentKafkaBrokerWrapper().getPubSubPositionTypeRegistry();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testEndToEndNearlineMetric() {
    // Check if producer timestamp is preserved
    // Send a push job
    // Send some nearline messages
    // Check nearline timestamp is recieved correctly by all servers
    String storeName = "test-hybrid";
    String parentControllerUrls = parentController.getControllerUrl();
    try (ControllerClient parentControllerCli = new ControllerClient(CLUSTER_NAME, parentControllerUrls);
        ControllerClient dc0Client =
            new ControllerClient(CLUSTER_NAME, childDatacenters.get(0).getControllerConnectString());
        ControllerClient dc1Client =
            new ControllerClient(CLUSTER_NAME, childDatacenters.get(1).getControllerConnectString())) {
      List<ControllerClient> dcControllerClientList = Arrays.asList(dc0Client, dc1Client);
      TestUtils.createAndVerifyStoreInAllRegions(storeName, parentControllerCli, dcControllerClientList);
      Assert.assertFalse(
          parentControllerCli
              .updateStore(
                  storeName,
                  new UpdateStoreQueryParams().setHybridRewindSeconds(10)
                      .setHybridOffsetLagThreshold(5)
                      .setNativeReplicationEnabled(true)
                      .setPartitionCount(1))
              .isError());
      TestUtils.verifyDCConfigNativeAndActiveRepl(storeName, true, false, dc0Client, dc1Client);
      VersionCreationResponse versionCreationResponse = parentControllerCli.requestTopicForWrites(
          storeName,
          1024,
          Version.PushType.BATCH,
          Version.guidBasedDummyPushId(),
          true,
          false,
          false,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          false,
          -1);
      Assert.assertFalse(versionCreationResponse.isError());
      PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
          childDatacenters.get(0).getKafkaBrokerWrapper().getPubSubClientsFactory().getProducerAdapterFactory();
      List<PubSubBrokerWrapper> pubSubBrokerWrappers =
          childDatacenters.stream().map(VeniceMultiClusterWrapper::getKafkaBrokerWrapper).collect(Collectors.toList());
      Map<String, String> additionalConfigs = PubSubBrokerWrapper.getBrokerDetailsForClients(pubSubBrokerWrappers);
      TestUtils.writeBatchData(
          versionCreationResponse,
          STRING_SCHEMA.toString(),
          STRING_SCHEMA.toString(),
          IntStream.range(0, 10).mapToObj(i -> new AbstractMap.SimpleEntry<>(String.valueOf(i), String.valueOf(i))),
          HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID,
          pubSubProducerAdapterFactory,
          additionalConfigs,
          pubSubPositionTypeRegistry);
      JobStatusQueryResponse response = parentControllerCli
          .queryDetailedJobStatus(versionCreationResponse.getKafkaTopic(), childDatacenters.get(0).getRegionName());
      Assert.assertFalse(response.isError());
      TestUtils.waitForNonDeterministicPushCompletion(
          versionCreationResponse.getKafkaTopic(),
          parentControllerCli,
          60,
          TimeUnit.SECONDS);
    }

    VeniceClusterWrapper cluster0 = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
    SystemProducer dc0Producer = getSamzaProducer(cluster0, storeName, Version.PushType.STREAM);

    for (int i = 10; i < 20; i++) {
      sendStreamingRecord(dc0Producer, storeName, i);
    }

    try (AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster0.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
        try {
          for (int i = 10; i < 20; i++) {
            String key = Integer.toString(i);
            Object value = client.get(key).get();
            Assert.assertNotNull(value, "Did not find key " + i + " in store before restarting SN.");
            Assert.assertEquals(value.toString(), "stream_" + key);
          }
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });
    }

    AtomicBoolean producerToLocalBroker = new AtomicBoolean(false);

    Set<Double> producerToLocalBrokerLatencies = new HashSet<>();
    cluster0.getVeniceServers().forEach(sw -> {
      VeniceServer vs = sw.getVeniceServer();
      MetricsRepository metricsRepository = vs.getMetricsRepository();
      // Only the current version will have metrics in this test case
      metricsRepository.metrics().forEach((k, v) -> {
        if (k.contains(IngestionStats.NEARLINE_PRODUCER_TO_LOCAL_BROKER_LATENCY)) {
          producerToLocalBroker.set(true);
          if (k.contains("_current")) {
            LOGGER.info("Server: {} , Metric: {}, Value: {}", sw.getAddressForLogging(), k, v.value());
            producerToLocalBrokerLatencies.add(v.value());
          }
        }
      });
    });
    Assert.assertTrue(producerToLocalBroker.get());
    Assert.assertTrue(producerToLocalBrokerLatencies.stream().anyMatch(v -> v > 0));
  }

  /**
   * Validates that the record-level delay OTel metric (ingestion.replication.record.delay) includes
   * the SLO classification dimensions: region locality, store write type, and chunking status.
   *
   * <p>Uses the same hybrid store setup as {@link #testEndToEndNearlineMetric()}: non-WC, non-chunked,
   * local-region ingestion. After streaming records and waiting for ingestion, verifies that at least
   * one server emitted the histogram with the expected dimension values.
   */
  @Test(timeOut = SLO_DIMS_TEST_TIMEOUT)
  public void testRecordLevelDelaySloDimensions() {
    String storeName = "test-slo-dims";
    String parentControllerUrls = parentController.getControllerUrl();
    int versionNumber;
    try (ControllerClient parentControllerCli = new ControllerClient(CLUSTER_NAME, parentControllerUrls);
        ControllerClient dc0Client =
            new ControllerClient(CLUSTER_NAME, childDatacenters.get(0).getControllerConnectString());
        ControllerClient dc1Client =
            new ControllerClient(CLUSTER_NAME, childDatacenters.get(1).getControllerConnectString())) {
      List<ControllerClient> dcControllerClientList = Arrays.asList(dc0Client, dc1Client);
      TestUtils.createAndVerifyStoreInAllRegions(storeName, parentControllerCli, dcControllerClientList);
      /*
       * Active-active replication so each fabric's leaders pull from EVERY fabric's RT —
       * a leader processing a record from a remote-fabric RT emits locality=REMOTE.
       * Without AA, only the source-fabric's RT carries records, and the cross-region branch
       * isn't exercised at all (verified empirically: dc1 won't even ingest "19" within 120s
       * under plain NR with a single dc0 producer).
       */
      Assert.assertFalse(
          parentControllerCli
              .updateStore(
                  storeName,
                  new UpdateStoreQueryParams().setHybridRewindSeconds(10)
                      .setHybridOffsetLagThreshold(5)
                      .setNativeReplicationEnabled(true)
                      .setActiveActiveReplicationEnabled(true)
                      .setPartitionCount(1))
              .isError());
      TestUtils.verifyDCConfigNativeAndActiveRepl(storeName, true, true, dc0Client, dc1Client);
      // Empty push to materialize the hybrid version — no batch records, just the version
      // boundary so the streaming records can be ingested.
      ControllerResponse emptyPushResponse = parentControllerCli
          .sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-slo-dims-push"), 1L, 60_000L);
      Assert.assertFalse(emptyPushResponse.isError());
      Assert.assertTrue(emptyPushResponse instanceof JobStatusQueryResponse);
      versionNumber = ((JobStatusQueryResponse) emptyPushResponse).getVersion();
      // Wait for the empty push to be current in BOTH fabrics so each fabric's leaders are ready
      // to ingest streaming records.
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, true, () -> {
        for (ControllerClient cc: dcControllerClientList) {
          Assert.assertEquals(cc.getStore(storeName).getStore().getCurrentVersion(), versionNumber);
        }
      });
    }

    /*
     * Write streaming records via per-fabric Samza producers. With AA enabled, each fabric's
     * leader pulls EVERY fabric's RT — so each leader sees records originating from BOTH its
     * local RT (locality=LOCAL) and the remote RT (locality=REMOTE).
     */
    Map<Integer, VeniceSystemProducer> fabricToProducer = new HashMap<>();
    int recordsPerFabric = 10;
    try {
      for (int dcIndex = 0; dcIndex < childDatacenters.size(); dcIndex++) {
        VeniceSystemProducer producer =
            IntegrationTestPushUtils.getSamzaProducerForStream(multiRegionMultiClusterWrapper, dcIndex, storeName);
        fabricToProducer.put(dcIndex, producer);
        String keyPrefix = "dc-" + dcIndex + "_key_";
        for (int i = 0; i < recordsPerFabric; i++) {
          sendStreamingRecordWithKeyPrefix(producer, storeName, keyPrefix, i);
        }
      }

      // Wait for cross-fabric replication: each fabric should serve records produced in BOTH
      // fabrics. This guarantees leaders on each fabric processed at least one record from
      // each region, which in turn guarantees both LOCAL and REMOTE record-delay points are
      // emitted before the metric assertion runs.
      for (VeniceMultiClusterWrapper dc: childDatacenters) {
        VeniceClusterWrapper cluster = dc.getClusters().get(CLUSTER_NAME);
        try (AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL()))) {
          TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, true, () -> {
            for (int dcIndex = 0; dcIndex < childDatacenters.size(); dcIndex++) {
              String key = "dc-" + dcIndex + "_key_" + (recordsPerFabric - 1);
              try {
                Assert.assertNotNull(
                    client.get(key).get(),
                    "Records from fabric dc-" + dcIndex + " not yet replicated to " + cluster.getRandomRouterURL()
                        + " (key=" + key + ")");
              } catch (Exception e) {
                throw new VeniceException(e);
              }
            }
          });
        }
      }
    } finally {
      for (VeniceSystemProducer producer: fabricToProducer.values()) {
        producer.stop();
      }
    }

    String localityKey = VENICE_REGION_LOCALITY.getDimensionNameInDefaultFormat();
    String writeTypeKey = VENICE_STORE_WRITE_TYPE.getDimensionNameInDefaultFormat();
    String chunkingKey = VENICE_CHUNKING_STATUS.getDimensionNameInDefaultFormat();
    String storeNameKey = VENICE_STORE_NAME.getDimensionNameInDefaultFormat();
    String replicaTypeKey = VENICE_REPLICA_TYPE.getDimensionNameInDefaultFormat();

    /*
     * Collect every (replica_type, locality) combination observed for this store across servers
     * in BOTH fabrics. With AA enabled and writes from both regions, each fabric's leaders pull
     * EVERY fabric's RT — so leaders emit BOTH locality=LOCAL (records from local-fabric RT)
     * AND locality=REMOTE (records from remote-fabric RT).
     *
     * NOTE: Followers always emit locality=LOCAL today because they read from local VT only.
     * If/when followers start emitting per-region (e.g., follower-side cross-region tracking
     * lands), update this test to also assert (FOLLOWER, REMOTE).
     */
    Set<String> observedLeaderLocalities = ConcurrentHashMap.newKeySet();
    Set<String> observedFollowerLocalities = ConcurrentHashMap.newKeySet();
    List<VeniceServerWrapper> serversInBothFabrics = new ArrayList<>();
    for (VeniceMultiClusterWrapper dc: childDatacenters) {
      serversInBothFabrics.addAll(dc.getClusters().get(CLUSTER_NAME).getVeniceServers());
    }

    TestUtils.waitForNonDeterministicAssertion(90, TimeUnit.SECONDS, true, true, () -> {
      observedLeaderLocalities.clear();
      observedFollowerLocalities.clear();
      for (VeniceServerWrapper sw: serversInBothFabrics) {
        InMemoryMetricReader reader = getOtelReader(sw);
        if (reader == null) {
          continue;
        }
        for (MetricData metric: reader.collectAllMetrics()) {
          if (!metric.getName().contains("ingestion.replication.record.delay")) {
            continue;
          }
          ExponentialHistogramData histData = metric.getExponentialHistogramData();
          if (histData == null) {
            continue;
          }
          for (ExponentialHistogramPointData point: histData.getPoints()) {
            if (point.getCount() == 0 || point.getSum() <= 0 || point.getMax() <= 0) {
              continue;
            }
            String pointStoreName = point.getAttributes().get(AttributeKey.stringKey(storeNameKey));
            String replicaType = point.getAttributes().get(AttributeKey.stringKey(replicaTypeKey));
            String locality = point.getAttributes().get(AttributeKey.stringKey(localityKey));
            String writeType = point.getAttributes().get(AttributeKey.stringKey(writeTypeKey));
            String chunking = point.getAttributes().get(AttributeKey.stringKey(chunkingKey));

            if (!storeName.equals(pointStoreName) || !VeniceStoreWriteType.REGULAR.getDimensionValue().equals(writeType)
                || !VeniceChunkingStatus.UNCHUNKED.getDimensionValue().equals(chunking)) {
              continue;
            }
            if (ReplicaType.LEADER.getDimensionValue().equals(replicaType)) {
              observedLeaderLocalities.add(locality);
            } else if (ReplicaType.FOLLOWER.getDimensionValue().equals(replicaType)) {
              observedFollowerLocalities.add(locality);
            }
          }
        }
      }
      LOGGER.info(
          "Observed leader localities: {}, follower localities: {}",
          observedLeaderLocalities,
          observedFollowerLocalities);

      // Leader replicas emit BOTH localities: LOCAL when consuming local-fabric RT records,
      // REMOTE when consuming remote-fabric RT records (AA cross-region pull).
      Assert.assertTrue(
          observedLeaderLocalities.contains(VeniceRegionLocality.LOCAL.getDimensionValue()),
          "No LEADER replica emitted record-delay with locality=LOCAL. " + "Observed leader localities: "
              + observedLeaderLocalities);
      Assert.assertTrue(
          observedLeaderLocalities.contains(VeniceRegionLocality.REMOTE.getDimensionValue()),
          "No LEADER replica emitted record-delay with locality=REMOTE (AA cross-region pull). "
              + "Observed leader localities: " + observedLeaderLocalities);
      // Follower replicas only emit LOCAL today (they read from local VT).
      Assert.assertTrue(
          observedFollowerLocalities.contains(VeniceRegionLocality.LOCAL.getDimensionValue()),
          "No FOLLOWER replica emitted record-delay with locality=LOCAL. " + "Observed follower localities: "
              + observedFollowerLocalities);
    });
  }

  private static InMemoryMetricReader getOtelReader(VeniceServerWrapper server) {
    MetricsRepository metricsRepo = server.getMetricsRepository();
    if (!(metricsRepo instanceof VeniceMetricsRepository)) {
      return null;
    }
    Object reader = ((VeniceMetricsRepository) metricsRepo).getVeniceMetricsConfig().getOtelAdditionalMetricsReader();
    return reader instanceof InMemoryMetricReader ? (InMemoryMetricReader) reader : null;
  }
}
