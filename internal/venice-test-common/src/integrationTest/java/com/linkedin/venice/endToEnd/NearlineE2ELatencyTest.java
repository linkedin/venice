package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CHUNKING_STATUS;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REGION_LOCALITY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_WRITE_TYPE;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducer;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;

import com.linkedin.davinci.stats.IngestionStats;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
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
import com.linkedin.venice.server.VeniceServer;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceChunkingStatus;
import com.linkedin.venice.stats.dimensions.VeniceRegionLocality;
import com.linkedin.venice.stats.dimensions.VeniceStoreWriteType;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
   * the SLO classification dimensions: region locality, partial update status, and chunking status.
   *
   * <p>Uses the same hybrid store setup as {@link #testEndToEndNearlineMetric()}: non-WC, non-chunked,
   * local-region ingestion. After streaming records and waiting for ingestion, verifies that at least
   * one server emitted the histogram with the expected dimension values.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testRecordLevelDelaySloDimensions() {
    String storeName = "test-slo-dims";
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

    // Wait for streaming data to be consumed
    try (AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster0.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
        try {
          Object value = client.get("19").get();
          Assert.assertNotNull(value, "Last streaming record not yet available");
        } catch (Exception e) {
          throw new VeniceException(e);
        }
      });
    }

    // Wait for the heartbeat reporter thread to emit record-level delay metrics.
    // The reporter runs every 60s, so we need to wait long enough for at least one cycle.
    String localityKey = VENICE_REGION_LOCALITY.getDimensionNameInDefaultFormat();
    String writeTypeKey = VENICE_STORE_WRITE_TYPE.getDimensionNameInDefaultFormat();
    String chunkingKey = VENICE_CHUNKING_STATUS.getDimensionNameInDefaultFormat();

    TestUtils.waitForNonDeterministicAssertion(90, TimeUnit.SECONDS, true, true, () -> {
      boolean foundMetric = false;
      for (VeniceServerWrapper sw: cluster0.getVeniceServers()) {
        InMemoryMetricReader reader = getOtelReader(sw);
        if (reader == null) {
          continue;
        }

        // Search all collected metrics for record delay histogram data points that contain
        // the new SLO dimensions. We check for subset containment rather than exact match
        // because the metric also has version_role, replica_type, and replica_state dimensions.
        for (MetricData metric: reader.collectAllMetrics()) {
          if (!metric.getName().contains("ingestion.replication.record.delay")) {
            continue;
          }
          ExponentialHistogramData histData = metric.getExponentialHistogramData();
          if (histData == null) {
            continue;
          }
          for (ExponentialHistogramPointData point: histData.getPoints()) {
            if (point.getCount() == 0) {
              continue;
            }
            String locality = point.getAttributes().get(AttributeKey.stringKey(localityKey));
            String writeType = point.getAttributes().get(AttributeKey.stringKey(writeTypeKey));
            String chunking = point.getAttributes().get(AttributeKey.stringKey(chunkingKey));

            if (VeniceRegionLocality.LOCAL.getDimensionValue().equals(locality)
                && VeniceStoreWriteType.REGULAR.getDimensionValue().equals(writeType)
                && VeniceChunkingStatus.UNCHUNKED.getDimensionValue().equals(chunking)) {
              foundMetric = true;
              LOGGER.info(
                  "Server {} emitted record-level delay metric with SLO dimensions: "
                      + "locality={}, writeType={}, chunking={}, count={}, sum={}ms",
                  sw.getAddressForLogging(),
                  locality,
                  writeType,
                  chunking,
                  point.getCount(),
                  point.getSum());
              break;
            }
          }
          if (foundMetric) {
            break;
          }
        }
        if (foundMetric) {
          break;
        }
      }
      Assert.assertTrue(foundMetric, "No server emitted record-level delay metric with expected SLO dimensions");
    });
  }

  private static InMemoryMetricReader getOtelReader(VeniceServerWrapper server) {
    MetricsRepository metricsRepo = server.getMetricsRepository();
    if (metricsRepo instanceof VeniceMetricsRepository) {
      return (InMemoryMetricReader) ((VeniceMetricsRepository) metricsRepo).getVeniceMetricsConfig()
          .getOtelAdditionalMetricsReader();
    }
    return null;
  }
}
