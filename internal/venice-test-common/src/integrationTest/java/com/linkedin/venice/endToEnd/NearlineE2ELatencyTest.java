package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
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
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.server.VeniceServer;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.system.SystemProducer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class NearlineE2ELatencyTest {
  private static final Logger LOGGER = LogManager.getLogger(NearlineE2ELatencyTest.class);
  private static final long TEST_TIMEOUT = 120_000;
  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final int NUMBER_OF_PARENT_CONTROLLERS = 1;
  private static final int NUMBER_OF_CONTROLLERS = 1;
  private static final int NUMBER_OF_SERVERS = 2;
  private static final int NUMBER_OF_ROUTERS = 1;
  private static final int REPLICATION_FACTOR = 2;

  private static final String CLUSTER_NAME = "venice-cluster0";
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private PubSubPositionTypeRegistry pubSubPositionTypeRegistry;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties serverProperties = new Properties();
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);

    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(NUMBER_OF_CHILD_DATACENTERS)
            .numberOfClusters(NUMBER_OF_CLUSTERS)
            .numberOfParentControllers(NUMBER_OF_PARENT_CONTROLLERS)
            .numberOfChildControllers(NUMBER_OF_CONTROLLERS)
            .numberOfServers(NUMBER_OF_SERVERS)
            .numberOfRouters(NUMBER_OF_ROUTERS)
            .replicationFactor(REPLICATION_FACTOR)
            .forkServer(false)
            .serverProperties(serverProperties);
    multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());
    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
    multiRegionMultiClusterWrapper.logMultiCluster();
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
    String parentControllerUrls =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));
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
    AtomicBoolean localBrokerToReadyToServe = new AtomicBoolean(false);

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
        } else if (k.contains(IngestionStats.NEARLINE_LOCAL_BROKER_TO_READY_TO_SERVE_LATENCY)) {
          localBrokerToReadyToServe.set(true);
          if (k.contains("_current")) {
            LOGGER.info("Server: {} , Metric: {}, Value: {}", sw.getAddressForLogging(), k, v.value());
            Assert.assertTrue(v.value() > 0);
          }
        }
      });
    });
    Assert.assertTrue(producerToLocalBroker.get());
    Assert.assertTrue(localBrokerToReadyToServe.get());
    Assert.assertTrue(producerToLocalBrokerLatencies.stream().anyMatch(v -> v > 0));
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

}
