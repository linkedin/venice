package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.*;
import static com.linkedin.venice.utils.TestWriteUtils.*;

import com.linkedin.davinci.stats.IngestionStats;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.StoreHealthAuditResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.RegionPushDetails;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.server.VeniceServer;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
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

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties serverProperties = new Properties();
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);

    multiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
        NUMBER_OF_CHILD_DATACENTERS,
        NUMBER_OF_CLUSTERS,
        NUMBER_OF_PARENT_CONTROLLERS,
        NUMBER_OF_CONTROLLERS,
        NUMBER_OF_SERVERS,
        NUMBER_OF_ROUTERS,
        REPLICATION_FACTOR,
        Optional.empty(),
        Optional.empty(),
        Optional.of(new VeniceProperties(serverProperties)),
        false);
    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
    logMultiCluster();
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
      TestUtils.writeBatchData(
          versionCreationResponse,
          STRING_SCHEMA,
          STRING_SCHEMA,
          IntStream.range(0, 10).mapToObj(i -> new AbstractMap.SimpleEntry<>(String.valueOf(i), String.valueOf(i))),
          HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID);
      JobStatusQueryResponse response = parentControllerCli
          .queryDetailedJobStatus(versionCreationResponse.getKafkaTopic(), childDatacenters.get(0).getRegionName());
      Assert.assertFalse(response.isError());
      TestUtils.waitForNonDeterministicPushCompletion(
          versionCreationResponse.getKafkaTopic(),
          parentControllerCli,
          60,
          TimeUnit.SECONDS);

      StoreHealthAuditResponse resp = parentControllerCli.listStorePushInfo(storeName, true);
      LOGGER.info("StoreHealthAuditResponse = {}", resp);
      Assert.assertFalse(resp.isError());
      Assert.assertFalse(resp.getRegionPushDetails().isEmpty());
      for (Map.Entry<String, RegionPushDetails> entry: resp.getRegionPushDetails().entrySet()) {
        RegionPushDetails detail = entry.getValue();
        Assert.assertEquals(entry.getKey(), detail.getRegionName());
        Assert.assertFalse(detail.getPartitionDetails().isEmpty());
        Assert.assertFalse(detail.getPartitionDetails().get(0).getReplicaDetails().isEmpty());
      }
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

    cluster0.getVeniceServers().forEach(s -> {
      VeniceServer veniceServer = s.getVeniceServer();
      MetricsRepository metricsRepository = veniceServer.getMetricsRepository();
      metricsRepository.metrics().forEach((k, v) -> {
        if (k.contains(IngestionStats.NEARLINE_PRODUCER_TO_LOCAL_BROKER_LATENCY)) {
          LOGGER.info("Server: {} , Metric: {}, Value: {}", s.getAddressForLogging(), k, v.value());
          producerToLocalBroker.set(true);
        } else if (k.contains(IngestionStats.NEARLINE_LOCAL_BROKER_TO_READY_TO_SERVE_LATENCY)) {
          LOGGER.info("Server: {} , Metric: {}, Value: {}", s.getAddressForLogging(), k, v.value());
          localBrokerToReadyToServe.set(true);
          // Assert.assertTrue(v.value() > 0);
        }
      });
    });
    Assert.assertTrue(producerToLocalBroker.get());
    Assert.assertTrue(localBrokerToReadyToServe.get());
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  private void logMultiCluster() {
    LOGGER.info(
        "--> Multiregion cluster created : parentRegion: {}, childRegions: {},"
            + " clusters: {}, zk: {}, broker: {}, controllers: {}",
        multiRegionMultiClusterWrapper.getParentRegionName(),
        multiRegionMultiClusterWrapper.getChildRegionNames(),
        Arrays.toString(multiRegionMultiClusterWrapper.getClusterNames()),
        multiRegionMultiClusterWrapper.getZkServerWrapper(),
        multiRegionMultiClusterWrapper.getParentKafkaBrokerWrapper(),
        multiRegionMultiClusterWrapper.getParentControllers()
            .stream()
            .map(VeniceControllerWrapper::getControllerUrl)
            .collect(Collectors.joining(",")));

    Map<String, String> debugNames = new HashMap<>();

    for (VeniceMultiClusterWrapper childDatacenter: childDatacenters) {
      LOGGER.info(
          "--> ChildDataCenter : name: {}, controllers: {} clusters: {}, zk: {}, broker: {}",
          childDatacenter.getRegionName(),
          childDatacenter.getControllers()
              .entrySet()
              .stream()
              .map(e -> e.getKey() + ":" + e.getValue().getControllerUrl())
              .collect(Collectors.joining(",")),
          Arrays.toString(childDatacenter.getClusterNames()),
          childDatacenter.getZkServerWrapper(),
          childDatacenter.getKafkaBrokerWrapper());
      Map<String, VeniceClusterWrapper> clusters = childDatacenter.getClusters();
      for (String cluster: clusters.keySet()) {
        VeniceClusterWrapper clusterWrapper = clusters.get(cluster);
        LOGGER.info(
            "--> Cluster -> cluster: {}, region: {} , controller: {}, zk: {}, broker: {} ",
            cluster,
            clusterWrapper.getRegionName(),
            clusterWrapper.getAllControllersURLs(),
            clusterWrapper.getZk(),
            clusterWrapper.getKafka());
        LOGGER.info("--> broker: {}", clusterWrapper.getKafka());
        debugNames.put(clusterWrapper.getKafka().getSSLAddress(), childDatacenter.getRegionName() + "-ks");
        debugNames.put(clusterWrapper.getKafka().getAddress(), childDatacenter.getRegionName() + "-kh");
        for (VeniceControllerWrapper controller: clusterWrapper.getVeniceControllers()) {
          LOGGER.info("--> Controller: {}", controller.getControllerUrl());
          debugNames.put(controller.getControllerUrl(), childDatacenter.getRegionName() + "-c");
        }
        for (int i = 0; i < clusterWrapper.getVeniceServers().size(); i++) {
          VeniceServerWrapper server = clusterWrapper.getVeniceServers().get(i);
          LOGGER.info("--> Server: {}", server.getAddressForLogging());
          debugNames.put(server.getAddressForLogging(), childDatacenter.getRegionName() + "-s" + i);
        }
        for (VeniceRouterWrapper router: clusterWrapper.getVeniceRouters()) {
          LOGGER.info("--> Router: {}", router.getAddressForLogging());
          debugNames.put(router.getAddressForLogging(), childDatacenter.getRegionName() + "-r");
        }
      }
    }

    // DebugUtils.debugNames(debugNames);
  }

}
