package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestWriteUtils.loadFileAsString;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.kafka.consumer.KafkaConsumerServiceDelegator;
import com.linkedin.davinci.kafka.consumer.ReplicaHeartbeatInfo;
import com.linkedin.venice.AdminTool;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestDumpIngestionContext {
  private static final Logger LOGGER = LogManager.getLogger(TestDumpIngestionContext.class);
  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final int TEST_TIMEOUT_MS = 180_000;

  private static final int REPLICATION_FACTOR = 2;
  private static final String CLUSTER_NAME = "venice-cluster0";

  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private VeniceControllerWrapper parentController;
  private List<VeniceMultiClusterWrapper> childDatacenters;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Properties serverProperties = new Properties();
    serverProperties.put(ConfigKeys.SERVER_RESUBSCRIPTION_TRIGGERED_BY_VERSION_INGESTION_CONTEXT_CHANGE_ENABLED, true);
    serverProperties.put(
        ConfigKeys.SERVER_CONSUMER_POOL_ALLOCATION_STRATEGY,
        KafkaConsumerServiceDelegator.ConsumerPoolStrategyType.CURRENT_VERSION_PRIORITIZATION.name());
    Properties controllerProps = new Properties();
    controllerProps.put(ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, false);
    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(NUMBER_OF_CHILD_DATACENTERS)
            .numberOfClusters(NUMBER_OF_CLUSTERS)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(2)
            .numberOfRouters(1)
            .replicationFactor(REPLICATION_FACTOR)
            .forkServer(false)
            .parentControllerProperties(controllerProps)
            .childControllerProperties(controllerProps)
            .serverProperties(serverProperties);
    this.multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());
    this.childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    List<VeniceControllerWrapper> parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
    this.parentController = parentControllers.get(0);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testDumpHostHeartbeatLag() {
    final String storeName = Utils.getUniqueString("dumpInfo");
    String parentControllerUrl = parentController.getControllerUrl();
    Schema keySchema = AvroCompatibilityHelper.parse(loadFileAsString("UserKey.avsc"));
    Schema valueSchema = AvroCompatibilityHelper.parse(loadFileAsString("UserValue.avsc"));

    try (ControllerClient parentControllerClient = new ControllerClient(CLUSTER_NAME, parentControllerUrl)) {
      assertCommand(
          parentControllerClient.createNewStore(storeName, "test_owner", keySchema.toString(), valueSchema.toString()));
      UpdateStoreQueryParams updateStoreParams =
          new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setCompressionStrategy(CompressionStrategy.NO_OP)
              .setActiveActiveReplicationEnabled(true)
              .setWriteComputationEnabled(true)
              .setHybridRewindSeconds(86400L)
              .setHybridOffsetLagThreshold(10L);
      ControllerResponse updateStoreResponse =
          parentControllerClient.retryableRequest(5, c -> c.updateStore(storeName, updateStoreParams));
      assertFalse(updateStoreResponse.isError(), "Update store got error: " + updateStoreResponse.getError());

      VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test_push_id", 1000);
      assertEquals(response.getVersion(), 1);
      assertFalse(response.isError(), "Empty push to parent colo should succeed");
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      VeniceClusterWrapper veniceCluster = childDatacenters.get(0).getClusters().get(CLUSTER_NAME);
      VeniceServerWrapper serverWrapper = veniceCluster.getVeniceServers().get(0);

      Map<String, ReplicaHeartbeatInfo> heartbeatInfoMap = serverWrapper.getVeniceServer()
          .getHeartbeatMonitoringService()
          .getHeartbeatInfo(Version.composeKafkaTopic(storeName, 1), -1, false);
      LOGGER.info("Heartbeat Info:\n" + heartbeatInfoMap);
      int totalReplicaCount = heartbeatInfoMap.size();

      heartbeatInfoMap = serverWrapper.getVeniceServer()
          .getHeartbeatMonitoringService()
          .getHeartbeatInfo(Version.composeKafkaTopic(storeName, 1), -1, false);
      LOGGER.info("Heartbeat Info with topic filtering:\n" + heartbeatInfoMap);
      Assert.assertEquals(heartbeatInfoMap.keySet().stream().filter(x -> x.endsWith("dc-0")).count(), 3);
      Assert.assertEquals(
          heartbeatInfoMap.keySet().stream().filter(x -> x.contains("dc-1")).count() * 2,
          heartbeatInfoMap.values().stream().filter(x -> x.getLeaderState().equals("LEADER")).count());

      heartbeatInfoMap = serverWrapper.getVeniceServer()
          .getHeartbeatMonitoringService()
          .getHeartbeatInfo(Version.composeKafkaTopic(storeName, 1), 2, false);
      LOGGER.info("Heartbeat Info with topic/partition filtering:\n" + heartbeatInfoMap);
      Assert.assertTrue(
          heartbeatInfoMap.keySet()
              .stream()
              .allMatch(x -> x.startsWith(Version.composeKafkaTopic(storeName, 1) + "-2")));

      heartbeatInfoMap = serverWrapper.getVeniceServer()
          .getHeartbeatMonitoringService()
          .getHeartbeatInfo(Version.composeKafkaTopic(storeName, 1), 2, false);
      Assert.assertNotEquals(heartbeatInfoMap.size(), totalReplicaCount);

      heartbeatInfoMap = serverWrapper.getVeniceServer()
          .getHeartbeatMonitoringService()
          .getHeartbeatInfo(Version.composeKafkaTopic(storeName, 1), -1, true);
      LOGGER.info("Heartbeat Info with lag filtering:\n" + heartbeatInfoMap);
      Assert.assertTrue(heartbeatInfoMap.isEmpty());

      // Print out for display only.
      String serverUrl = "http://" + serverWrapper.getHost() + ":" + serverWrapper.getPort();

      String[] args = { "--dump-host-heartbeat", "--server-url", serverUrl, "--kafka-topic-name",
          Version.composeKafkaTopic(storeName, 1) };
      try {
        AdminTool.main(args);
      } catch (Exception e) {
        throw new VeniceException(e);
      }
    }
  }
}
