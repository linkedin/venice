package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.StoreHealthAuditResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.RegionPushDetails;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class OneTouchDataRecoveryTest {
  private static final Logger LOGGER = LogManager.getLogger(OneTouchDataRecoveryTest.class);
  private static final long TEST_TIMEOUT = 120_000;
  private static final int NUMBER_OF_CHILD_DATACENTERS = 2;
  private static final int NUMBER_OF_CLUSTERS = 1;
  private static final int NUMBER_OF_PARENT_CONTROLLERS = 1;
  private static final int NUMBER_OF_CONTROLLERS = 1;
  private static final int NUMBER_OF_SERVERS = 1;
  private static final int NUMBER_OF_ROUTERS = 1;
  private static final int REPLICATION_FACTOR = 1;
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
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
  }

  /**
   * testBatchOnlyDataRecoveryAPIs does the following steps:
   *
   * 1.  Create a new store and push data.
   * 2.  Wait for the push job to be in completed state.
   * 3.  Call 'listStorePushInfo' controller api, and it contains partition details as expected.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testBatchOnlyDataRecoveryAPIs() {
    String storeName = Utils.getUniqueString("oneTouch-dataRecovery-store-batch");
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
                  new UpdateStoreQueryParams().setNativeReplicationEnabled(true).setPartitionCount(1))
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

      // Call listStorePushInfo with isPartitionDetailEnabled set to true.
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

      // Call listStorePushInfo with isPartitionDetailEnabled set to false.
      resp = parentControllerCli.listStorePushInfo(storeName, false);
      LOGGER.info("StoreHealthAuditResponse = {}", resp);
      Assert.assertFalse(resp.isError());
      Assert.assertFalse(resp.getRegionPushDetails().isEmpty());
      for (Map.Entry<String, RegionPushDetails> entry: resp.getRegionPushDetails().entrySet()) {
        Assert.assertTrue(entry.getValue().getPartitionDetails().isEmpty());
      }
    }
  }
}
