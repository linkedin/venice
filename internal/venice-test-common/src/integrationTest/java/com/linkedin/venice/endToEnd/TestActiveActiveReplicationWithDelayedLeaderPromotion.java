package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.CHILD_DATA_CENTER_KAFKA_URL_PREFIX;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_VALUE_SCHEMA;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.view.TestView;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.samza.system.SystemProducer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestActiveActiveReplicationWithDelayedLeaderPromotion {
  private static final String[] CLUSTER_NAMES =
      IntStream.range(0, 1).mapToObj(i -> "venice-cluster" + i).toArray(String[]::new);

  private List<VeniceMultiClusterWrapper> childDatacenters;
  private List<VeniceControllerWrapper> parentControllers;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private String clusterName;
  private VeniceClusterWrapper clusterWrapper;
  private ControllerClient parentControllerClient;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    Properties serverProperties = new Properties();
    serverProperties.put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, false);
    serverProperties.put(
        CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + DEFAULT_PARENT_DATA_CENTER_REGION_NAME,
        "localhost:" + TestUtils.getFreePort());
    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfClusters(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(1)
            .forkServer(false)
            .serverProperties(serverProperties);
    multiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());

    childDatacenters = multiRegionMultiClusterWrapper.getChildRegions();
    parentControllers = multiRegionMultiClusterWrapper.getParentControllers();
    clusterName = CLUSTER_NAMES[0];
    clusterWrapper = childDatacenters.get(0).getClusters().get(clusterName);

    String parentControllerURLs =
        parentControllers.stream().map(VeniceControllerWrapper::getControllerUrl).collect(Collectors.joining(","));
    parentControllerClient = new ControllerClient(clusterName, parentControllerURLs);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    multiRegionMultiClusterWrapper.close();
    TestView.resetCounters();
  }

  @Test(timeOut = 180 * Time.MS_PER_SECOND)
  public void testLeaderShouldCalculateRewindDuringPromotion() {
    final int keyCount = 10;
    String storeName = Utils.getUniqueString("store");
    assertCommand(
        parentControllerClient.createNewStore(storeName, "test_owner", DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA));
    UpdateStoreQueryParams updateStoreParams =
        new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
            .setPartitionCount(1)
            .setHybridRewindSeconds(10L)
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
    SystemProducer producer =
        IntegrationTestPushUtils.getSamzaProducer(clusterWrapper, storeName, Version.PushType.STREAM);
    int badKeyId = 10000;
    IntegrationTestPushUtils.sendStreamingRecord(producer, storeName, badKeyId, badKeyId);
    Utils.sleep(10000);
    response = parentControllerClient.emptyPush(storeName, "test_push_id_2", 1000);
    assertEquals(response.getVersion(), 2);
    assertFalse(response.isError(), "Empty push to parent colo should succeed");
    TestUtils.waitForNonDeterministicPushCompletion(
        Version.composeKafkaTopic(storeName, 1),
        parentControllerClient,
        30,
        TimeUnit.SECONDS);
    for (int i = 0; i < keyCount; i++) {
      IntegrationTestPushUtils.sendStreamingRecord(producer, storeName, i, 2 * i);
    }
    producer.stop();

    try (AvroGenericStoreClient client = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(clusterWrapper.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, true, () -> {
        for (int i = 0; i < keyCount; i++) {
          assertEquals(client.get(i).get(), 2 * i);
        }
        assertNull(client.get(badKeyId).get());
      });
    }
  }

}
