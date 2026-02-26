package com.linkedin.venice.endToEnd;

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
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.view.TestView;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.samza.system.SystemProducer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestActiveActiveReplicationWithDelayedLeaderPromotion extends AbstractMultiRegionTest {
  private String clusterName;
  private VeniceClusterWrapper clusterWrapper;
  private ControllerClient parentControllerClient;

  @Override
  protected int getNumberOfRegions() {
    return 1;
  }

  @Override
  protected int getNumberOfServers() {
    return 1;
  }

  @Override
  protected int getReplicationFactor() {
    return 1;
  }

  @Override
  protected Properties getExtraServerProperties() {
    Properties serverProperties = new Properties();
    serverProperties.put(
        CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + DEFAULT_PARENT_DATA_CENTER_REGION_NAME,
        "localhost:" + TestUtils.getFreePort());
    return serverProperties;
  }

  @Override
  @BeforeClass(alwaysRun = true)
  public void setUp() {
    super.setUp();
    clusterName = CLUSTER_NAME;
    clusterWrapper = childDatacenters.get(0).getClusters().get(clusterName);
    parentControllerClient = new ControllerClient(clusterName, getParentControllerUrl());
  }

  @Override
  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    super.cleanUp();
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
