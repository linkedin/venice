package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.OFFLINE_JOB_START_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.AdminTool;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerClientFactory;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class StoreMetadataRecoveryTest {
  private static final int TEST_TIMEOUT = 120 * Time.MS_PER_SECOND;

  private VeniceTwoLayerMultiRegionMultiClusterWrapper twoLayerClusterWrapper;
  private VeniceMultiClusterWrapper multiClusterWrapper;
  private String clusterName;
  private String parentControllerUrl;
  private String parentZKUrl;
  private String parentKafkaUrl;
  private String childControllerUrl;

  @BeforeClass
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties parentControllerProperties = new Properties();
    // Disable topic cleanup since parent and child are sharing the same kafka cluster.
    parentControllerProperties
        .setProperty(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, String.valueOf(Long.MAX_VALUE));
    parentControllerProperties.setProperty(OFFLINE_JOB_START_TIMEOUT_MS, "180000");
    parentControllerProperties.setProperty(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, "false");
    parentControllerProperties.setProperty(CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE, "false");

    Properties serverProperties = new Properties();
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);

    // 1 parent controller, 1 child region, 1 clusters per child region, 2 servers per cluster
    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfClusters(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(2)
            .numberOfRouters(0)
            .replicationFactor(2)
            .forkServer(false)
            .parentControllerProperties(parentControllerProperties)
            .serverProperties(serverProperties);
    twoLayerClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());

    multiClusterWrapper = twoLayerClusterWrapper.getChildRegions().get(0);
    String[] clusterNames = multiClusterWrapper.getClusterNames();
    Arrays.sort(clusterNames);
    clusterName = clusterNames[0]; // venice-cluster0
    parentControllerUrl = twoLayerClusterWrapper.getControllerConnectString();
    parentZKUrl = twoLayerClusterWrapper.getZkServerWrapper().getAddress();
    parentKafkaUrl = twoLayerClusterWrapper.getParentKafkaBrokerWrapper().getAddress();
    childControllerUrl = multiClusterWrapper.getControllerConnectString();

    for (String cluster: clusterNames) {
      try (ControllerClient controllerClient = new ControllerClient(cluster, childControllerUrl)) {
        // Verify the participant store is up and running in child region
        String participantStoreName = VeniceSystemStoreUtils.getParticipantStoreNameForCluster(cluster);
        TestUtils.waitForNonDeterministicPushCompletion(
            Version.composeKafkaTopic(participantStoreName, 1),
            controllerClient,
            5,
            TimeUnit.MINUTES);
      }
    }
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(twoLayerClusterWrapper);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testStoreMetadataRecoveryAfterDeletion() throws Exception {
    // Create two stores and delete one of them
    ControllerClient parentControllerClient =
        ControllerClientFactory.getControllerClient(clusterName, parentControllerUrl, Optional.empty());
    ControllerClient childControllerClient =
        ControllerClientFactory.getControllerClient(clusterName, childControllerUrl, Optional.empty());
    String storeName1 = TestUtils.getUniqueTopicString("store_metadata_recovery");
    String storeName2 = TestUtils.getUniqueTopicString("store_metadata_recovery");
    String keySchemaStr = "\"string\"";
    String simpleValueSchemaStr = "\"string\"";
    String recordValueSchemaStr1 =
        "{\n" + "  \"name\": \"ValueRecord\",\n" + "  \"type\": \"record\",\n" + "  \"fields\": [\n"
            + "  {\"name\": \"string_field\", \"type\": \"string\", \"default\": \"\"}\n" + "  ]\n" + "}";
    String recordValueSchemaStr2 = "{\n" + "  \"name\": \"ValueRecord\",\n" + "  \"type\": \"record\",\n"
        + "  \"fields\": [\n" + "  {\"name\": \"string_field\", \"type\": \"string\", \"default\": \"\"},\n"
        + "  {\"name\": \"int_field\", \"type\": \"int\", \"default\": 10}\n" + "  ]\n" + "}";

    ControllerResponse controllerResponse =
        parentControllerClient.createNewStore(storeName1, "test_owner", keySchemaStr, simpleValueSchemaStr);
    if (controllerResponse.isError()) {
      throw new VeniceException(
          "Failed to create store: " + storeName1 + " with error: " + controllerResponse.getError());
    }

    controllerResponse =
        parentControllerClient.createNewStore(storeName2, "test_owner", keySchemaStr, recordValueSchemaStr1);
    if (controllerResponse.isError()) {
      throw new VeniceException(
          "Failed to create store: " + storeName2 + " with error: " + controllerResponse.getError());
    }
    controllerResponse = parentControllerClient.addValueSchema(storeName2, recordValueSchemaStr2);
    if (controllerResponse.isError()) {
      throw new VeniceException(
          "Failed to add value schema to store: " + storeName2 + " with error: " + controllerResponse.getError());
    }
    controllerResponse = parentControllerClient.updateStore(
        storeName2,
        new UpdateStoreQueryParams().setHybridOffsetLagThreshold(100).setHybridRewindSeconds(100));
    if (controllerResponse.isError()) {
      throw new VeniceException(
          "Failed to update store: " + storeName2 + " with error: " + controllerResponse.getError());
    }
    // Make sure both stores are available in child cluster
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      StoreResponse storeResponse = childControllerClient.getStore(storeName1);
      assertFalse(storeResponse.isError(), storeName1 + " should present in child region");
      storeResponse = childControllerClient.getStore(storeName2);
      assertFalse(storeResponse.isError(), storeName2 + " should present in child region");
    });

    // delete store2
    controllerResponse = parentControllerClient.disableAndDeleteStore(storeName2);
    if (controllerResponse.isError()) {
      throw new VeniceException(
          "Failed to delete store: " + storeName2 + " with error: " + controllerResponse.getError());
    }
    // Make sure both stores are available in child cluster
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      StoreResponse storeResponse = childControllerClient.getStore(storeName2);

      assertTrue(storeResponse.isError(), storeName2 + " shouldn't present in child region");
    });

    // Recover the deleted stores via admin tool
    String[] storeRecovery = { "--recover-store-metadata", "--url", parentControllerUrl, "--venice-zookeeper-url",
        parentZKUrl + "/", "--kafka-bootstrap-servers", parentKafkaUrl, "--store", storeName2, "--repair", "true",
        "--graveyard-clusters", clusterName };
    AdminTool.main(storeRecovery);

    // Make sure the store is recovered with the right config and schema
    StoreResponse storeResponse = parentControllerClient.getStore(storeName2);
    if (storeResponse.isError()) {
      throw new VeniceException("Failed to retrieve store info for store: " + storeName2);
    }
    // Check hybrid config
    assertEquals(storeResponse.getStore().getHybridStoreConfig().getRewindTimeInSeconds(), 100);
    assertEquals(storeResponse.getStore().getHybridStoreConfig().getOffsetLagThresholdToGoOnline(), 100);
    // Verify schemas
    SchemaResponse keySchemaResponse = parentControllerClient.getKeySchema(storeName2);
    assertEquals(keySchemaResponse.getSchemaStr(), keySchemaStr);
    MultiSchemaResponse valueSchemaResponse = parentControllerClient.getAllValueSchema(storeName2);
    MultiSchemaResponse.Schema[] schemas = valueSchemaResponse.getSchemas();
    assertEquals(schemas.length, 2);
    assertEquals(Schema.parse(schemas[0].getSchemaStr()), Schema.parse(recordValueSchemaStr1));
    assertEquals(Schema.parse(schemas[1].getSchemaStr()), Schema.parse(recordValueSchemaStr2));
  }
}
