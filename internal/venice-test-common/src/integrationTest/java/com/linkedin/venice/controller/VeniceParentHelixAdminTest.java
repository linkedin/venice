package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.TERMINAL_STATE_TOPIC_CHECK_DELAY_MS;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicPushCompletion;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.ETLStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class VeniceParentHelixAdminTest {
  private static final long DEFAULT_TEST_TIMEOUT_MS = 60000;
  private VeniceParentHelixAdminTestFixture fixture;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private VeniceClusterWrapper venice;
  private String clusterName;

  @BeforeClass
  public void setUp() {
    fixture = new VeniceParentHelixAdminTestFixture();
    multiRegionMultiClusterWrapper = fixture.getMultiRegionMultiClusterWrapper();
    venice = fixture.getVenice();
    clusterName = fixture.getClusterName();
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(fixture);
  }

  @Test(timeOut = DEFAULT_TEST_TIMEOUT_MS)
  public void testTerminalStateTopicChecker() {
    try (ControllerClient parentControllerClient =
        new ControllerClient(clusterName, multiRegionMultiClusterWrapper.getControllerConnectString())) {
      String storeName = Utils.getUniqueString("testStore");
      assertFalse(
          parentControllerClient.createNewStore(storeName, "test", "\"string\"", "\"string\"").isError(),
          "Failed to create test store");
      // Empty push without checking its push status
      ControllerResponse response =
          parentControllerClient.sendEmptyPushAndWait(storeName, "test-push", 1000, 30 * Time.MS_PER_SECOND);
      assertFalse(response.isError(), "Failed to perform empty push on test store");
      String versionTopic = null;
      if (response instanceof VersionCreationResponse) {
        versionTopic = ((VersionCreationResponse) response).getKafkaTopic();
      } else if (response instanceof JobStatusQueryResponse) {
        versionTopic = Version.composeKafkaTopic(storeName, ((JobStatusQueryResponse) response).getVersion());
      }

      if (versionTopic != null) {
        assertTrue(
            multiRegionMultiClusterWrapper.getParentControllers()
                .get(0)
                .getVeniceAdmin()
                .isTopicTruncated(versionTopic));
      }
    }
  }

  @Test(timeOut = 2 * DEFAULT_TEST_TIMEOUT_MS)
  public void testAddVersion() {
    try (ControllerClient parentControllerClient =
        new ControllerClient(clusterName, multiRegionMultiClusterWrapper.getControllerConnectString())) {
      // Adding store
      String storeName = Utils.getUniqueString("test_store");
      String owner = "test_owner";
      String keySchemaStr = "\"long\"";
      Schema valueSchema = generateSchema(false);
      venice.useControllerClient(childControllerClient -> {
        assertCommand(
            parentControllerClient.createNewStore(storeName, owner, keySchemaStr, valueSchema.toString()),
            "Failed to create store:" + storeName);

        // Configure the store to hybrid
        UpdateStoreQueryParams params = new UpdateStoreQueryParams().setHybridRewindSeconds(600)
            .setHybridOffsetLagThreshold(10000)
            .setNativeReplicationEnabled(true)
            .setActiveActiveReplicationEnabled(true);
        assertCommand(parentControllerClient.updateStore(storeName, params));
        HybridStoreConfig hybridStoreConfig =
            assertCommand(parentControllerClient.getStore(storeName)).getStore().getHybridStoreConfig();
        Assert.assertEquals(hybridStoreConfig.getRewindTimeInSeconds(), 600);
        Assert.assertEquals(hybridStoreConfig.getOffsetLagThresholdToGoOnline(), 10000);
        // Check the store config in Child Colo
        waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
          StoreResponse storeResponseFromChild = assertCommand(childControllerClient.getStore(storeName));
          Assert.assertNotNull(storeResponseFromChild.getStore());
          Assert.assertNotNull(storeResponseFromChild.getStore().getHybridStoreConfig());
          Assert.assertEquals(storeResponseFromChild.getStore().getHybridStoreConfig().getRewindTimeInSeconds(), 600);
        });

        // Test add version without rewind time override
        assertCommand(
            parentControllerClient.requestTopicForWrites(
                storeName,
                1000,
                Version.PushType.BATCH,
                Version.numberBasedDummyPushId(1),
                true,
                true,
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.of("dc-0"),
                false,
                -1));
        // Check version-level rewind time config
        Optional<Version> versionFromParent =
            assertCommand(parentControllerClient.getStore(storeName)).getStore().getVersion(1);
        assertTrue(
            versionFromParent.isPresent()
                && versionFromParent.get().getHybridStoreConfig().getRewindTimeInSeconds() == 600);
        // Validate version-level rewind time config in child
        waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
          Optional<Version> versionFromChild =
              assertCommand(childControllerClient.getStore(storeName)).getStore().getVersion(1);
          assertTrue(
              versionFromChild.isPresent()
                  && versionFromChild.get().getHybridStoreConfig().getRewindTimeInSeconds() == 600);
        });

        // Need to kill the current version since it is not allowed to have multiple ongoing versions.
        assertCommand(parentControllerClient.killOfflinePushJob(Version.composeKafkaTopic(storeName, 1)));
        // Test add version with rewind time override
        assertCommand(
            parentControllerClient.requestTopicForWrites(
                storeName,
                1000,
                Version.PushType.BATCH,
                Version.numberBasedDummyPushId(2),
                true,
                true,
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                false,
                1000));

        // Check version-level config
        versionFromParent = assertCommand(parentControllerClient.getStore(storeName)).getStore().getVersion(2);
        assertTrue(
            versionFromParent.isPresent()
                && versionFromParent.get().getHybridStoreConfig().getRewindTimeInSeconds() == 1000);
        Assert.assertEquals(versionFromParent.get().getRmdVersionId(), 1);

        // Validate version-level config in child
        waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
          Optional<Version> versionFromChild =
              assertCommand(childControllerClient.getStore(storeName)).getStore().getVersion(2);
          assertTrue(
              versionFromChild.isPresent()
                  && versionFromChild.get().getHybridStoreConfig().getRewindTimeInSeconds() == 1000);
          Assert.assertEquals(versionFromChild.get().getRmdVersionId(), 1);
        });

        // Check store level config
        StoreResponse storeResponseFromChild = assertCommand(childControllerClient.getStore(storeName));
        Assert.assertNotNull(storeResponseFromChild.getStore());
        Assert.assertNotNull(storeResponseFromChild.getStore().getHybridStoreConfig());
        Assert.assertEquals(storeResponseFromChild.getStore().getHybridStoreConfig().getRewindTimeInSeconds(), 600);
      });
    }
  }

  @Test(timeOut = DEFAULT_TEST_TIMEOUT_MS * 2)
  public void testResourceCleanupCheckForStoreRecreation() {
    Properties properties = new Properties();
    // Disable topic deletion
    properties.setProperty(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, String.valueOf(Long.MAX_VALUE));
    properties.setProperty(TERMINAL_STATE_TOPIC_CHECK_DELAY_MS, String.valueOf(1000L));
    // Recreation of the same store will fail due to lingering system store resources
    // TODO: Will come up with a solution to make sure system store creation is blocked until previous resources are
    // cleaned up.
    properties.setProperty(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, String.valueOf(false));
    properties.setProperty(CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE, String.valueOf(false));

    VeniceMultiRegionClusterCreateOptions.Builder optionsBuilder =
        new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
            .numberOfClusters(1)
            .numberOfParentControllers(1)
            .numberOfChildControllers(1)
            .numberOfServers(1)
            .numberOfRouters(1)
            .replicationFactor(1)
            .forkServer(false)
            .parentControllerProperties(properties)
            .childControllerProperties(properties);
    try (
        VeniceTwoLayerMultiRegionMultiClusterWrapper twoLayerMultiRegionMultiClusterWrapper =
            ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(optionsBuilder.build());
        ControllerClient parentControllerClient = new ControllerClient(
            twoLayerMultiRegionMultiClusterWrapper.getClusterNames()[0],
            twoLayerMultiRegionMultiClusterWrapper.getControllerConnectString())) {
      String storeName = Utils.getUniqueString("testStore");
      assertFalse(
          parentControllerClient.createNewStore(storeName, "test", "\"string\"", "\"string\"").isError(),
          "Failed to create test store");
      // Trying to create the same store will fail
      assertTrue(
          parentControllerClient.createNewStore(storeName, "test", "\"string\"", "\"string\"").isError(),
          "Trying to create an existing store should fail");
      // Empty push without checking its push status
      ControllerResponse response =
          parentControllerClient.sendEmptyPushAndWait(storeName, "test-push", 1000, 30 * Time.MS_PER_SECOND);
      assertFalse(response.isError(), "Failed to perform empty push on test store");
      String versionTopic = null;
      if (response instanceof VersionCreationResponse) {
        versionTopic = ((VersionCreationResponse) response).getKafkaTopic();
      } else if (response instanceof JobStatusQueryResponse) {
        versionTopic = Version.composeKafkaTopic(storeName, ((JobStatusQueryResponse) response).getVersion());
      }

      if (versionTopic != null) {
        assertTrue(
            multiRegionMultiClusterWrapper.getParentControllers()
                .get(0)
                .getVeniceAdmin()
                .isTopicTruncated(versionTopic));
      }

      assertFalse(parentControllerClient.disableAndDeleteStore(storeName).isError(), "Delete store shouldn't fail");

      ControllerResponse controllerResponse =
          parentControllerClient.createNewStore(storeName, "test", "\"string\"", "\"string\"");
      assertFalse(
          controllerResponse.isError(),
          "Trying to re-create the store with lingering version topics should succeed");

      // Enabling meta system store by triggering an empty push to the corresponding meta system store
      String metaSystemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
      VersionCreationResponse versionCreationResponseForMetaSystemStore =
          parentControllerClient.emptyPush(metaSystemStoreName, "test_meta_system_store_push_1", 10000);
      assertFalse(
          versionCreationResponseForMetaSystemStore.isError(),
          "New version creation for meta system store: " + metaSystemStoreName + " should success, but got error: "
              + versionCreationResponseForMetaSystemStore.getError());
      waitForNonDeterministicPushCompletion(
          versionCreationResponseForMetaSystemStore.getKafkaTopic(),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      // Delete the store and try re-creation.
      TestUtils.assertCommand(parentControllerClient.disableAndDeleteStore(storeName), "Delete store shouldn't fail");

      PubSubBrokerWrapper parentPubSub = twoLayerMultiRegionMultiClusterWrapper.getParentKafkaBrokerWrapper();
      PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
      // Manually create an RT topic in the parent region to simulate its presence for lingering system store resources.
      // This is necessary because RT topics are no longer automatically created for regional system stores such as meta
      // and ps3.
      try (TopicManagerRepository topicManagerRepo = IntegrationTestPushUtils
          .getTopicManagerRepo(PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE, 100, 0l, parentPubSub, pubSubTopicRepository);
          TopicManager topicManager = topicManagerRepo.getLocalTopicManager()) {
        PubSubTopic metaStoreRT = pubSubTopicRepository.getTopic(Utils.composeRealTimeTopic(metaSystemStoreName));
        topicManager.createTopic(metaStoreRT, 1, 1, true);
        TestUtils.waitForNonDeterministicAssertion(
            30,
            TimeUnit.SECONDS,
            () -> assertTrue(topicManager.containsTopic(metaStoreRT)));
      }

      // Re-create the same store right away will fail because of lingering system store resources
      controllerResponse = parentControllerClient.createNewStore(storeName, "test", "\"string\"", "\"string\"");
      assertTrue(
          controllerResponse.isError(),
          "Trying to re-create the store with lingering system store resource should fail");
    }
  }

  @Test(timeOut = DEFAULT_TEST_TIMEOUT_MS)
  public void testHybridAndETLStoreConfig() {
    // Adding store
    String storeName = "test_store";
    String owner = "test_owner";
    String keySchemaStr = "\"long\"";
    String proxyUser = "test_user";
    Schema valueSchema = generateSchema(false);
    try (ControllerClient controllerClient =
        new ControllerClient(clusterName, multiRegionMultiClusterWrapper.getControllerConnectString())) {
      assertCommand(controllerClient.createNewStore(storeName, owner, keySchemaStr, valueSchema.toString()));

      // Configure the store to hybrid
      UpdateStoreQueryParams params =
          new UpdateStoreQueryParams().setHybridRewindSeconds(600).setHybridOffsetLagThreshold(10000);
      assertCommand(controllerClient.updateStore(storeName, params));
      HybridStoreConfig hybridStoreConfig =
          assertCommand(controllerClient.getStore(storeName)).getStore().getHybridStoreConfig();
      Assert.assertEquals(hybridStoreConfig.getRewindTimeInSeconds(), 600);
      Assert.assertEquals(hybridStoreConfig.getOffsetLagThresholdToGoOnline(), 10000);

      // Try to update the hybrid store with different hybrid configs
      params = new UpdateStoreQueryParams().setHybridRewindSeconds(172800);
      assertCommand(controllerClient.updateStore(storeName, params));
      hybridStoreConfig = assertCommand(controllerClient.getStore(storeName)).getStore().getHybridStoreConfig();
      Assert.assertEquals(hybridStoreConfig.getRewindTimeInSeconds(), 172800);
      Assert.assertEquals(hybridStoreConfig.getOffsetLagThresholdToGoOnline(), 10000);

      // test enabling ETL without etl proxy account, expected failure
      params = new UpdateStoreQueryParams();
      params.setRegularVersionETLEnabled(true);
      params.setFutureVersionETLEnabled(true);
      ControllerResponse controllerResponse = controllerClient.updateStore(storeName, params);
      ETLStoreConfig etlStoreConfig =
          assertCommand(controllerClient.getStore(storeName)).getStore().getEtlStoreConfig();
      Assert.assertFalse(etlStoreConfig.isRegularVersionETLEnabled());
      Assert.assertFalse(etlStoreConfig.isFutureVersionETLEnabled());
      Assert.assertTrue(
          controllerResponse.getError()
              .contains("Cannot enable ETL for this store " + "because etled user proxy account is not set"));

      // test enabling ETL with empty proxy account, expected failure
      params = new UpdateStoreQueryParams();
      params.setRegularVersionETLEnabled(true).setEtledProxyUserAccount("");
      params.setFutureVersionETLEnabled(true).setEtledProxyUserAccount("");
      controllerResponse = controllerClient.updateStore(storeName, params);
      etlStoreConfig = assertCommand(controllerClient.getStore(storeName)).getStore().getEtlStoreConfig();
      Assert.assertFalse(etlStoreConfig.isRegularVersionETLEnabled());
      Assert.assertFalse(etlStoreConfig.isFutureVersionETLEnabled());
      Assert.assertTrue(
          controllerResponse.getError()
              .contains("Cannot enable ETL for this store " + "because etled user proxy account is not set"));

      // test enabling ETL with etl proxy account, expected success
      params = new UpdateStoreQueryParams();
      params.setRegularVersionETLEnabled(true).setEtledProxyUserAccount(proxyUser);
      params.setFutureVersionETLEnabled(true).setEtledProxyUserAccount(proxyUser);
      controllerClient.updateStore(storeName, params);
      etlStoreConfig = assertCommand(controllerClient.getStore(storeName)).getStore().getEtlStoreConfig();
      Assert.assertTrue(etlStoreConfig.isRegularVersionETLEnabled());
      Assert.assertTrue(etlStoreConfig.isFutureVersionETLEnabled());

      // set the ETL back to false
      params = new UpdateStoreQueryParams();
      params.setRegularVersionETLEnabled(false);
      params.setFutureVersionETLEnabled(false);
      controllerClient.updateStore(storeName, params);
      etlStoreConfig = assertCommand(controllerClient.getStore(storeName)).getStore().getEtlStoreConfig();
      Assert.assertFalse(etlStoreConfig.isRegularVersionETLEnabled());
      Assert.assertFalse(etlStoreConfig.isFutureVersionETLEnabled());

      // test enabling ETL again without etl proxy account, expected success
      params = new UpdateStoreQueryParams();
      params.setRegularVersionETLEnabled(true);
      params.setFutureVersionETLEnabled(true);
      controllerClient.updateStore(storeName, params);
      etlStoreConfig = assertCommand(controllerClient.getStore(storeName)).getStore().getEtlStoreConfig();
      Assert.assertTrue(etlStoreConfig.isRegularVersionETLEnabled());
      Assert.assertTrue(etlStoreConfig.isFutureVersionETLEnabled());
    }
  }

  private Schema generateSchema(boolean addFieldWithDefaultValue) {
    String schemaStr = "{\"namespace\": \"example.avro\",\n" + " \"type\": \"record\",\n" + " \"name\": \"User\",\n"
        + " \"fields\": [\n" + "      { \"name\": \"id\", \"type\": \"string\", \"default\": \"default_ID\"},\n"
        + "      {\n" + "       \"name\": \"value\",\n" + "       \"type\": [\"null\" , {\n"
        + "           \"type\": \"record\",\n" + "           \"name\": \"ValueRecord\",\n"
        + "           \"fields\" : [\n";
    if (addFieldWithDefaultValue) {
      schemaStr += "{\"name\": \"favorite_color\", \"type\": \"string\", \"default\": \"blue\"}\n";
    } else {
      schemaStr += "{\"name\": \"favorite_number\", \"type\": \"int\", \"default\" : 0}\n";
    }
    schemaStr += "           ]\n" + "      }]," + "       \"default\": null\n" + "    }\n" + " ]\n" + "}";
    return AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr);
  }
}
