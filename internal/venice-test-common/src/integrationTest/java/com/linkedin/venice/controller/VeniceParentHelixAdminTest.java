package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PARENT_EXTERNAL_SUPERSET_SCHEMA_GENERATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.TERMINAL_STATE_TOPIC_CHECK_DELAY_MS;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS;
import static com.linkedin.venice.controller.SchemaConstants.BAD_VALUE_SCHEMA_FOR_WRITE_COMPUTE_V2;
import static com.linkedin.venice.controller.SchemaConstants.VALUE_SCHEMA_FOR_WRITE_COMPUTE_V1;
import static com.linkedin.venice.controller.SchemaConstants.VALUE_SCHEMA_FOR_WRITE_COMPUTE_V3;
import static com.linkedin.venice.controller.SchemaConstants.VALUE_SCHEMA_FOR_WRITE_COMPUTE_V4;
import static com.linkedin.venice.controller.SchemaConstants.VALUE_SCHEMA_FOR_WRITE_COMPUTE_V5;
import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_MB;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicPushCompletion;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.supersetschema.SupersetSchemaGeneratorWithCustomProp;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiRegionClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.ETLStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class VeniceParentHelixAdminTest {
  private static final long DEFAULT_TEST_TIMEOUT_MS = 60000;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private VeniceClusterWrapper venice;
  private String clusterName;

  @BeforeClass
  public void setUp() {
    Utils.thisIsLocalhost();
    multiRegionMultiClusterWrapper = ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(1, 1, 1, 1, 1, 1);
    clusterName = multiRegionMultiClusterWrapper.getClusterNames()[0];
    venice = multiRegionMultiClusterWrapper.getChildRegions().get(0).getClusters().get(clusterName);
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(multiRegionMultiClusterWrapper);
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
        assertEquals(versionFromParent.get().getRmdVersionId(), 1);

        // Validate version-level config in child
        waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
          Optional<Version> versionFromChild =
              assertCommand(childControllerClient.getStore(storeName)).getStore().getVersion(2);
          assertTrue(
              versionFromChild.isPresent()
                  && versionFromChild.get().getHybridStoreConfig().getRewindTimeInSeconds() == 1000);
          assertEquals(versionFromChild.get().getRmdVersionId(), 1);
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

    try (
        VeniceTwoLayerMultiRegionMultiClusterWrapper twoLayerMultiRegionMultiClusterWrapper =
            ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                Optional.of(properties),
                Optional.of(properties),
                Optional.empty());
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
      assertFalse(parentControllerClient.disableAndDeleteStore(storeName).isError(), "Delete store shouldn't fail");
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

  @Test(timeOut = DEFAULT_TEST_TIMEOUT_MS)
  public void testSupersetSchemaWithCustomSupersetSchemaGenerator() throws IOException {
    final String CUSTOM_PROP = "custom_prop";
    // Contains f0, f1
    Schema valueSchemaV1 =
        AvroCompatibilityHelper.parse(TestWriteUtils.loadFileAsString("valueSchema/supersetschemas/ValueV1.avsc"));
    // Contains f2, f3
    Schema valueSchemaV4 =
        AvroCompatibilityHelper.parse(TestWriteUtils.loadFileAsString("valueSchema/supersetschemas/ValueV4.avsc"));
    // Contains f0
    Schema valueSchemaV6 =
        AvroCompatibilityHelper.parse(TestWriteUtils.loadFileAsString("valueSchema/supersetschemas/ValueV6.avsc"));
    Properties properties = new Properties();
    // This cluster setup don't have server, we cannot perform push here.
    properties.setProperty(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, String.valueOf(false));
    properties.setProperty(CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE, String.valueOf(false));
    properties.setProperty(CONTROLLER_PARENT_EXTERNAL_SUPERSET_SCHEMA_GENERATION_ENABLED, String.valueOf(true));
    properties
        .put(VeniceControllerWrapper.SUPERSET_SCHEMA_GENERATOR, new SupersetSchemaGeneratorWithCustomProp(CUSTOM_PROP));

    try (VeniceTwoLayerMultiRegionMultiClusterWrapper twoLayerMultiRegionMultiClusterWrapper =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
            1,
            1,
            1,
            1,
            0,
            0,
            1,
            Optional.of(properties),
            Optional.empty(),
            Optional.empty())) {
      String parentControllerUrl = twoLayerMultiRegionMultiClusterWrapper.getControllerConnectString();
      try (ControllerClient parentControllerClient =
          new ControllerClient(twoLayerMultiRegionMultiClusterWrapper.getClusterNames()[0], parentControllerUrl)) {
        TestUtils.waitForNonDeterministicAssertion(
            30,
            TimeUnit.SECONDS,
            false,
            true,
            () -> parentControllerClient.getLeaderControllerUrl());
        // Create a new store
        String storeName = Utils.getUniqueString("test_store_");
        String owner = "test_owner";
        String keySchemaStr = "\"long\"";
        String valueSchemaStr = valueSchemaV1.toString();
        NewStoreResponse newStoreResponse =
            parentControllerClient.createNewStore(storeName, owner, keySchemaStr, valueSchemaStr);
        Assert.assertNotNull(newStoreResponse);
        Assert.assertFalse(newStoreResponse.isError(), "error in newStoreResponse: " + newStoreResponse.getError());
        // Enable write compute
        ControllerResponse updateStoreResponse = parentControllerClient
            .updateStore(storeName, new UpdateStoreQueryParams().setWriteComputationEnabled(true));
        Assert.assertFalse(updateStoreResponse.isError());

        MultiSchemaResponse schemaResponse = parentControllerClient.getAllValueSchema(storeName);
        Assert.assertNotNull(schemaResponse);
        Assert.assertFalse(schemaResponse.isError(), "error in schemaResponse: " + schemaResponse.getError());
        Assert.assertNotNull(schemaResponse.getSchemas());
        Assert.assertEquals(schemaResponse.getSchemas().length, 1, "There should be one value schema.");

        StoreResponse storeResponse = parentControllerClient.getStore(storeName);
        Assert.assertNotNull(storeResponse);
        Assert.assertFalse(storeResponse.isError(), "error in storeResponse: " + storeResponse.getError());
        Assert.assertNotNull(storeResponse.getStore());
        Assert.assertEquals(
            storeResponse.getStore().getLatestSuperSetValueSchemaId(),
            1,
            "Superset schema ID should be the first schema");

        // Add a new value schema with custom prop
        String customPropValue = "custom_prop_value_for_v2";
        valueSchemaV4.addProp(CUSTOM_PROP, customPropValue);
        SchemaResponse addValueSchemaResponse =
            parentControllerClient.addValueSchema(storeName, valueSchemaV4.toString());
        Assert.assertFalse(addValueSchemaResponse.isError());

        schemaResponse = parentControllerClient.getAllValueSchema(storeName);
        Assert.assertNotNull(schemaResponse);
        Assert.assertFalse(schemaResponse.isError(), "error in schemaResponse: " + schemaResponse.getError());
        Assert.assertNotNull(schemaResponse.getSchemas());
        Assert.assertEquals(schemaResponse.getSchemas().length, 3, "There should be 3 value schemas.");

        // Verify superset schema id
        storeResponse = parentControllerClient.getStore(storeName);
        Assert.assertFalse(storeResponse.isError(), "error in storeResponse: " + storeResponse.getError());
        Assert.assertEquals(
            storeResponse.getStore().getLatestSuperSetValueSchemaId(),
            3,
            "Superset schema ID should be the last schema");

        // Verify whether the superset schema contains the CUSTOM_PROP or not.
        SchemaResponse supersetSchemaResponse = parentControllerClient.getValueSchema(storeName, 3);
        Assert.assertFalse(supersetSchemaResponse.isError(), "error in schemaResponse: " + schemaResponse.getError());
        Schema supersetSchema = AvroCompatibilityHelper.parse(supersetSchemaResponse.getSchemaStr());
        assertEquals(supersetSchema.getProp(CUSTOM_PROP), customPropValue);

        // Register a schema, which is identical to the superset schema, but with a different value for CUSTOM_PROP
        String valueSchemaSameAsSupersetSchemaWithDifferentCustomProp = supersetSchemaResponse.getSchemaStr();
        String newCustomPropValue = "new_custom_prop_value";
        valueSchemaSameAsSupersetSchemaWithDifferentCustomProp =
            valueSchemaSameAsSupersetSchemaWithDifferentCustomProp.replace(customPropValue, newCustomPropValue);
        addValueSchemaResponse =
            parentControllerClient.addValueSchema(storeName, valueSchemaSameAsSupersetSchemaWithDifferentCustomProp);
        Assert.assertFalse(
            addValueSchemaResponse.isError(),
            "error in addValueSchemaResponse: " + addValueSchemaResponse.getError());
        schemaResponse = parentControllerClient.getAllValueSchema(storeName);
        Assert.assertNotNull(schemaResponse);
        Assert.assertFalse(schemaResponse.isError(), "error in schemaResponse: " + schemaResponse.getError());
        Assert.assertNotNull(schemaResponse.getSchemas());
        Assert.assertEquals(schemaResponse.getSchemas().length, 4, "There should be 4 value schemas.");
        storeResponse = parentControllerClient.getStore(storeName);
        Assert.assertFalse(storeResponse.isError(), "error in storeResponse: " + storeResponse.getError());
        Assert.assertEquals(
            storeResponse.getStore().getLatestSuperSetValueSchemaId(),
            4,
            "Superset schema ID should be the last schema");
        supersetSchemaResponse = parentControllerClient.getValueSchema(storeName, 4);
        Assert.assertFalse(supersetSchemaResponse.isError(), "error in schemaResponse: " + schemaResponse.getError());
        supersetSchema = AvroCompatibilityHelper.parse(supersetSchemaResponse.getSchemaStr());
        assertEquals(supersetSchema.getProp(CUSTOM_PROP), newCustomPropValue);

        // Register a schema, which is a subset of current superset schema, but with a different value for CUSTOM_PROP
        Schema newValueSchemaWithSubsetOfFieldsWithDifferentCustomProp =
            AvroCompatibilityHelper.parse(valueSchemaV6.toString());
        String newCustomPropValueForNewValueSchemaWithSubsetOfFields =
            "custom_prop_for_newValueSchemaWithSubsetOfFieldsWithDifferentCustomProp";
        newValueSchemaWithSubsetOfFieldsWithDifferentCustomProp
            .addProp(CUSTOM_PROP, newCustomPropValueForNewValueSchemaWithSubsetOfFields);
        addValueSchemaResponse = parentControllerClient
            .addValueSchema(storeName, newValueSchemaWithSubsetOfFieldsWithDifferentCustomProp.toString());
        Assert.assertFalse(
            addValueSchemaResponse.isError(),
            "error in addValueSchemaResponse: " + addValueSchemaResponse.getError());
        schemaResponse = parentControllerClient.getAllValueSchema(storeName);
        Assert.assertNotNull(schemaResponse);
        Assert.assertFalse(schemaResponse.isError(), "error in schemaResponse: " + schemaResponse.getError());
        Assert.assertNotNull(schemaResponse.getSchemas());
        Assert.assertEquals(schemaResponse.getSchemas().length, 6, "There should be 4 value schemas.");
        storeResponse = parentControllerClient.getStore(storeName);
        Assert.assertFalse(storeResponse.isError(), "error in storeResponse: " + storeResponse.getError());
        Assert.assertEquals(
            storeResponse.getStore().getLatestSuperSetValueSchemaId(),
            6,
            "Superset schema ID should be the last schema");
        supersetSchemaResponse = parentControllerClient.getValueSchema(storeName, 6);
        Assert.assertFalse(supersetSchemaResponse.isError(), "error in schemaResponse: " + schemaResponse.getError());
        supersetSchema = AvroCompatibilityHelper.parse(supersetSchemaResponse.getSchemaStr());
        assertEquals(supersetSchema.getProp(CUSTOM_PROP), newCustomPropValueForNewValueSchemaWithSubsetOfFields);
        assertNotNull(supersetSchema.getField("f0"));
        assertNotNull(supersetSchema.getField("f1"));
        assertNotNull(supersetSchema.getField("f2"));
        assertNotNull(supersetSchema.getField("f3"));
      }
    }
  }

  @DataProvider(name = "CONTROLLER_SSL_SUPERSET_SCHEMA_GENERATOR")
  public static Object[][] controllerSSLAndSupersetSchemaGenerator() {
    return new Object[][] { new Object[] { true, true }, new Object[] { false, false } };
  }

  @Test(dataProvider = "CONTROLLER_SSL_SUPERSET_SCHEMA_GENERATOR", timeOut = DEFAULT_TEST_TIMEOUT_MS * 10)
  public void testStoreMetaDataUpdateFromParentToChildController(
      boolean isControllerSslEnabled,
      boolean isSupersetSchemaGeneratorEnabled) throws IOException {
    Properties parentControllerProps = new Properties();
    // This cluster setup don't have server, we cannot perform push here.
    parentControllerProps.setProperty(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, String.valueOf(false));
    parentControllerProps
        .setProperty(CONTROLLER_AUTO_MATERIALIZE_DAVINCI_PUSH_STATUS_SYSTEM_STORE, String.valueOf(false));
    if (isSupersetSchemaGeneratorEnabled) {
      parentControllerProps
          .setProperty(CONTROLLER_PARENT_EXTERNAL_SUPERSET_SCHEMA_GENERATION_ENABLED, String.valueOf(true));
      parentControllerProps.put(
          VeniceControllerWrapper.SUPERSET_SCHEMA_GENERATOR,
          new SupersetSchemaGeneratorWithCustomProp("test_prop"));
    }

    try (VeniceTwoLayerMultiRegionMultiClusterWrapper venice =
        ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper(
            new VeniceMultiRegionClusterCreateOptions.Builder().numberOfRegions(1)
                .numberOfClusters(1)
                .numberOfParentControllers(1)
                .numberOfChildControllers(1)
                .numberOfServers(0)
                .numberOfRouters(0)
                .replicationFactor(1)
                .parentControllerProperties(parentControllerProps)
                .sslToKafka(isControllerSslEnabled)
                .build())) {
      String childControllerUrl = venice.getChildRegions().get(0).getControllerConnectString();
      String parentControllerUrl = venice.getControllerConnectString();
      Optional<SSLFactory> sslFactory =
          isControllerSslEnabled ? Optional.of(SslUtils.getVeniceLocalSslFactory()) : Optional.empty();
      try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerUrl, sslFactory);
          ControllerClient childControllerClient = new ControllerClient(clusterName, childControllerUrl, sslFactory)) {
        testBadDefaultSchemaValidation(parentControllerClient);
        testBackupVersionRetentionUpdate(parentControllerClient, childControllerClient);
        testLatestSupersetSchemaIdUpdate(parentControllerClient, childControllerClient);
        testSuperSetSchemaGen(parentControllerClient);
        testSuperSetSchemaGenWithSameUpcomingSchema(parentControllerClient);
        testSupersetSchemaRegistration(parentControllerClient);
        testAddValueSchemaDocUpdate(parentControllerClient);
        testAddBadValueSchema(parentControllerClient);
        testWriteComputeSchemaAutoGeneration(parentControllerClient);
        testWriteComputeSchemaEnable(parentControllerClient);
        testWriteComputeSchemaAutoGenerationFailure(parentControllerClient);
        testSupersetSchemaGenerationWithUpdateDefaultValue(parentControllerClient);
        testUpdateConfigs(parentControllerClient, childControllerClient);
      }
    }
  }

  private void testBackupVersionRetentionUpdate(
      ControllerClient parentControllerClient,
      ControllerClient childControllerClient) {
    String storeName = Utils.getUniqueString("test_store_");
    String owner = "test_owner";
    String keySchemaStr = "\"long\"";
    String valueSchemaStr = "\"string\"";
    NewStoreResponse newStoreResponse =
        parentControllerClient.createNewStore(storeName, owner, keySchemaStr, valueSchemaStr);
    Assert.assertNotNull(newStoreResponse);
    Assert.assertFalse(newStoreResponse.isError(), "error in newStoreResponse: " + newStoreResponse.getError());
    long backupVersionRetentionMs = TimeUnit.HOURS.toMillis(1);
    ControllerResponse controllerResponse = parentControllerClient.updateStore(
        storeName,
        new UpdateStoreQueryParams().setBackupVersionRetentionMs(backupVersionRetentionMs).setReadQuotaInCU(10000));
    Assert.assertNotNull(controllerResponse);
    Assert
        .assertFalse(controllerResponse.isError(), "Error in store update response: " + controllerResponse.getError());

    // Verify the update in Parent Controller
    StoreResponse storeResponseFromParentController = parentControllerClient.getStore(storeName);
    Assert.assertFalse(
        storeResponseFromParentController.isError(),
        "Error in store response from Parent Controller: " + storeResponseFromParentController.getError());
    Assert.assertEquals(
        storeResponseFromParentController.getStore().getBackupVersionRetentionMs(),
        backupVersionRetentionMs);
    Assert.assertEquals(storeResponseFromParentController.getStore().getReadQuotaInCU(), 10000);
    // Verify the update in Child Controller
    waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      StoreResponse storeResponseFromChildController = childControllerClient.getStore(storeName);
      Assert.assertFalse(
          storeResponseFromChildController.isError(),
          "Error in store response from Child Controller: " + storeResponseFromChildController.getError());
      Assert.assertEquals(
          storeResponseFromChildController.getStore().getBackupVersionRetentionMs(),
          backupVersionRetentionMs);
      Assert.assertEquals(storeResponseFromChildController.getStore().getReadQuotaInCU(), 10000);
    });
  }

  private void testBadDefaultSchemaValidation(ControllerClient parentControllerClient) {
    String storeName = Utils.getUniqueString("test_store_");
    String owner = "test_owner";
    String keySchemaStr = "\"long\"";
    String valueSchemaStr =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"}]}";
    String valueSchemaStrWithBadDefault =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"salary\",\"type\":\"float\",\"default\":123}]}";

    NewStoreResponse newStoreResponse =
        parentControllerClient.createNewStore(storeName, owner, keySchemaStr, valueSchemaStrWithBadDefault);
    Assert.assertTrue(newStoreResponse.isError());
    Assert.assertTrue(
        newStoreResponse.getError()
            .contains("Invalid default for field KeyRecord.salary: 123 (a IntNode) not a \"float\""));
    newStoreResponse = parentControllerClient.createNewStore(storeName, owner, keySchemaStr, valueSchemaStr);
    Assert.assertFalse(newStoreResponse.isError());
    SchemaResponse addSchemaResponse = parentControllerClient.addValueSchema(storeName, valueSchemaStrWithBadDefault);
    Assert.assertTrue(addSchemaResponse.isError());
    Assert.assertTrue(
        addSchemaResponse.getError()
            .contains("Invalid default for field KeyRecord.salary: 123 (a IntNode) not a \"float\""));
  }

  private void testLatestSupersetSchemaIdUpdate(
      ControllerClient parentControllerClient,
      ControllerClient childControllerClient) {
    String storeName = Utils.getUniqueString("test_store_");
    String owner = "test_owner";
    String keySchemaStr = "\"long\"";
    String valueSchemaStr = "\"string\"";
    NewStoreResponse newStoreResponse =
        parentControllerClient.createNewStore(storeName, owner, keySchemaStr, valueSchemaStr);
    Assert.assertNotNull(newStoreResponse);
    Assert.assertFalse(newStoreResponse.isError(), "error in newStoreResponse: " + newStoreResponse.getError());
    Map<Integer, Boolean> schemaIdToStatusMap = new HashMap<>();
    schemaIdToStatusMap.put(1, true);
    schemaIdToStatusMap.put(2, false);
    schemaIdToStatusMap.put(-1, true);
    for (Map.Entry<Integer, Boolean> entry: schemaIdToStatusMap.entrySet()) {
      int schemaId = entry.getKey();
      boolean result = entry.getValue();
      ControllerResponse controllerResponse = parentControllerClient
          .updateStore(storeName, new UpdateStoreQueryParams().setLatestSupersetSchemaId(schemaId));
      Assert.assertNotNull(controllerResponse);
      if (!result) {
        Assert.assertTrue(controllerResponse.isError(), "There should be an error when setting up invalid schema id");
      } else {
        Assert.assertFalse(
            controllerResponse.isError(),
            "Error in store update response: " + controllerResponse.getError());

        // Verify the update in Parent Controller
        StoreResponse storeResponseFromParentController = parentControllerClient.getStore(storeName);
        Assert.assertFalse(
            storeResponseFromParentController.isError(),
            "Error in store response from Parent Controller: " + storeResponseFromParentController.getError());
        Assert.assertEquals(storeResponseFromParentController.getStore().getLatestSuperSetValueSchemaId(), schemaId);
        // Verify the update in Child Controller
        waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          StoreResponse storeResponseFromChildController = childControllerClient.getStore(storeName);
          Assert.assertFalse(
              storeResponseFromChildController.isError(),
              "Error in store response from Child Controller: " + storeResponseFromChildController.getError());
          Assert.assertEquals(storeResponseFromChildController.getStore().getLatestSuperSetValueSchemaId(), schemaId);
        });
      }
    }

  }

  private void testSuperSetSchemaGen(ControllerClient parentControllerClient) {
    // Adding store
    String storeName = Utils.getUniqueString("test_store");
    String owner = "test_owner";
    String keySchemaStr = "\"long\"";
    Schema valueSchemaV1 = generateSchema(false);

    NewStoreResponse newStoreResponse =
        parentControllerClient.createNewStore(storeName, owner, keySchemaStr, valueSchemaV1.toString());
    Assert.assertNotNull(newStoreResponse);
    Assert.assertFalse(newStoreResponse.isError(), "error in newStoreResponse: " + newStoreResponse.getError());

    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setReadComputationEnabled(true);
    params.setAutoSchemaPushJobEnabled(true);
    ControllerResponse updateStoreResponse = parentControllerClient.updateStore(storeName, params);
    Assert.assertNotNull(updateStoreResponse);
    Assert
        .assertFalse(updateStoreResponse.isError(), "error in updateStoreResponse: " + updateStoreResponse.getError());

    Schema valueSchemaV2 = generateSchema(true);
    SchemaResponse addSchemaResponse = parentControllerClient.addValueSchema(storeName, valueSchemaV2.toString());
    Assert.assertNotNull(addSchemaResponse);
    Assert.assertFalse(addSchemaResponse.isError(), "error in addSchemaResponse: " + addSchemaResponse.getError());

    MultiSchemaResponse schemaResponse = parentControllerClient.getAllValueSchema(storeName);
    Assert.assertNotNull(schemaResponse);
    Assert.assertFalse(schemaResponse.isError(), "error in schemaResponse: " + schemaResponse.getError());
    Assert.assertNotNull(schemaResponse.getSchemas());
    Assert.assertEquals(
        schemaResponse.getSchemas().length,
        3,
        "2 value schemas + 1 superset schema. So should expect a total of 3 schemas.");

    StoreResponse storeResponse = parentControllerClient.getStore(storeName);
    Assert.assertNotNull(storeResponse);
    Assert.assertFalse(storeResponse.isError(), "error in storeResponse: " + storeResponse.getError());
    Assert.assertNotNull(storeResponse.getStore());
    Assert.assertEquals(
        storeResponse.getStore().getLatestSuperSetValueSchemaId(),
        3,
        "Superset schema ID should be the latest schema ID among schema ID 1, 2, 3");

    Schema valueSchemaV3 = generateSuperSetSchemaNewField();
    addSchemaResponse = parentControllerClient.addValueSchema(storeName, valueSchemaV3.toString());
    Assert.assertNotNull(addSchemaResponse);
    Assert.assertFalse(addSchemaResponse.isError(), "error in addSchemaResponse: " + addSchemaResponse.getError());

    schemaResponse = parentControllerClient.getAllValueSchema(storeName);
    Assert.assertNotNull(schemaResponse);
    Assert.assertFalse(schemaResponse.isError(), "error in schemaResponse: " + schemaResponse.getError());
    Assert.assertNotNull(schemaResponse.getSchemas());
    Assert.assertEquals(schemaResponse.getSchemas().length, 4);

    storeResponse = parentControllerClient.getStore(storeName);
    Assert.assertEquals(
        storeResponse.getStore().getLatestSuperSetValueSchemaId(),
        4,
        "Superset schema ID should be the same as the latest value schema because the latest value schema should "
            + "be the superset schema at this point.");
  }

  private void testSupersetSchemaRegistration(ControllerClient parentControllerClient) throws IOException {
    String storeName = Utils.getUniqueString("test_store");
    String owner = "test_owner";
    String keySchemaStr = "\"long\"";
    Schema valueSchemaV1 =
        AvroCompatibilityHelper.parse(TestWriteUtils.loadFileAsString("valueSchema/supersetschemas/ValueV1.avsc"));
    Schema valueSchemaV2 =
        AvroCompatibilityHelper.parse(TestWriteUtils.loadFileAsString("valueSchema/supersetschemas/ValueV2.avsc"));
    Schema valueSchemaV3 =
        AvroCompatibilityHelper.parse(TestWriteUtils.loadFileAsString("valueSchema/supersetschemas/ValueV3.avsc"));
    Schema valueSchemaV4 =
        AvroCompatibilityHelper.parse(TestWriteUtils.loadFileAsString("valueSchema/supersetschemas/ValueV4.avsc"));
    Schema valueSchemaV5 =
        AvroCompatibilityHelper.parse(TestWriteUtils.loadFileAsString("valueSchema/supersetschemas/ValueV5.avsc"));

    NewStoreResponse newStoreResponse =
        parentControllerClient.createNewStore(storeName, owner, keySchemaStr, valueSchemaV1.toString());
    Assert.assertNotNull(newStoreResponse);
    Assert.assertFalse(newStoreResponse.isError(), "error in newStoreResponse: " + newStoreResponse.getError());

    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setReadComputationEnabled(true);
    params.setAutoSchemaPushJobEnabled(true);
    ControllerResponse updateStoreResponse = parentControllerClient.updateStore(storeName, params);
    Assert.assertNotNull(updateStoreResponse);
    Assert
        .assertFalse(updateStoreResponse.isError(), "error in updateStoreResponse: " + updateStoreResponse.getError());
    validateAllValueSchemas(parentControllerClient, storeName, 1, "There should be one value schema.");
    int supersetSchemaID = parentControllerClient.getStore(storeName).getStore().getLatestSuperSetValueSchemaId();
    Assert.assertEquals(supersetSchemaID, 1, "The first value schema ID should be the superset value schema ID.");

    addValueSchema(parentControllerClient, valueSchemaV2, storeName);
    supersetSchemaID = parentControllerClient.getStore(storeName).getStore().getLatestSuperSetValueSchemaId();
    Assert.assertEquals(supersetSchemaID, 2);

    addValueSchema(parentControllerClient, valueSchemaV3, storeName);
    validateAllValueSchemas(parentControllerClient, storeName, 4, "3 value schemas + 1 superset schema.");

    supersetSchemaID = parentControllerClient.getStore(storeName).getStore().getLatestSuperSetValueSchemaId();
    Assert.assertEquals(supersetSchemaID, 4);

    addValueSchema(parentControllerClient, valueSchemaV4, storeName);
    addValueSchema(parentControllerClient, valueSchemaV5, storeName);
    validateAllValueSchemas(parentControllerClient, storeName, 6, "5 value schemas + 1 superset schema.");

    supersetSchemaID = parentControllerClient.getStore(storeName).getStore().getLatestSuperSetValueSchemaId();
    Assert.assertEquals(supersetSchemaID, 4, "Got unexpected superset schema ID: " + supersetSchemaID);

    // Superset schema should contain all fields.
    Schema supersetSchema = AvroCompatibilityHelper
        .parse(parentControllerClient.getValueSchema(storeName, supersetSchemaID).getSchemaStr());
    assertNotNull(supersetSchema.getField("f0"));
    assertNotNull(supersetSchema.getField("f1"));
    assertNotNull(supersetSchema.getField("f2"));
    assertNotNull(supersetSchema.getField("f3"));
    assertNotNull(supersetSchema.getField("f4"));
  }

  private void validateAllValueSchemas(
      ControllerClient controllerClient,
      String storeName,
      int expectedValueSchemaCount,
      String expectationReason) {
    MultiSchemaResponse schemaResponse = controllerClient.getAllValueSchema(storeName);
    Assert.assertNotNull(schemaResponse);
    Assert.assertFalse(schemaResponse.isError(), "error in schemaResponse: " + schemaResponse.getError());
    Assert.assertNotNull(schemaResponse.getSchemas());
    Assert.assertEquals(schemaResponse.getSchemas().length, expectedValueSchemaCount, expectationReason);
  }

  private void addValueSchema(ControllerClient parentControllerClient, Schema newValueSchema, String storeName) {
    SchemaResponse addSchemaResponse = parentControllerClient.addValueSchema(storeName, newValueSchema.toString());
    Assert.assertNotNull(addSchemaResponse);
    Assert.assertFalse(addSchemaResponse.isError(), "error in addSchemaResponse: " + addSchemaResponse.getError());
  }

  private void testSuperSetSchemaGenWithSameUpcomingSchema(ControllerClient parentControllerClient) {
    // Adding store
    String storeName = Utils.getUniqueString("test_store");
    String owner = "test_owner";
    String keySchemaStr = "\"long\"";
    Schema valueSchema = generateSchema(false);

    parentControllerClient.createNewStore(storeName, owner, keySchemaStr, valueSchema.toString());

    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setReadComputationEnabled(true);
    params.setAutoSchemaPushJobEnabled(true);
    parentControllerClient.updateStore(storeName, params);

    valueSchema = generateSuperSetSchema();
    parentControllerClient.addValueSchema(storeName, valueSchema.toString());

    MultiSchemaResponse schemaResponse = parentControllerClient.getAllValueSchema(storeName);

    Assert.assertEquals(schemaResponse.getSchemas().length, 2);
    StoreResponse storeResponse = parentControllerClient.getStore(storeName);
    Assert.assertEquals(
        storeResponse.getStore().getLatestSuperSetValueSchemaId(),
        2,
        "Second schema should be the superset schema.");
  }

  private void testAddValueSchemaDocUpdate(ControllerClient parentControllerClient) {
    // Adding store
    String storeName = Utils.getUniqueString("test_store");
    String owner = "test_owner";
    String keySchemaStr = "\"long\"";
    String schemaStr =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"id1\",\"type\":\"double\", \"default\": 0.0}]}";
    String schemaStrDoc =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field updated\", \"default\": \"default name\"},{\"name\":\"id1\",\"type\":\"double\",\"default\": 0.0}]}";
    Schema valueSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr);
    parentControllerClient.createNewStore(storeName, owner, keySchemaStr, valueSchema.toString());
    valueSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStrDoc);
    parentControllerClient.addValueSchema(storeName, valueSchema.toString());
    MultiSchemaResponse schemaResponse = parentControllerClient.getAllValueSchema(storeName);
    Assert.assertEquals(schemaResponse.getSchemas().length, 2);
  }

  private void testUpdateConfigs(ControllerClient parentControllerClient, ControllerClient childControllerClient) {
    testUpdateCompactionLag(parentControllerClient, childControllerClient);
    testUpdateMaxRecordSize(parentControllerClient, childControllerClient);
    testUpdateBlobTransfer(parentControllerClient, childControllerClient);
    testUpdateNearlineProducerConfig(parentControllerClient, childControllerClient);
    testUpdateTargetSwapRegion(parentControllerClient, childControllerClient);
  }

  /**
   * Base test flow for updating store configurations by updating the store metadata from parent to child controller.
   * @param parentControllerClient The parent controller client which will perform the update.
   * @param childControllerClient The child controller client which will receive the update.
   * @param paramsConsumer Used to create the UpdateStoreQueryParams to update the configurations for the store.
   * @param responseConsumer Used to validate the configurations have been updated correctly for both parent and child.
   */
  private void testUpdateConfig(
      ControllerClient parentControllerClient,
      ControllerClient childControllerClient,
      Consumer<UpdateStoreQueryParams> paramsConsumer,
      Consumer<StoreResponse> responseConsumer) {
    // Step 1. Create a store
    String storeName = Utils.getUniqueString("test_store");
    String owner = "test_owner";
    String keySchemaStr = "\"long\"";
    String valueSchemaStr = generateSchema(false).toString();
    parentControllerClient.createNewStore(storeName, owner, keySchemaStr, valueSchemaStr);

    // Step 2. Update the store configurations
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    paramsConsumer.accept(params);
    parentControllerClient.updateStore(storeName, params);

    // Step 3. Validate the configurations have been updated correctly for the parent controller
    StoreResponse parentStoreResponse = parentControllerClient.getStore(storeName);
    responseConsumer.accept(parentStoreResponse);

    // Step 4. Validate the configurations have been updated correctly for the child controller
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      StoreResponse childStoreResponse = childControllerClient.getStore(storeName);
      responseConsumer.accept(childStoreResponse);
    });
  }

  private void testUpdateTargetSwapRegion(ControllerClient parentClient, ControllerClient childClient) {
    final String region = "prod";
    final int waitTime = 100;
    final boolean isDavinci = false;
    Consumer<UpdateStoreQueryParams> paramsConsumer = params -> {
      params.setTargetRegionSwap(region);
      params.setTargetRegionSwapWaitTime(waitTime);
      params.setIsDavinciHeartbeatReported(isDavinci);
    };
    Consumer<StoreResponse> responseConsumer = response -> {
      Assert.assertEquals(response.getStore().getTargetRegionSwap(), region);
      Assert.assertEquals(response.getStore().getTargetRegionSwapWaitTime(), waitTime);
      Assert.assertEquals(response.getStore().getIsDavinciHeartbeatReported(), isDavinci);
    };
    testUpdateConfig(parentClient, childClient, paramsConsumer, responseConsumer);
  }

  private void testUpdateCompactionLag(ControllerClient parentClient, ControllerClient childClient) {
    final long expectedMinCompactionLagSeconds = 100;
    final long expectedMaxCompactionLagSeconds = 200;
    Consumer<UpdateStoreQueryParams> paramsConsumer = params -> {
      params.setMinCompactionLagSeconds(expectedMinCompactionLagSeconds);
      params.setMaxCompactionLagSeconds(expectedMaxCompactionLagSeconds);
    };
    Consumer<StoreResponse> responseConsumer = response -> {
      Assert.assertEquals(response.getStore().getMinCompactionLagSeconds(), expectedMinCompactionLagSeconds);
      Assert.assertEquals(response.getStore().getMaxCompactionLagSeconds(), expectedMaxCompactionLagSeconds);
    };
    testUpdateConfig(parentClient, childClient, paramsConsumer, responseConsumer);
  }

  private void testUpdateNearlineProducerConfig(ControllerClient parentClient, ControllerClient childClient) {
    final boolean nearlineProducerCompressionEnabled = false;
    final int nearlineProducerCountPerWriter = 10;
    Consumer<UpdateStoreQueryParams> paramsConsumer = params -> {
      params.setNearlineProducerCompressionEnabled(nearlineProducerCompressionEnabled);
      params.setNearlineProducerCountPerWriter(nearlineProducerCountPerWriter);
    };
    Consumer<StoreResponse> responseConsumer = response -> {
      Assert
          .assertEquals(response.getStore().isNearlineProducerCompressionEnabled(), nearlineProducerCompressionEnabled);
      Assert.assertEquals(response.getStore().getNearlineProducerCountPerWriter(), nearlineProducerCountPerWriter);
    };
    testUpdateConfig(parentClient, childClient, paramsConsumer, responseConsumer);
  }

  private void testUpdateMaxRecordSize(ControllerClient parentClient, ControllerClient childClient) {
    final int expectedMaxRecordSizeBytes = 7 * BYTES_PER_MB;
    testUpdateConfig(
        parentClient,
        childClient,
        params -> params.setMaxRecordSizeBytes(expectedMaxRecordSizeBytes),
        response -> Assert.assertEquals(response.getStore().getMaxRecordSizeBytes(), expectedMaxRecordSizeBytes));
    testUpdateConfig(
        parentClient,
        childClient,
        params -> params.setMaxNearlineRecordSizeBytes(expectedMaxRecordSizeBytes),
        response -> Assert
            .assertEquals(response.getStore().getMaxNearlineRecordSizeBytes(), expectedMaxRecordSizeBytes));
  }

  private void testUpdateBlobTransfer(ControllerClient parentClient, ControllerClient childClient) {
    testUpdateConfig(
        parentClient,
        childClient,
        params -> params.setBlobTransferEnabled(true),
        response -> Assert.assertTrue(response.getStore().isBlobTransferEnabled()));
  }

  private void testAddBadValueSchema(ControllerClient parentControllerClient) {
    // Adding store
    String storeName = Utils.getUniqueString("test_store");
    String owner = "test_owner";
    String keySchemaStr = "\"long\"";
    String schemaStr =
        "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"name\",\"type\":\"string\", \"default\": \"default\"},{\"name\":\"kind\",\"type\":{\"type\":\"enum\",\"name\":\"Kind\",\"symbols\":[\"ONE\",\"TWO\"], \"default\": \"ONE\"}}]}";
    String schemaStr1 =
        "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"name\",\"type\":\"string\", \"default\": \"default\"},{\"name\":\"kind\",\"type\":{\"type\":\"enum\",\"name\":\"Kind\",\"symbols\":[\"ONE\",\"FOUR\",\"THREE\"], \"default\": \"ONE\"}}]}";
    Schema valueSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr);
    parentControllerClient.createNewStore(storeName, owner, keySchemaStr, valueSchema.toString());
    valueSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr1);
    parentControllerClient.addValueSchema(storeName, valueSchema.toString());
    SchemaResponse schemaResponse = parentControllerClient.addValueSchema(storeName, valueSchema.toString());
    Assert.assertTrue(schemaResponse.isError());
  }

  private void testWriteComputeSchemaAutoGenerationFailure(ControllerClient parentControllerClient) {
    String storeName = Utils.getUniqueString("test_store");
    String owner = "test_owner";
    String keySchemaStr = "\"long\"";
    Schema valueSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(VALUE_SCHEMA_FOR_WRITE_COMPUTE_V4);

    // Step 1. Create a store
    parentControllerClient.createNewStore(storeName, owner, keySchemaStr, valueSchema.toString());
    MultiSchemaResponse valueAndWriteComputeSchemaResponse =
        parentControllerClient.getAllValueAndDerivedSchema(storeName);
    MultiSchemaResponse.Schema[] registeredSchemas = valueAndWriteComputeSchemaResponse.getSchemas();
    Assert.assertEquals(registeredSchemas.length, 1);
    MultiSchemaResponse.Schema registeredSchema = registeredSchemas[0];
    Assert.assertEquals(registeredSchema.getSchemaStr(), valueSchema.toString());
    Assert.assertFalse(registeredSchema.isDerivedSchema()); // No write compute schema yet.

    // Step 2. Update this store to enable write compute and expect it to fail.
    validateEnablingWriteComputeFailed(storeName, parentControllerClient);

    // Step 3. Add a value schema which has a new field with default value. Expect enabling Write Compute to still fail.
    SchemaResponse schemaResponse = parentControllerClient.addValueSchema(storeName, VALUE_SCHEMA_FOR_WRITE_COMPUTE_V5);
    Assert.assertFalse(schemaResponse.isError(), "Users should be able to continue to add value schemas");
    validateEnablingWriteComputeFailed(storeName, parentControllerClient); // Still cannot enable Write Compute.
  }

  private void validateEnablingWriteComputeFailed(String storeName, ControllerClient parentControllerClient) {
    UpdateStoreQueryParams updateStoreQueryParams = new UpdateStoreQueryParams();
    updateStoreQueryParams.setWriteComputationEnabled(true);
    ControllerResponse response = parentControllerClient.updateStore(storeName, updateStoreQueryParams);
    Assert.assertTrue(
        response.isError(),
        "Enabling Write Compute should fail because the value schema has a field that does not have default value.");
    final String expectedErrorMsg = "top level field probably missing defaults";
    Assert.assertTrue(response.getError().contains(expectedErrorMsg));
  }

  private void testWriteComputeSchemaAutoGeneration(ControllerClient parentControllerClient) {
    String storeName = Utils.getUniqueString("test_store");
    String owner = "test_owner";
    String keySchemaStr = "\"long\"";
    Schema valueSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(VALUE_SCHEMA_FOR_WRITE_COMPUTE_V1);

    // Step 1. Create a store
    parentControllerClient.createNewStore(storeName, owner, keySchemaStr, valueSchema.toString());
    MultiSchemaResponse valueAndWriteComputeSchemaResponse =
        parentControllerClient.getAllValueAndDerivedSchema(storeName);
    MultiSchemaResponse.Schema[] registeredSchemas = valueAndWriteComputeSchemaResponse.getSchemas();
    Assert.assertEquals(registeredSchemas.length, 1);
    MultiSchemaResponse.Schema registeredSchema = registeredSchemas[0];
    Assert.assertEquals(registeredSchema.getSchemaStr(), valueSchema.toString());
    Assert.assertFalse(registeredSchema.isDerivedSchema()); // No write compute schema yet.

    // Step 2. Update this store to enable write compute.
    UpdateStoreQueryParams updateStoreQueryParams = new UpdateStoreQueryParams();
    updateStoreQueryParams.setWriteComputationEnabled(true);
    parentControllerClient.updateStore(storeName, updateStoreQueryParams);

    // Step 3. Get value schema and write compute schema generated by the controller.
    registeredSchemas = parentControllerClient.getAllValueAndDerivedSchema(storeName).getSchemas();
    Assert.assertEquals(registeredSchemas.length, 2);
    List<MultiSchemaResponse.Schema> registeredWriteComputeSchema = getWriteComputeSchemaStrs(registeredSchemas);
    Assert.assertEquals(registeredWriteComputeSchema.size(), 1);
    Assert.assertEquals(registeredWriteComputeSchema.get(0).getId(), 1);

    // Note that currently there is only one WriteComputeSchemaConverter implementation so that we know the Venice
    // controller
    // must have used this WriteComputeSchemaConverter impl to generate and register Write Compute schema.
    final WriteComputeSchemaConverter writeComputeSchemaConverter = WriteComputeSchemaConverter.getInstance();
    Schema expectedWriteComputeSchema = writeComputeSchemaConverter.convertFromValueRecordSchema(valueSchema);
    // Validate that the controller generates the correct schema.
    Assert.assertEquals(registeredWriteComputeSchema.get(0).getSchemaStr(), expectedWriteComputeSchema.toString());

    // Step 4. Add more value schemas and expect to get their corresponding write-compute schemas.
    parentControllerClient.addValueSchema(storeName, BAD_VALUE_SCHEMA_FOR_WRITE_COMPUTE_V2); // This won't generate any
                                                                                             // derived schema
    parentControllerClient.addValueSchema(storeName, VALUE_SCHEMA_FOR_WRITE_COMPUTE_V3);
    registeredSchemas = parentControllerClient.getAllValueAndDerivedSchema(storeName).getSchemas();
    Assert.assertEquals(registeredSchemas.length, 4);

    registeredWriteComputeSchema = getWriteComputeSchemaStrs(registeredSchemas);
    Assert.assertEquals(registeredWriteComputeSchema.size(), 2);
    // Sort registered write compute schemas by their value schema IDs.
    registeredWriteComputeSchema.sort(Comparator.comparingInt(MultiSchemaResponse.Schema::getId));
    // Validate all registered write compute schemas are generated as expected.
    expectedWriteComputeSchema =
        writeComputeSchemaConverter.convertFromValueRecordSchemaStr(VALUE_SCHEMA_FOR_WRITE_COMPUTE_V1);
    Assert.assertEquals(registeredWriteComputeSchema.get(0).getSchemaStr(), expectedWriteComputeSchema.toString());
    // Missing top field default will fail
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> writeComputeSchemaConverter.convertFromValueRecordSchemaStr(BAD_VALUE_SCHEMA_FOR_WRITE_COMPUTE_V2));
    // Assert.assertEquals(registeredWriteComputeSchema.get(1).getSchemaStr(), expectedWriteComputeSchema.toString());
    expectedWriteComputeSchema =
        writeComputeSchemaConverter.convertFromValueRecordSchemaStr(VALUE_SCHEMA_FOR_WRITE_COMPUTE_V3);
    Assert.assertEquals(registeredWriteComputeSchema.get(1).getSchemaStr(), expectedWriteComputeSchema.toString());

    for (MultiSchemaResponse.Schema writeComputeSchema: registeredWriteComputeSchema) {
      Assert.assertEquals(writeComputeSchema.getDerivedSchemaId(), 1);
    }
  }

  private void testWriteComputeSchemaEnable(ControllerClient parentControllerClient) {
    String storeName = Utils.getUniqueString("test_store");
    String owner = "test_owner";
    String keySchemaStr = "\"long\"";

    // Step 1. Create a store with missing default fields schema
    parentControllerClient.createNewStore(storeName, owner, keySchemaStr, BAD_VALUE_SCHEMA_FOR_WRITE_COMPUTE_V2);
    MultiSchemaResponse valueAndWriteComputeSchemaResponse =
        parentControllerClient.getAllValueAndDerivedSchema(storeName);
    MultiSchemaResponse.Schema[] registeredSchemas = valueAndWriteComputeSchemaResponse.getSchemas();
    Assert.assertEquals(registeredSchemas.length, 1);
    MultiSchemaResponse.Schema registeredSchema = registeredSchemas[0];
    Assert.assertFalse(registeredSchema.isDerivedSchema()); // No write compute schema yet.

    // Step 2. Update this store to enable write compute.
    UpdateStoreQueryParams updateStoreQueryParams = new UpdateStoreQueryParams();
    updateStoreQueryParams.setWriteComputationEnabled(true);
    parentControllerClient.updateStore(storeName, updateStoreQueryParams);

    // Could not enable write compute bad schema did not have defaults
    StoreInfo store = parentControllerClient.getStore(storeName).getStore();
    Assert.assertFalse(store.isWriteComputationEnabled());

    // Step 3. Add a valid latest value schema for write-compute
    parentControllerClient.addValueSchema(storeName, VALUE_SCHEMA_FOR_WRITE_COMPUTE_V3);

    registeredSchemas = parentControllerClient.getAllValueAndDerivedSchema(storeName).getSchemas();
    Assert.assertEquals(registeredSchemas.length, 2);
    List<MultiSchemaResponse.Schema> registeredWriteComputeSchema = getWriteComputeSchemaStrs(registeredSchemas);
    Assert.assertEquals(registeredWriteComputeSchema.size(), 0);
    parentControllerClient.updateStore(storeName, updateStoreQueryParams);

    registeredSchemas = parentControllerClient.getAllValueAndDerivedSchema(storeName).getSchemas();
    Assert.assertEquals(registeredSchemas.length, 3); // 2 value + 1 write compute schema

    registeredWriteComputeSchema = getWriteComputeSchemaStrs(registeredSchemas);
    Assert.assertEquals(registeredWriteComputeSchema.size(), 1);
  }

  private void testSupersetSchemaGenerationWithUpdateDefaultValue(ControllerClient parentControllerClient) {
    String storeName = Utils.getUniqueString("test_store");
    String owner = "test_owner";
    String keySchemaStr = "\"long\"";

    // Step 1. Create a store with missing default fields schema
    parentControllerClient
        .createNewStore(storeName, owner, keySchemaStr, TestWriteUtils.UNION_RECORD_V1_SCHEMA.toString());
    MultiSchemaResponse valueAndWriteComputeSchemaResponse =
        parentControllerClient.getAllValueAndDerivedSchema(storeName);
    MultiSchemaResponse.Schema[] registeredSchemas = valueAndWriteComputeSchemaResponse.getSchemas();
    Assert.assertEquals(registeredSchemas.length, 1);
    MultiSchemaResponse.Schema registeredSchema = registeredSchemas[0];
    Assert.assertFalse(registeredSchema.isDerivedSchema()); // No write compute schema yet.

    // Step 2. Update this store to enable write compute.
    UpdateStoreQueryParams updateStoreQueryParams = new UpdateStoreQueryParams();
    updateStoreQueryParams.setWriteComputationEnabled(true);
    parentControllerClient.updateStore(storeName, updateStoreQueryParams);

    // Could not enable write compute bad schema did not have defaults
    StoreInfo store = parentControllerClient.getStore(storeName).getStore();
    Assert.assertTrue(store.isWriteComputationEnabled());
    Assert.assertEquals(store.getLatestSuperSetValueSchemaId(), 1);

    // Step 3. Add a valid latest value schema for write-compute
    parentControllerClient.addValueSchema(storeName, TestWriteUtils.UNION_RECORD_V2_SCHEMA.toString());
    TestUtils.waitForNonDeterministicAssertion(
        30,
        TimeUnit.SECONDS,
        () -> Assert
            .assertEquals(parentControllerClient.getStore(storeName).getStore().getLatestSuperSetValueSchemaId(), 2));

    parentControllerClient.addValueSchema(storeName, TestWriteUtils.UNION_RECORD_V3_SCHEMA.toString());
    TestUtils.waitForNonDeterministicAssertion(
        30,
        TimeUnit.SECONDS,
        () -> Assert
            .assertEquals(parentControllerClient.getStore(storeName).getStore().getLatestSuperSetValueSchemaId(), 3));

  }

  private List<MultiSchemaResponse.Schema> getWriteComputeSchemaStrs(MultiSchemaResponse.Schema[] registeredSchemas) {
    List<MultiSchemaResponse.Schema> writeComputeSchemaStrs = new ArrayList<>();
    for (MultiSchemaResponse.Schema schema: registeredSchemas) {
      if (schema.isDerivedSchema()) {
        writeComputeSchemaStrs.add(schema);
      }
    }
    return writeComputeSchemaStrs;
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

  private Schema generateSuperSetSchema() {
    String schemaStr = "{\"namespace\": \"example.avro\",\n" + " \"type\": \"record\",\n" + " \"name\": \"User\",\n"
        + " \"fields\": [\n" + "      { \"name\": \"id\", \"type\": \"string\", \"default\": \"default_ID\"},\n"
        + "      {\n" + "       \"name\": \"value\",\n" + "       \"type\": [\"null\" , {\n"
        + "           \"type\": \"record\",\n" + "           \"name\": \"ValueRecord\",\n"
        + "           \"fields\" : [\n"
        + "{\"name\": \"favorite_color\", \"type\": \"string\", \"default\": \"blue\"},\n"
        + "{\"name\": \"favorite_number\", \"type\": \"int\", \"default\" : 0}\n";

    schemaStr += "           ]\n" + "     }],\n" + "    \"default\": null" + "   }\n" + " ]\n" + "}";
    return AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr);
  }

  private Schema generateSuperSetSchemaNewField() {
    String schemaStr = "{\"namespace\": \"example.avro\",\n" + " \"type\": \"record\",\n" + " \"name\": \"User\",\n"
        + " \"fields\": [\n" + "      { \"name\": \"id\", \"type\": \"string\", \"default\": \"default_ID\"},\n"
        + "      {\n" + "       \"name\": \"value\",\n" + "       \"type\": [\"null\" ,{\n"
        + "           \"type\": \"record\",\n" + "           \"name\": \"ValueRecord\",\n"
        + "           \"fields\" : [\n"
        + "{\"name\": \"favorite_color\", \"type\": \"string\", \"default\": \"blue\"},\n"
        + "{\"name\": \"favorite_company\", \"type\": \"string\", \"default\": \"linkedin\"},\n"
        + "{\"name\": \"favorite_number\", \"type\": \"int\", \"default\" : 0}\n";

    schemaStr += "           ]\n" + "      }], " + "     \"default\": null\n" + "    }\n" + " ]\n" + "}";
    return AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(schemaStr);
  }

}
