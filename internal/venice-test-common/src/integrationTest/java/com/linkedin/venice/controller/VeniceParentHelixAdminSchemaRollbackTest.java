package com.linkedin.venice.controller;

import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V3_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static org.testng.Assert.assertEquals;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class VeniceParentHelixAdminSchemaRollbackTest {
  private static final long DEFAULT_TEST_TIMEOUT_MS = 60000;
  private VeniceParentHelixAdminTestFixture fixture;
  private VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionMultiClusterWrapper;
  private String clusterName;

  @BeforeClass
  public void setUp() {
    fixture = new VeniceParentHelixAdminTestFixture();
    multiRegionMultiClusterWrapper = fixture.getMultiRegionMultiClusterWrapper();
    clusterName = fixture.getClusterName();
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(fixture);
  }

  @Test(timeOut = 2 * DEFAULT_TEST_TIMEOUT_MS)
  public void testRollbackToBackupVersion() throws IOException {
    File inputDir = getTempDataDirectory();
    TestWriteUtils.writeSimpleAvroFileWithStringToV3Schema(inputDir, 100, 100);
    // Setup job properties
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("testRollbackToBackupVersion");
    Properties props =
        IntegrationTestPushUtils.defaultVPJProps(multiRegionMultiClusterWrapper, inputDirPath, storeName);
    String keySchemaStr = "\"string\"";
    createStoreForJob(clusterName, keySchemaStr, NAME_RECORD_V3_SCHEMA.toString(), props, new UpdateStoreQueryParams())
        .close();

    try (ControllerClient parentControllerClient =
        new ControllerClient(clusterName, multiRegionMultiClusterWrapper.getControllerConnectString())) {
      // Create version 1
      IntegrationTestPushUtils.runVPJ(props);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 1),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      // Create version 2
      IntegrationTestPushUtils.runVPJ(props);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 2),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      // Rollback to backup version
      parentControllerClient.rollbackToBackupVersion(storeName);

      // Verify store is back on version 1
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        Map<String, Integer> coloVersions =
            parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions();

        coloVersions.forEach((colo, version) -> {
          Assert.assertEquals((int) version, 1);
        });
      });

      // Check that child version status is marked as ERROR after rollback
      for (VeniceMultiClusterWrapper childDatacenter: multiRegionMultiClusterWrapper.getChildRegions()) {
        try (ControllerClient childControllerClient =
            new ControllerClient(clusterName, childDatacenter.getControllerConnectString())) {
          StoreResponse store = childControllerClient.getStore(storeName);
          Optional<Version> version = store.getStore().getVersion(2);
          Assert.assertTrue(version.isPresent(), "Version 2 should exist after rollback");
          assertEquals(version.get().getStatus(), VersionStatus.ERROR);
        }
      }

      // Create version 3
      IntegrationTestPushUtils.runVPJ(props);
      TestUtils.waitForNonDeterministicPushCompletion(
          Version.composeKafkaTopic(storeName, 3),
          parentControllerClient,
          30,
          TimeUnit.SECONDS);

      // Rollback to backup version
      parentControllerClient.rollbackToBackupVersion(storeName);

      // Verify store is back on version 1 even after version 3 creation
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        Map<String, Integer> coloVersions =
            parentControllerClient.getStore(storeName).getStore().getColoToCurrentVersions();

        coloVersions.forEach((colo, version) -> {
          Assert.assertEquals((int) version, 1);
        });
      });
    }
  }

  @Test(timeOut = DEFAULT_TEST_TIMEOUT_MS)
  public void testSupersetSchemaUpdateBehaviorWhenComputeDisabled() {
    String storeName = Utils.getUniqueString("test_superset_compute_disabled");
    String owner = "test_owner";
    String keySchemaStr = "\"long\"";

    // Load schemas from test resources
    Schema valueSchemaV1 =
        AvroCompatibilityHelper.parse(TestWriteUtils.loadFileAsString("valueSchema/supersetschemas/ValueV1.avsc"));
    Schema valueSchemaV2 =
        AvroCompatibilityHelper.parse(TestWriteUtils.loadFileAsString("valueSchema/supersetschemas/ValueV2.avsc"));
    Schema valueSchemaV3 =
        AvroCompatibilityHelper.parse(TestWriteUtils.loadFileAsString("valueSchema/supersetschemas/ValueV3.avsc"));
    Schema valueSchemaV7 =
        AvroCompatibilityHelper.parse(TestWriteUtils.loadFileAsString("valueSchema/supersetschemas/ValueV7.avsc"));
    Schema valueSchemaV8 =
        AvroCompatibilityHelper.parse(TestWriteUtils.loadFileAsString("valueSchema/supersetschemas/ValueV8.avsc"));

    try (ControllerClient parentControllerClient =
        new ControllerClient(clusterName, multiRegionMultiClusterWrapper.getControllerConnectString())) {

      // ==================== PART 1: No superset created when compute disabled ====================

      NewStoreResponse newStoreResponse =
          parentControllerClient.createNewStore(storeName, owner, keySchemaStr, valueSchemaV1.toString());
      Assert.assertNotNull(newStoreResponse);
      Assert.assertFalse(newStoreResponse.isError(), "error in newStoreResponse: " + newStoreResponse.getError());

      StoreResponse storeResponse = parentControllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError(), "error in storeResponse: " + storeResponse.getError());
      Assert.assertFalse(storeResponse.getStore().isReadComputationEnabled(), "Read computation should be disabled");
      Assert.assertFalse(storeResponse.getStore().isWriteComputationEnabled(), "Write computation should be disabled");
      Assert.assertEquals(
          storeResponse.getStore().getLatestSuperSetValueSchemaId(),
          -1,
          "No superset schema should exist initially");

      SchemaResponse addSchemaResponse = parentControllerClient.addValueSchema(storeName, valueSchemaV2.toString());
      Assert.assertNotNull(addSchemaResponse);
      Assert.assertFalse(addSchemaResponse.isError(), "error in addSchemaResponse: " + addSchemaResponse.getError());

      storeResponse = parentControllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError(), "error in storeResponse: " + storeResponse.getError());
      Assert.assertEquals(
          storeResponse.getStore().getLatestSuperSetValueSchemaId(),
          -1,
          "No superset schema should exist after adding V2 with compute disabled");

      addSchemaResponse = parentControllerClient.addValueSchema(storeName, valueSchemaV3.toString());
      Assert.assertNotNull(addSchemaResponse);
      Assert.assertFalse(addSchemaResponse.isError(), "error in addSchemaResponse: " + addSchemaResponse.getError());

      storeResponse = parentControllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError(), "error in storeResponse: " + storeResponse.getError());
      Assert.assertEquals(
          storeResponse.getStore().getLatestSuperSetValueSchemaId(),
          -1,
          "No superset schema should be created when compute is disabled and none existed");

      MultiSchemaResponse schemaResponse = parentControllerClient.getAllValueSchema(storeName);
      Assert.assertNotNull(schemaResponse);
      Assert.assertFalse(schemaResponse.isError(), "error in schemaResponse: " + schemaResponse.getError());
      Assert.assertEquals(
          schemaResponse.getSchemas().length,
          3,
          "There should be exactly 3 value schemas (no superset created)");

      // ==================== PART 2: Superset IS updated when one already exists ====================

      UpdateStoreQueryParams params = new UpdateStoreQueryParams();
      params.setReadComputationEnabled(true);
      ControllerResponse updateStoreResponse = parentControllerClient.updateStore(storeName, params);
      Assert.assertNotNull(updateStoreResponse);
      Assert.assertFalse(
          updateStoreResponse.isError(),
          "error in updateStoreResponse: " + updateStoreResponse.getError());

      storeResponse = parentControllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError(), "error in storeResponse: " + storeResponse.getError());
      Assert.assertTrue(storeResponse.getStore().isReadComputationEnabled(), "Read computation should be enabled");

      addSchemaResponse = parentControllerClient.addValueSchema(storeName, valueSchemaV7.toString());
      Assert.assertNotNull(addSchemaResponse);
      Assert.assertFalse(addSchemaResponse.isError(), "error in addSchemaResponse: " + addSchemaResponse.getError());

      storeResponse = parentControllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError(), "error in storeResponse: " + storeResponse.getError());
      int supersetSchemaIdBeforeDisable = storeResponse.getStore().getLatestSuperSetValueSchemaId();
      Assert.assertTrue(
          supersetSchemaIdBeforeDisable > 0,
          "Superset schema should exist after enabling compute and adding schema");

      SchemaResponse supersetSchemaResponse =
          parentControllerClient.getValueSchema(storeName, supersetSchemaIdBeforeDisable);
      Assert.assertFalse(
          supersetSchemaResponse.isError(),
          "error in schemaResponse: " + supersetSchemaResponse.getError());
      Schema supersetSchema = AvroCompatibilityHelper.parse(supersetSchemaResponse.getSchemaStr());
      Assert.assertNotNull(supersetSchema.getField("f0"), "Superset schema should contain f0");
      Assert.assertNotNull(supersetSchema.getField("f1"), "Superset schema should contain f1");
      Assert.assertNotNull(supersetSchema.getField("f2"), "Superset schema should contain f2");
      Assert.assertNotNull(supersetSchema.getField("f3"), "Superset schema should contain f3");
      Assert.assertNotNull(supersetSchema.getField("f4"), "Superset schema should contain f4");
      Assert.assertNotNull(supersetSchema.getField("f5"), "Superset schema should contain f5");

      params = new UpdateStoreQueryParams();
      params.setReadComputationEnabled(false);
      updateStoreResponse = parentControllerClient.updateStore(storeName, params);
      Assert.assertNotNull(updateStoreResponse);
      Assert.assertFalse(
          updateStoreResponse.isError(),
          "error in updateStoreResponse: " + updateStoreResponse.getError());

      storeResponse = parentControllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError(), "error in storeResponse: " + storeResponse.getError());
      Assert.assertFalse(storeResponse.getStore().isReadComputationEnabled(), "Read computation should be disabled");

      addSchemaResponse = parentControllerClient.addValueSchema(storeName, valueSchemaV8.toString());
      Assert.assertNotNull(addSchemaResponse);
      Assert.assertFalse(addSchemaResponse.isError(), "error in addSchemaResponse: " + addSchemaResponse.getError());

      storeResponse = parentControllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError(), "error in storeResponse: " + storeResponse.getError());
      int supersetSchemaIdAfterDisable = storeResponse.getStore().getLatestSuperSetValueSchemaId();

      Assert.assertNotEquals(
          supersetSchemaIdAfterDisable,
          supersetSchemaIdBeforeDisable,
          "Superset schema ID should change when a new field is added, even with compute disabled since a superset exists");

      supersetSchemaResponse = parentControllerClient.getValueSchema(storeName, supersetSchemaIdAfterDisable);
      Assert.assertFalse(
          supersetSchemaResponse.isError(),
          "error in schemaResponse: " + supersetSchemaResponse.getError());
      Schema supersetSchemaAfterDisable = AvroCompatibilityHelper.parse(supersetSchemaResponse.getSchemaStr());
      Assert.assertNotNull(
          supersetSchemaAfterDisable.getField("f6"),
          "Superset schema should contain f6 because superset update continues when one already exists");
      Assert.assertNotNull(supersetSchemaAfterDisable.getField("f0"), "Superset schema should still contain f0");
      Assert.assertNotNull(supersetSchemaAfterDisable.getField("f1"), "Superset schema should still contain f1");
      Assert.assertNotNull(supersetSchemaAfterDisable.getField("f2"), "Superset schema should still contain f2");
      Assert.assertNotNull(supersetSchemaAfterDisable.getField("f3"), "Superset schema should still contain f3");
      Assert.assertNotNull(supersetSchemaAfterDisable.getField("f4"), "Superset schema should still contain f4");
      Assert.assertNotNull(supersetSchemaAfterDisable.getField("f5"), "Superset schema should still contain f5");

      // ==================== PART 3: Clear superset schema via updateStore ====================

      params = new UpdateStoreQueryParams();
      params.setLatestSupersetSchemaId(SchemaData.INVALID_VALUE_SCHEMA_ID);
      updateStoreResponse = parentControllerClient.updateStore(storeName, params);
      Assert.assertNotNull(updateStoreResponse);
      Assert.assertFalse(
          updateStoreResponse.isError(),
          "error in updateStoreResponse: " + updateStoreResponse.getError());

      storeResponse = parentControllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError(), "error in storeResponse: " + storeResponse.getError());
      Assert.assertEquals(
          storeResponse.getStore().getLatestSuperSetValueSchemaId(),
          SchemaData.INVALID_VALUE_SCHEMA_ID,
          "Superset schema ID should be INVALID after clearing it via updateStore");

      Assert.assertFalse(
          storeResponse.getStore().isReadComputationEnabled(),
          "Read computation should still be disabled");
      Assert.assertFalse(
          storeResponse.getStore().isWriteComputationEnabled(),
          "Write computation should still be disabled");

      Schema valueSchemaV9 =
          AvroCompatibilityHelper.parse(TestWriteUtils.loadFileAsString("valueSchema/supersetschemas/ValueV9.avsc"));
      addSchemaResponse = parentControllerClient.addValueSchema(storeName, valueSchemaV9.toString());
      Assert.assertNotNull(addSchemaResponse);
      Assert.assertFalse(addSchemaResponse.isError(), "error in addSchemaResponse: " + addSchemaResponse.getError());

      storeResponse = parentControllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError(), "error in storeResponse: " + storeResponse.getError());
      Assert.assertEquals(
          storeResponse.getStore().getLatestSuperSetValueSchemaId(),
          SchemaData.INVALID_VALUE_SCHEMA_ID,
          "No superset schema should be created after clearing it when compute is disabled");
    }
  }
}
