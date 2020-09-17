package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.ETLStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;
import static org.testng.Assert.*;

public class VeniceParentHelixAdminTest {
  private static final long DEFAULT_TEST_TIMEOUT = 30000;
  VeniceClusterWrapper venice;
  ZkServerWrapper zkServerWrapper;

  @BeforeClass
  public void setup() {
    venice = ServiceFactory.getVeniceCluster(1, 1, 1, 1, 100000, false, false);
    zkServerWrapper = ServiceFactory.getZkServer();
  }

  @AfterClass
  public void cleanup() {
    venice.close();
    zkServerWrapper.close();
  }

  @Test(timeOut = DEFAULT_TEST_TIMEOUT)
  public void testTerminalStateTopicChecker() {
    Properties properties = new Properties();
    properties.setProperty(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, String.valueOf(Long.MAX_VALUE));
    properties.setProperty(TERMINAL_STATE_TOPIC_CHECK_DELAY_MS, String.valueOf(1000L));
    VeniceControllerWrapper parentController = ServiceFactory.getVeniceParentController(venice.getClusterName(),
        zkServerWrapper.getAddress(), venice.getKafka(), new VeniceControllerWrapper[]{venice.getMasterVeniceController()},
        new VeniceProperties(properties), false);
    ControllerClient parentControllerClient = new ControllerClient(venice.getClusterName(), parentController.getControllerUrl());

    String storeName = TestUtils.getUniqueString("testStore");
    assertFalse(parentControllerClient.createNewStore(storeName, "test", "\"string\"", "\"string\"").isError(),
        "Failed to create test store");
    // Empty push without checking its push status
    VersionCreationResponse response = parentControllerClient.emptyPush(storeName, "test-push", 1000);
    assertFalse(response.isError(), "Failed to perform empty push on test store");
    // The empty push should eventually complete and have its version topic truncated by job status polling invoked by
    // the TerminalStateTopicCheckerForParentController.
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true,
       () -> assertTrue(parentController.getVeniceAdmin().isTopicTruncated(response.getKafkaTopic())));

    parentControllerClient.close();
    parentController.close();
  }


  @Test(timeOut = DEFAULT_TEST_TIMEOUT)
  public void testHybridAndETLStoreConfig() {
    try (VeniceControllerWrapper parentControllerWrapper =
            ServiceFactory.getVeniceParentController(venice.getClusterName(), zkServerWrapper.getAddress(), venice.getKafka(),
                new VeniceControllerWrapper[]{venice.getMasterVeniceController()},false)) {
      String controllerUrl = parentControllerWrapper.getControllerUrl();

      // Adding store
      String storeName = "test_store";
      String owner = "test_owner";
      String keySchemaStr = "\"long\"";
      String proxyUser = "test_user";
      Schema valueSchema = generateSchema(false);
      try (ControllerClient controllerClient = new ControllerClient(venice.getClusterName(), controllerUrl)) {
        controllerClient.createNewStore(storeName, owner, keySchemaStr, valueSchema.toString());

        // Configure the store to hybrid
        UpdateStoreQueryParams params = new UpdateStoreQueryParams()
            .setHybridRewindSeconds(600)
            .setHybridOffsetLagThreshold(10000);
        ControllerResponse controllerResponse = controllerClient.updateStore(storeName, params);
        Assert.assertFalse(controllerResponse.isError());
        HybridStoreConfig hybridStoreConfig = controllerClient.getStore(storeName).getStore().getHybridStoreConfig();
        Assert.assertEquals(hybridStoreConfig.getRewindTimeInSeconds(), 600);
        Assert.assertEquals(hybridStoreConfig.getOffsetLagThresholdToGoOnline(), 10000);

        // Try to update the hybrid store with different hybrid configs
        params = new UpdateStoreQueryParams()
            .setHybridRewindSeconds(172800);
        controllerResponse = controllerClient.updateStore(storeName, params);
        Assert.assertFalse(controllerResponse.isError());
        hybridStoreConfig = controllerClient.getStore(storeName).getStore().getHybridStoreConfig();
        Assert.assertEquals(hybridStoreConfig.getRewindTimeInSeconds(), 172800);
        Assert.assertEquals(hybridStoreConfig.getOffsetLagThresholdToGoOnline(), 10000);

        // test enabling ETL without etl proxy account, expected failure
        params = new UpdateStoreQueryParams();
        params.setRegularVersionETLEnabled(true);
        params.setFutureVersionETLEnabled(true);
        controllerResponse = controllerClient.updateStore(storeName, params);
        ETLStoreConfig etlStoreConfig = controllerClient.getStore(storeName).getStore().getEtlStoreConfig();
        Assert.assertFalse(etlStoreConfig.isRegularVersionETLEnabled());
        Assert.assertFalse(etlStoreConfig.isFutureVersionETLEnabled());
        Assert.assertTrue(controllerResponse.getError().contains("Cannot enable ETL for this store "
            + "because etled user proxy account is not set"));

        // test enabling ETL with empty proxy account, expected failure
        params = new UpdateStoreQueryParams();
        params.setRegularVersionETLEnabled(true).setEtledProxyUserAccount("");
        params.setFutureVersionETLEnabled(true).setEtledProxyUserAccount("");
        controllerResponse = controllerClient.updateStore(storeName, params);
        etlStoreConfig = controllerClient.getStore(storeName).getStore().getEtlStoreConfig();
        Assert.assertFalse(etlStoreConfig.isRegularVersionETLEnabled());
        Assert.assertFalse(etlStoreConfig.isFutureVersionETLEnabled());
        Assert.assertTrue(controllerResponse.getError().contains("Cannot enable ETL for this store "
            + "because etled user proxy account is not set"));

        // test enabling ETL with etl proxy account, expected success
        params = new UpdateStoreQueryParams();
        params.setRegularVersionETLEnabled(true).setEtledProxyUserAccount(proxyUser);
        params.setFutureVersionETLEnabled(true).setEtledProxyUserAccount(proxyUser);
        controllerClient.updateStore(storeName, params);
        etlStoreConfig = controllerClient.getStore(storeName).getStore().getEtlStoreConfig();
        Assert.assertTrue(etlStoreConfig.isRegularVersionETLEnabled());
        Assert.assertTrue(etlStoreConfig.isFutureVersionETLEnabled());

        // set the ETL back to false
        params = new UpdateStoreQueryParams();
        params.setRegularVersionETLEnabled(false);
        params.setFutureVersionETLEnabled(false);
        controllerClient.updateStore(storeName, params);
        etlStoreConfig = controllerClient.getStore(storeName).getStore().getEtlStoreConfig();
        Assert.assertFalse(etlStoreConfig.isRegularVersionETLEnabled());
        Assert.assertFalse(etlStoreConfig.isFutureVersionETLEnabled());

        // test enabling ETL again without etl proxy account, expected success
        params = new UpdateStoreQueryParams();
        params.setRegularVersionETLEnabled(true);
        params.setFutureVersionETLEnabled(true);
        controllerClient.updateStore(storeName, params);
        etlStoreConfig = controllerClient.getStore(storeName).getStore().getEtlStoreConfig();
        Assert.assertTrue(etlStoreConfig.isRegularVersionETLEnabled());
        Assert.assertTrue(etlStoreConfig.isFutureVersionETLEnabled());
      }
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = DEFAULT_TEST_TIMEOUT)
  public void testStoreMetaDataUpdateFromParentToChildController(boolean isControllerSslEnabled) {
    String clusterName = TestUtils.getUniqueString("testStoreMetadataUpdate");
    try (KafkaBrokerWrapper kafkaBrokerWrapper = ServiceFactory.getKafkaBroker();
        VeniceControllerWrapper childControllerWrapper =
            ServiceFactory.getVeniceController(clusterName, kafkaBrokerWrapper, isControllerSslEnabled);
        ZkServerWrapper parentZk = ServiceFactory.getZkServer();
        VeniceControllerWrapper parentControllerWrapper =
            ServiceFactory.getVeniceParentController(clusterName, parentZk.getAddress(), kafkaBrokerWrapper,
                new VeniceControllerWrapper[]{childControllerWrapper}, isControllerSslEnabled)) {
      String childControllerUrl =
          isControllerSslEnabled ? childControllerWrapper.getSecureControllerUrl() : childControllerWrapper.getControllerUrl();
      String parentControllerUrl =
          isControllerSslEnabled ? parentControllerWrapper.getSecureControllerUrl() : parentControllerWrapper.getControllerUrl();
      Optional<SSLFactory> sslFactory =
          isControllerSslEnabled ? Optional.of(SslUtils.getVeniceLocalSslFactory()) : Optional.empty();
      try (ControllerClient parentControllerClient = new ControllerClient(clusterName, parentControllerUrl, sslFactory);
          ControllerClient childControllerClient = new ControllerClient(clusterName, childControllerUrl, sslFactory)) {

        testBackupVersionRetentionUpdate(parentControllerClient, childControllerClient);

        testSuperSetSchemaGen(parentControllerClient, childControllerClient);

        testSuperSetSchemaGenWithSameUpcomingSchema(parentControllerClient, childControllerClient);

        testAddValueSchemaDocUpdate(parentControllerClient, childControllerClient);
      }
    }
  }

  private void testBackupVersionRetentionUpdate(ControllerClient parentControllerClient, ControllerClient childControllerClient) {
    String storeName = TestUtils.getUniqueString("test_store_");
    String owner = "test_owner";
    String keySchemaStr = "\"long\"";
    String valueSchemaStr ="\"string\"";
    NewStoreResponse
        newStoreResponse = parentControllerClient.createNewStore(storeName, owner, keySchemaStr, valueSchemaStr);
    Assert.assertNotNull(newStoreResponse);
    Assert.assertFalse(newStoreResponse.isError(), "error in newStoreResponse: " + newStoreResponse.getError());
    long backupVersionRetentionMs = TimeUnit.HOURS.toMillis(1);
    ControllerResponse controllerResponse = parentControllerClient.updateStore(storeName,
        new UpdateStoreQueryParams().setBackupVersionRetentionMs(backupVersionRetentionMs));
    Assert.assertNotNull(controllerResponse);
    Assert.assertFalse(controllerResponse.isError(), "Error in store update response: " + controllerResponse.getError());

    // Verify the update in Parent Controller
    StoreResponse storeResponseFromParentController = parentControllerClient.getStore(storeName);
    Assert.assertFalse(storeResponseFromParentController.isError(), "Error in store response from Parent Controller: " + storeResponseFromParentController.getError());
    Assert.assertEquals(storeResponseFromParentController.getStore().getBackupVersionRetentionMs(), backupVersionRetentionMs);
    // Verify the update in Child Controller
    TestUtils.waitForNonDeterministicAssertion(30000, TimeUnit.MILLISECONDS, () -> {
      StoreResponse storeResponseFromChildController = childControllerClient.getStore(storeName);
      Assert.assertFalse(storeResponseFromChildController.isError(), "Error in store response from Child Controller: " + storeResponseFromChildController.getError());
      Assert.assertEquals(storeResponseFromChildController.getStore().getBackupVersionRetentionMs(), backupVersionRetentionMs);
    });
  }


  private void testSuperSetSchemaGen(ControllerClient parentControllerClient, ControllerClient childControllerClient) {
    // Adding store
    String storeName = TestUtils.getUniqueString("test_store");
    String owner = "test_owner";
    String keySchemaStr = "\"long\"";
    Schema valueSchema = generateSchema(false);

    NewStoreResponse newStoreResponse = parentControllerClient.createNewStore(storeName, owner, keySchemaStr, valueSchema.toString());
    Assert.assertNotNull(newStoreResponse);
    Assert.assertFalse(newStoreResponse.isError(), "error in newStoreResponse: " + newStoreResponse.getError());

    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setReadComputationEnabled(true);
    params.setAutoSchemaPushJobEnabled(true);
    ControllerResponse updateStoreResponse = parentControllerClient.updateStore(storeName, params);
    Assert.assertNotNull(updateStoreResponse);
    Assert.assertFalse(updateStoreResponse.isError(), "error in updateStoreResponse: " + updateStoreResponse.getError());

    valueSchema = generateSchema(true);
    SchemaResponse addSchemaRespone = parentControllerClient.addValueSchema(storeName, valueSchema.toString());
    Assert.assertNotNull(addSchemaRespone);
    Assert.assertFalse(addSchemaRespone.isError(), "error in addSchemaRespone: " + addSchemaRespone.getError());

    MultiSchemaResponse schemaResponse = parentControllerClient.getAllValueSchema(storeName);
    Assert.assertNotNull(schemaResponse);
    Assert.assertFalse(schemaResponse.isError(), "error in schemaResponse: " + schemaResponse.getError());
    Assert.assertNotNull(schemaResponse.getSchemas());
    Assert.assertEquals(schemaResponse.getSchemas().length,3);

    StoreResponse storeResponse = parentControllerClient.getStore(storeName);
    Assert.assertNotNull(storeResponse);
    Assert.assertFalse(storeResponse.isError(), "error in storeResponse: " + storeResponse.getError());
    Assert.assertNotNull(storeResponse.getStore());
    Assert.assertTrue(storeResponse.getStore().getLatestSuperSetValueSchemaId() != -1);

    valueSchema = generateSuperSetSchemaNewField();
    addSchemaRespone = parentControllerClient.addValueSchema(storeName, valueSchema.toString());
    Assert.assertNotNull(addSchemaRespone);
    Assert.assertFalse(addSchemaRespone.isError(), "error in addSchemaRespone: " + addSchemaRespone.getError());

    schemaResponse = parentControllerClient.getAllValueSchema(storeName);
    Assert.assertNotNull(schemaResponse);
    Assert.assertFalse(schemaResponse.isError(), "error in schemaResponse: " + schemaResponse.getError());
    Assert.assertNotNull(schemaResponse.getSchemas());
    Assert.assertEquals(schemaResponse.getSchemas().length,4);
  }

  private void testSuperSetSchemaGenWithSameUpcomingSchema(ControllerClient parentControllerClient, ControllerClient childControllerClient) {
    // Adding store
    String storeName = TestUtils.getUniqueString("test_store");;
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

    Assert.assertEquals(schemaResponse.getSchemas().length,2);
    StoreResponse storeResponse = parentControllerClient.getStore(storeName);
    Assert.assertTrue(storeResponse.getStore().getLatestSuperSetValueSchemaId() == -1);
  }

  private void testAddValueSchemaDocUpdate(ControllerClient parentControllerClient, ControllerClient childControllerClient) {
    // Adding store
    String storeName = TestUtils.getUniqueString("test_store");;
    String owner = "test_owner";
    String keySchemaStr = "\"long\"";
    String schemaStr = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"id1\",\"type\":\"double\"}]}";
    String schemaStrDoc = "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field updated\"},{\"name\":\"id1\",\"type\":\"double\"}]}";
    Schema valueSchema = Schema.parse(schemaStr);
    parentControllerClient.createNewStore(storeName, owner, keySchemaStr, valueSchema.toString());
    valueSchema = Schema.parse(schemaStrDoc);
    parentControllerClient.addValueSchema(storeName, valueSchema.toString());
    MultiSchemaResponse schemaResponse = parentControllerClient.getAllValueSchema(storeName);
    Assert.assertEquals(schemaResponse.getSchemas().length,2);
  }

  private Schema generateSchema(boolean addFieldWithDefaultValue) {
    String schemaStr = "{\"namespace\": \"example.avro\",\n" +
        " \"type\": \"record\",\n" +
        " \"name\": \"User\",\n" +
        " \"fields\": [\n" +
        "      { \"name\": \"id\", \"type\": \"string\"},\n" +
        "      {\n" +
        "       \"name\": \"value\",\n" +
        "       \"type\": {\n" +
        "           \"type\": \"record\",\n" +
        "           \"name\": \"ValueRecord\",\n" +
        "           \"fields\" : [\n";
    if (addFieldWithDefaultValue) {
      schemaStr += "{\"name\": \"favorite_color\", \"type\": \"string\", \"default\": \"blue\"}\n";
    } else {
      schemaStr +=   "{\"name\": \"favorite_number\", \"type\": \"int\", \"default\" : 0}\n";
    }
    schemaStr +=
        "           ]\n" +
            "        }\n" +
            "      }\n" +
            " ]\n" +
            "}";
    return Schema.parse(schemaStr);
  }

  private Schema generateSuperSetSchema() {
    String schemaStr = "{\"namespace\": \"example.avro\",\n" +
        " \"type\": \"record\",\n" +
        " \"name\": \"User\",\n" +
        " \"fields\": [\n" +
        "      { \"name\": \"id\", \"type\": \"string\"},\n" +
        "      {\n" +
        "       \"name\": \"value\",\n" +
        "       \"type\": {\n" +
        "           \"type\": \"record\",\n" +
        "           \"name\": \"ValueRecord\",\n" +
        "           \"fields\" : [\n" +
        "{\"name\": \"favorite_color\", \"type\": \"string\", \"default\": \"blue\"},\n" +
        "{\"name\": \"favorite_number\", \"type\": \"int\", \"default\" : 0}\n";

    schemaStr +=
        "           ]\n" +
            "        }\n" +
            "      }\n" +
            " ]\n" +
            "}";
    return Schema.parse(schemaStr);
  }

  private Schema generateSuperSetSchemaNewField() {
    String schemaStr = "{\"namespace\": \"example.avro\",\n" +
        " \"type\": \"record\",\n" +
        " \"name\": \"User\",\n" +
        " \"fields\": [\n" +
        "      { \"name\": \"id\", \"type\": \"string\"},\n" +
        "      {\n" +
        "       \"name\": \"value\",\n" +
        "       \"type\": {\n" +
        "           \"type\": \"record\",\n" +
        "           \"name\": \"ValueRecord\",\n" +
        "           \"fields\" : [\n" +
        "{\"name\": \"favorite_color\", \"type\": \"string\", \"default\": \"blue\"},\n" +
        "{\"name\": \"favorite_company\", \"type\": \"string\", \"default\": \"linkedin\"},\n" +
        "{\"name\": \"favorite_number\", \"type\": \"int\", \"default\" : 0}\n";

    schemaStr +=
        "           ]\n" +
            "        }\n" +
            "      }\n" +
            " ]\n" +
            "}";
    return Schema.parse(schemaStr);
  }
}
