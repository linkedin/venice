package com.linkedin.venice.controller.server;

import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_MB;
import static com.linkedin.venice.utils.TestUtils.assertCommand;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.LastSucceedExecutionIdResponse;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.InstanceRemovableStatuses;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.AdminCommandExecution;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.controllerapi.MultiNodeResponse;
import com.linkedin.venice.controllerapi.MultiNodesStatusResponse;
import com.linkedin.venice.controllerapi.MultiReplicaResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.MultiStoreStatusResponse;
import com.linkedin.venice.controllerapi.MultiStoreTopicsResponse;
import com.linkedin.venice.controllerapi.MultiVersionResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.OwnerResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StorageEngineOverheadRatioResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.TrackableControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.controllerapi.routes.AdminCommandExecutionResponse;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.ExceptionType;
import com.linkedin.venice.httpclient.HttpClientUtils;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.InstanceStatus;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.StoreStatus;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.EncodingUtils;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.nio.client.HttpAsyncClient;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestAdminSparkServer extends AbstractTestAdminSparkServer {
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

  /**
   * Seems that Helix has limit on the number of resource each node is able to handle.
   * If the test case needs more than one storage node like testing failover etc, please put it into {@link TestAdminSparkServerWithMultiServers}
   *
   * And please collect the store and version you created in the end of your test case.
   */

  private static final int TEST_MAX_RECORD_SIZE_BYTES = 16 * BYTES_PER_MB; // arbitrary 16MB

  @BeforeClass
  public void setUp() {
    Properties extraProperties = new Properties();

    extraProperties.put(
        ConfigKeys.CONTROLLER_JETTY_CONFIG_OVERRIDE_PREFIX + "org.eclipse.jetty.server.Request.maxFormContentSize",
        BYTES_PER_MB);
    // Set topic cleanup interval to a large number and min number of unused topic to preserve to 1 to test
    // getDeletableStoreTopics deterministically.
    extraProperties.put(
        ConfigKeys.TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS,
        Long.toString(TimeUnit.DAYS.toMillis(7)));
    extraProperties.put(ConfigKeys.DEFAULT_MAX_RECORD_SIZE_BYTES, Integer.toString(TEST_MAX_RECORD_SIZE_BYTES));
    extraProperties.put(ConfigKeys.MIN_NUMBER_OF_UNUSED_KAFKA_TOPICS_TO_PRESERVE, Integer.toString(1));
    super.setUp(false, Optional.empty(), extraProperties);
  }

  @AfterClass
  public void cleanUp() {
    super.cleanUp();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanQueryNodesInCluster() {
    MultiNodeResponse nodeResponse = controllerClient.listStorageNodes();
    Assert.assertFalse(nodeResponse.isError(), nodeResponse.getError());
    Assert.assertEquals(nodeResponse.getNodes().length, STORAGE_NODE_COUNT, "Node count does not match");
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanQueryInstanceStatusInCluster() {
    MultiNodesStatusResponse nodeResponse = controllerClient.listInstancesStatuses(false);
    Assert.assertFalse(nodeResponse.isError(), nodeResponse.getError());
    Assert.assertEquals(nodeResponse.getInstancesStatusMap().size(), STORAGE_NODE_COUNT, "Node count does not match");
    Assert.assertEquals(
        nodeResponse.getInstancesStatusMap().values().iterator().next(),
        InstanceStatus.CONNECTED.toString(),
        "Node status does not match.");
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanQueryReplicasOnAStorageNode() {
    String storeName = Utils.getUniqueString("test-store");
    assertCommand(parentControllerClient.createNewStore(storeName, "owner", "\"string\"", "\"string\""));
    assertCommand(
        parentControllerClient
            .sendEmptyPushAndWait(storeName, Utils.getUniqueString(storeName), 1024, 60 * Time.MS_PER_SECOND));
    try {
      MultiNodeResponse nodeResponse = controllerClient.listStorageNodes();
      String nodeId = nodeResponse.getNodes()[0];
      MultiReplicaResponse replicas = controllerClient.listStorageNodeReplicas(nodeId);
      Assert.assertFalse(replicas.isError(), replicas.getError());
    } finally {
      deleteStore(storeName);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanQueryReplicasForTopic() {
    String storeName = Utils.getUniqueString("test-store");
    assertCommand(parentControllerClient.createNewStore(storeName, "owner", "\"string\"", "\"string\""));
    VersionCreationResponse versionCreationResponse =
        parentControllerClient.emptyPush(storeName, Utils.getUniqueString(storeName), 1024);
    Assert.assertFalse(versionCreationResponse.isError(), versionCreationResponse.getError());
    try {
      String kafkaTopic = versionCreationResponse.getKafkaTopic();
      Assert.assertNotNull(
          kafkaTopic,
          "parentControllerClient.emptyPush should not return a null topic name\n" + versionCreationResponse);

      String store = Version.parseStoreFromKafkaTopicName(kafkaTopic);
      int version = Version.parseVersionFromKafkaTopicName(kafkaTopic);
      MultiReplicaResponse response = controllerClient.listReplicas(store, version);
      Assert.assertFalse(response.isError(), response.getError());
      int totalReplicasCount = versionCreationResponse.getPartitions() * versionCreationResponse.getReplicas();
      Assert.assertEquals(response.getReplicas().length, totalReplicasCount, "Replica count does not match");
    } finally {
      deleteStore(storeName);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanCreateNewStore() throws IOException, ExecutionException, InterruptedException {
    String storeToCreate = "newTestStore123";
    String keySchema = "\"string\"";
    String valueSchema = "\"long\"";

    // create Store
    NewStoreResponse newStoreResponse =
        parentControllerClient.createNewStore(storeToCreate, "owner", keySchema, valueSchema);
    Assert.assertFalse(newStoreResponse.isError(), "create new store should succeed for a store that doesn't exist");
    try {
      NewStoreResponse duplicateNewStoreResponse =
          parentControllerClient.createNewStore(storeToCreate, "owner", keySchema, valueSchema);
      Assert
          .assertTrue(duplicateNewStoreResponse.isError(), "create new store should fail for duplicate store creation");

      // ensure creating a duplicate store throws a http 409, status code isn't exposed in controllerClient
      CloseableHttpAsyncClient httpClient = HttpClientUtils.getMinimalHttpClient(1, 1, Optional.empty());
      httpClient.start();
      List<NameValuePair> params = new ArrayList<>();
      params.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, venice.getClusterNames()[0]));
      params.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeToCreate));
      params.add(new BasicNameValuePair(ControllerApiConstants.OWNER, "owner"));
      params.add(new BasicNameValuePair(ControllerApiConstants.KEY_SCHEMA, keySchema));
      params.add(new BasicNameValuePair(ControllerApiConstants.VALUE_SCHEMA, valueSchema));
      final HttpPost post = new HttpPost(venice.getControllerConnectString() + ControllerRoute.NEW_STORE.getPath());
      post.setEntity(new UrlEncodedFormEntity(params));
      HttpResponse duplicateStoreCreationHttpResponse = httpClient.execute(post, null).get();
      Assert.assertEquals(
          duplicateStoreCreationHttpResponse.getStatusLine().getStatusCode(),
          409,
          IOUtils.toString(duplicateStoreCreationHttpResponse.getEntity().getContent()));
      httpClient.close();
    } finally {
      // clear the store since the cluster is shared by other test cases
      deleteStore(storeToCreate);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testAggregatedHealthStatusCall() throws IOException, ExecutionException, InterruptedException {
    String clusterName = venice.getClusterNames()[0];
    CloseableHttpAsyncClient httpClient = HttpClientUtils.getMinimalHttpClient(1, 1, Optional.empty());
    httpClient.start();
    int serverPort = venice.getChildRegions().get(0).getClusters().get(clusterName).getVeniceServers().get(0).getPort();
    String server = Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort);

    // API call with all fields
    Map<String, Object> payloads = new HashMap<>();
    payloads.put("cluster_id", clusterName);
    payloads.put("instances", Collections.singletonList(server));
    payloads.put("to_be_stopped_instances", Collections.emptyList());

    InstanceRemovableStatuses statuses = makeAggregatedHealthStatusCall(httpClient, payloads);
    Assert.assertTrue(statuses.getNonStoppableInstancesWithReasons().containsKey(server));

    // API call without optional to_be_stopped_instances
    Map<String, Object> payloads2 = new HashMap<>();
    payloads2.put("cluster_id", clusterName);
    payloads2.put("instances", Collections.singletonList(server));

    InstanceRemovableStatuses statuses2 = makeAggregatedHealthStatusCall(httpClient, payloads2);
    Assert.assertTrue(statuses2.getNonStoppableInstancesWithReasons().containsKey(server));

    httpClient.close();
  }

  private InstanceRemovableStatuses makeAggregatedHealthStatusCall(
      HttpAsyncClient httpClient,
      Map<String, Object> payloads) throws IOException, ExecutionException, InterruptedException {
    StringEntity entity = new StringEntity(OBJECT_MAPPER.writeValueAsString(payloads), ContentType.APPLICATION_JSON);

    final HttpPost post = new HttpPost(
        venice.getChildRegions().get(0).getControllerConnectString()
            + ControllerRoute.AGGREGATED_HEALTH_STATUS.getPath());
    post.setEntity(entity);
    HttpResponse httpResponse = httpClient.execute(post, null).get();

    Assert.assertEquals(httpResponse.getStatusLine().getStatusCode(), 200);
    String responseString = IOUtils.toString(httpResponse.getEntity().getContent());
    return OBJECT_MAPPER.readValue(responseString, InstanceRemovableStatuses.class);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientGetKeySchema() {
    String storeToCreate = Utils.getUniqueString("newTestStore125");
    String keySchemaStr = "\"string\"";
    String valueSchemaStr = "\"long\"";
    // Get key schema from non-existed store
    SchemaResponse sr0 = controllerClient.getKeySchema(storeToCreate);
    Assert.assertTrue(sr0.isError());
    // Create Store
    assertCommand(parentControllerClient.createNewStore(storeToCreate, "owner", keySchemaStr, valueSchemaStr));
    try {
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
        SchemaResponse sr1 = assertCommand(controllerClient.getKeySchema(storeToCreate));
        Assert.assertEquals(sr1.getId(), 1);
        Assert.assertEquals(sr1.getSchemaStr(), keySchemaStr);
      });
    } finally {
      // clear the store since the cluster is shared by other test cases
      deleteStore(storeToCreate);
    }
  }

  private String formatSchema(String schema) {
    return new Schema.Parser().parse(schema).toString();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientManageValueSchema() {
    String storeToCreate = Utils.getUniqueString("newTestStore");
    String keySchemaStr = "\"string\"";
    String schemaPrefix = "        {\n" + "           \"type\": \"record\",\n" + "           \"name\": \"KeyRecord\",\n"
        + "           \"fields\" : [\n"
        + "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n"
        + "               {\"name\": \"company\", \"type\": \"string\"},\n" + "               {\n"
        + "                 \"name\": \"Suit\", \n" + "                 \"type\": {\n"
        + "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"HEART\", \"CLUBS\"]\n"
        + "                }\n" + "              },\n";

    String schemaSuffix = "           ]\n" + "        }";
    String salaryFieldWithoutDefault = "               {\"name\": \"salary\", \"type\": \"long\"}\n";

    String salaryFieldWithDefault = "               {\"name\": \"salary\", \"type\": \"long\", \"default\": 123 }\n";

    String schemaStr =
        "{\"type\":\"record\",\"name\":\"KeyRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"name field\"},{\"name\":\"experience\",\"type\":[\"int\", \"float\", \"null\"], \"default\" : null},{\"name\":\"company\",\"type\":\"string\"}]}";

    String schema1 = formatSchema(schemaPrefix + salaryFieldWithoutDefault + schemaSuffix);
    String schema2 = formatSchema(schemaPrefix + salaryFieldWithDefault + schemaSuffix);
    String invalidSchema = "abc";
    String incompatibleSchema = "\"string\"";

    // Add value schema to non-existed store
    SchemaResponse sr0 = parentControllerClient.addValueSchema(storeToCreate, schema1);
    Assert.assertTrue(sr0.isError());
    // Add value schema to an existing store
    NewStoreResponse newStoreResponse =
        parentControllerClient.createNewStore(storeToCreate, "owner", keySchemaStr, schema1);
    Assert.assertFalse(newStoreResponse.isError(), "create new store should succeed for a store that doesn't exist");
    try {
      SchemaResponse sr1 = parentControllerClient.addValueSchema(storeToCreate, schema1);
      Assert.assertFalse(sr1.isError());
      Assert.assertEquals(sr1.getId(), 1);
      // Add same value schema
      SchemaResponse sr2 = parentControllerClient.addValueSchema(storeToCreate, schema1);
      Assert.assertFalse(sr2.isError());
      Assert.assertEquals(sr2.getId(), sr1.getId());
      // Add a new value schema
      SchemaResponse sr3 = parentControllerClient.addValueSchema(storeToCreate, schema2);
      Assert.assertFalse(sr3.isError());
      Assert.assertEquals(sr3.getId(), 2);
      // Add invalid schema
      SchemaResponse sr4 = parentControllerClient.addValueSchema(storeToCreate, invalidSchema);
      Assert.assertTrue(sr4.isError());
      // Add incompatible schema
      SchemaResponse sr5 = parentControllerClient.addValueSchema(storeToCreate, incompatibleSchema);
      Assert.assertTrue(sr5.isError());
      Assert.assertEquals(sr5.getErrorType(), ErrorType.INVALID_SCHEMA);
      Assert.assertEquals(sr5.getExceptionType(), ExceptionType.INVALID_SCHEMA);

      // Formatted schema string
      String formattedSchemaStr1 = formatSchema(schema1);
      String formattedSchemaStr2 = formatSchema(schema2);
      // Get schema by id
      SchemaResponse sr6 = parentControllerClient.getValueSchema(storeToCreate, 1);
      Assert.assertFalse(sr6.isError());
      Assert.assertEquals(sr6.getSchemaStr(), formattedSchemaStr1);
      SchemaResponse sr7 = parentControllerClient.getValueSchema(storeToCreate, 2);
      Assert.assertFalse(sr7.isError());
      Assert.assertEquals(sr7.getSchemaStr(), formattedSchemaStr2);
      // Get schema by non-existed schema id
      SchemaResponse sr8 = parentControllerClient.getValueSchema(storeToCreate, 3);
      Assert.assertTrue(sr8.isError());

      // Get value schema by schema
      SchemaResponse sr9 = parentControllerClient.getValueSchemaID(storeToCreate, schema1);
      Assert.assertFalse(sr9.isError());
      Assert.assertEquals(sr9.getId(), 1);
      SchemaResponse sr10 = parentControllerClient.getValueSchemaID(storeToCreate, schema2);
      Assert.assertFalse(sr10.isError());
      Assert.assertEquals(sr10.getId(), 2);
      SchemaResponse sr11 = parentControllerClient.getValueSchemaID(storeToCreate, invalidSchema);
      Assert.assertTrue(sr11.isError());
      SchemaResponse sr12 = parentControllerClient.getValueSchemaID(storeToCreate, incompatibleSchema);
      Assert.assertTrue(sr12.isError());

      // Get all value schema
      MultiSchemaResponse msr = parentControllerClient.getAllValueSchema(storeToCreate);
      Assert.assertFalse(msr.isError());
      MultiSchemaResponse.Schema[] schemas = msr.getSchemas();
      Assert.assertEquals(schemas.length, 2);
      Assert.assertEquals(schemas[0].getId(), 1);
      Assert.assertEquals(schemas[0].getSchemaStr(), formattedSchemaStr1);
      Assert.assertEquals(schemas[1].getId(), 2);
      Assert.assertEquals(schemas[1].getSchemaStr(), formattedSchemaStr2);

      // Add way more schemas, to test for the bug where we ordered schemas lexicographically: 1, 10, 11, 2, 3, ...
      String[] allSchemas = new String[100];
      allSchemas[0] = schema1;
      allSchemas[1] = schema2;
      StringBuilder prefixForLotsOfSchemas = new StringBuilder(schemaPrefix + salaryFieldWithDefault);

      // add incorrect schema
      sr1 = parentControllerClient.addValueSchema(storeToCreate, schemaStr);
      Assert.assertTrue(sr1.isError());
      for (int i = 3; i < allSchemas.length; i++) {
        prefixForLotsOfSchemas.append("," + "               {\"name\": \"newField")
            .append(i)
            .append("\", \"type\": \"long\", \"default\": 123 }\n");
        String schema = formatSchema(prefixForLotsOfSchemas + schemaSuffix);
        allSchemas[i - 1] = schema;
        SchemaResponse sr = parentControllerClient.addValueSchema(storeToCreate, schema);
        Assert.assertFalse(sr.isError());
        Assert.assertEquals(sr.getId(), i);

        // At each new schema we create, we test that the ordering is correct
        MultiSchemaResponse msr2 = parentControllerClient.getAllValueSchema(storeToCreate);
        Assert.assertFalse(msr2.isError());
        MultiSchemaResponse.Schema[] schemasFromController = msr2.getSchemas();
        Assert.assertEquals(
            schemasFromController.length,
            i,
            "getAllValueSchema request should return " + i + " schemas.");

        for (int j = 1; j <= i; j++) {
          Assert.assertEquals(
              schemasFromController[j - 1].getId(),
              j,
              "getAllValueSchema request should return the right schema ID for item " + j + " after " + i
                  + " schemas have been created.");
          Assert.assertEquals(
              schemasFromController[j - 1].getSchemaStr(),
              allSchemas[j - 1],
              "getAllValueSchema request should return the right schema string for item " + j + " after " + i
                  + " schemas have been created.");
        }
      }
    } finally {
      // clear the store since the cluster is shared by other test cases
      deleteStore(storeToCreate);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientSchemaOperationsAgainstInvalidStore() {
    String schema1 = "\"string\"";
    // Verify getting operations against non-existed store
    String nonExistedStore = Utils.getUniqueString("test2434095i02");
    SchemaResponse sr1 = controllerClient.getValueSchema(nonExistedStore, 1);
    Assert.assertTrue(sr1.isError());
    SchemaResponse sr2 = controllerClient.getValueSchemaID(nonExistedStore, schema1);
    Assert.assertTrue(sr2.isError());
    MultiSchemaResponse msr1 = controllerClient.getAllValueSchema(nonExistedStore);
    Assert.assertTrue(msr1.isError());
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanGetStoreInfo() {
    String storeName = Utils.getUniqueString("test-store");
    assertCommand(parentControllerClient.createNewStore(storeName, "owner", "\"string\"", "\"string\""));
    VersionCreationResponse versionCreationResponse =
        parentControllerClient.emptyPush(storeName, Utils.getUniqueString(storeName), 1024);
    Assert.assertFalse(versionCreationResponse.isError(), versionCreationResponse.getError());
    try {
      StoreResponse storeResponse = controllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError(), storeResponse.getError());

      StoreInfo store = storeResponse.getStore();
      Assert.assertEquals(
          parentController.getVeniceAdmin().getBackupVersionDefaultRetentionMs(),
          store.getBackupVersionRetentionMs(),
          "Store Info should have correct default retention time in ms.");
      Assert.assertEquals(
          parentController.getVeniceAdmin().getDefaultMaxRecordSizeBytes(),
          TEST_MAX_RECORD_SIZE_BYTES,
          "Default max record size bytes setting should've been correctly set by the test.");
      Assert.assertEquals(
          store.getMaxRecordSizeBytes(),
          TEST_MAX_RECORD_SIZE_BYTES,
          "Store Info should have the same default max record size in bytes.");
      Assert.assertEquals(store.getName(), storeName, "Store Info should have same store name as request");
      Assert.assertTrue(store.isEnableStoreWrites(), "New store should not be disabled");
      Assert.assertTrue(store.isEnableStoreReads(), "New store should not be disabled");
      List<Version> versions = store.getVersions();
      Assert.assertEquals(versions.size(), 1, " Store from new store-version should only have one version");
    } finally {
      deleteStore(storeName);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanDisableStoresWrite() {
    String storeName = Utils.getUniqueString("test-store");
    assertCommand(parentControllerClient.createNewStore(storeName, "owner", "\"string\"", "\"string\""));
    VersionCreationResponse versionCreationResponse =
        parentControllerClient.emptyPush(storeName, Utils.getUniqueString(storeName), 1024);
    Assert.assertFalse(versionCreationResponse.isError(), versionCreationResponse.getError());
    try {
      StoreInfo store = controllerClient.getStore(storeName).getStore();
      Assert.assertTrue(store.isEnableStoreWrites(), "Store should NOT be disabled after creating new store-version");

      ControllerResponse response = controllerClient.enableStoreWrites(storeName, false);
      Assert.assertFalse(response.isError(), response.getError());

      store = controllerClient.getStore(storeName).getStore();
      Assert.assertFalse(store.isEnableStoreWrites(), "Store should be disabled after setting disabled status to true");
    } finally {
      deleteStore(storeName);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanDisableStoresRead() {
    String storeName = Utils.getUniqueString("test-store");
    assertCommand(parentControllerClient.createNewStore(storeName, "owner", "\"string\"", "\"string\""));
    VersionCreationResponse versionCreationResponse =
        parentControllerClient.emptyPush(storeName, Utils.getUniqueString(storeName), 1024);
    Assert.assertFalse(versionCreationResponse.isError(), versionCreationResponse.getError());
    try {
      StoreInfo store = controllerClient.getStore(storeName).getStore();
      Assert.assertTrue(store.isEnableStoreReads(), "Store should NOT be disabled after creating new store-version");

      ControllerResponse response = controllerClient.enableStoreReads(storeName, false);
      Assert.assertFalse(response.isError(), response.getError());

      store = controllerClient.getStore(storeName).getStore();
      Assert.assertFalse(store.isEnableStoreReads(), "Store should be disabled after setting disabled status to true");
    } finally {
      // clear the store since the cluster is shared by other test cases
      deleteStore(storeName);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanDisableStoresReadWrite() {
    String storeName = Utils.getUniqueString("test-store");
    assertCommand(parentControllerClient.createNewStore(storeName, "owner", "\"string\"", "\"string\""));
    VersionCreationResponse versionCreationResponse =
        parentControllerClient.emptyPush(storeName, Utils.getUniqueString(storeName), 1024);
    Assert.assertFalse(versionCreationResponse.isError(), versionCreationResponse.getError());
    try {
      StoreInfo store = controllerClient.getStore(storeName).getStore();
      Assert.assertTrue(store.isEnableStoreReads(), "Store should NOT be disabled after creating new store-version");
      Assert.assertTrue(store.isEnableStoreWrites(), "Store should NOT be disabled after creating new store-version");

      ControllerResponse response = controllerClient.enableStoreReadWrites(storeName, false);
      Assert.assertFalse(response.isError(), response.getError());

      store = controllerClient.getStore(storeName).getStore();
      Assert.assertFalse(store.isEnableStoreReads(), "Store should be disabled after setting disabled status to true");
      Assert.assertFalse(store.isEnableStoreWrites(), "Store should be disabled after setting disabled status to true");
    } finally {
      deleteStore(storeName);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanSetStoreMetadata() {
    String storeName = Utils.getUniqueString("store");
    String owner = Utils.getUniqueString("owner");
    int partitionCount = 2;

    assertCommand(parentControllerClient.createNewStore(storeName, owner, "\"string\"", "\"string\""));
    VersionCreationResponse versionCreationResponse =
        parentControllerClient.emptyPush(storeName, Utils.getUniqueString(storeName), 1024);
    Assert.assertFalse(versionCreationResponse.isError(), versionCreationResponse.getError());
    try {
      OwnerResponse ownerRes = controllerClient.setStoreOwner(storeName, owner);
      Assert.assertFalse(ownerRes.isError(), ownerRes.getError());
      Assert.assertEquals(ownerRes.getOwner(), owner);

      UpdateStoreQueryParams updateStoreQueryParams =
          new UpdateStoreQueryParams().setPartitionCount(partitionCount).setIncrementalPushEnabled(true);
      ControllerResponse partitionRes = parentControllerClient.updateStore(storeName, updateStoreQueryParams);
      Assert.assertFalse(partitionRes.isError(), partitionRes.getError());

      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
        StoreResponse storeResponse = controllerClient.getStore(storeName);
        Assert.assertEquals(storeResponse.getStore().getPartitionCount(), partitionCount);
        Assert.assertTrue(storeResponse.getStore().isIncrementalPushEnabled());
      });
    } finally {
      deleteStore(storeName);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanQueryRemovability() {
    VeniceMultiClusterWrapper multiClusterWrapper = venice.getChildRegions().get(0);
    String clusterName = multiClusterWrapper.getClusterNames()[0];
    VeniceClusterWrapper venice = multiClusterWrapper.getClusters().get(clusterName);
    VeniceServerWrapper server = venice.getVeniceServers().get(0);
    String nodeId = Utils.getHelixNodeIdentifier(Utils.getHostName(), server.getPort());

    ControllerResponse response = controllerClient.isNodeRemovable(nodeId);
    Assert.assertFalse(response.isError(), response.getError());
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanDeleteAllVersion() {
    String storeName = Utils.getUniqueString("test-store");
    assertCommand(parentControllerClient.createNewStore(storeName, "owner", "\"string\"", "\"string\""));
    VersionCreationResponse versionCreationResponse =
        parentControllerClient.emptyPush(storeName, Utils.getUniqueString(storeName), 1024);
    Assert.assertFalse(versionCreationResponse.isError(), versionCreationResponse.getError());
    try {
      controllerClient.enableStoreReads(storeName, false);
      controllerClient.enableStoreWrites(storeName, false);

      MultiVersionResponse deleteVersionsResponse = controllerClient.deleteAllVersions(storeName);
      Assert.assertFalse(deleteVersionsResponse.isError(), deleteVersionsResponse.getError());
      Assert.assertEquals(
          deleteVersionsResponse.getExecutionId(),
          0,
          "The command executed in non-parent controller should have an execution id 0");

      StoreResponse storeResponse = controllerClient.getStore(storeName);
      Assert.assertFalse(storeResponse.isError(), storeResponse.getError());
      Assert.assertEquals(storeResponse.getStore().getVersions().size(), 0);
    } finally {
      deleteStore(storeName);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanDeleteOldVersion() {
    String storeName = Utils.getUniqueString("test-store");
    assertCommand(parentControllerClient.createNewStore(storeName, "owner", "\"string\"", "\"string\""));

    // Create two versions. version 1 will be backup, and version 2 will be current
    assertCommand(
        parentControllerClient
            .sendEmptyPushAndWait(storeName, Utils.getUniqueString(storeName), 1024, 10 * Time.MS_PER_SECOND));
    assertCommand(
        parentControllerClient
            .sendEmptyPushAndWait(storeName, Utils.getUniqueString(storeName), 1024, 10 * Time.MS_PER_SECOND));
    try {
      VersionResponse response = assertCommand(controllerClient.deleteOldVersion(storeName, 1));
      Assert.assertEquals(response.getVersion(), 1);

      StoreInfo store = controllerClient.getStore(storeName).getStore();
      Assert.assertEquals(store.getVersions().size(), 1);
    } finally {
      deleteStore(storeName);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanGetLastSucceedExecutionId() {
    LastSucceedExecutionIdResponse response = controllerClient.getLastSucceedExecutionId();
    Assert.assertFalse(response.isError());
    Assert.assertTrue(response.getLastSucceedExecutionId() > -1);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanGetExecutionOfDeleteAllVersions() {
    String clusterName = venice.getClusterNames()[0];
    String storeName = Utils.getUniqueString("controllerClientCanDeleteAllVersion");

    parentController.getVeniceAdmin().createStore(clusterName, storeName, "test", "\"string\"", "\"string\"");
    parentController.getVeniceAdmin()
        .incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(), 1, 1);

    try {
      parentControllerClient.enableStoreReads(storeName, false);
      parentControllerClient.enableStoreWrites(storeName, false);

      MultiVersionResponse multiVersionResponse = parentControllerClient.deleteAllVersions(storeName);
      long executionId = multiVersionResponse.getExecutionId();

      AdminCommandExecutionResponse response = parentControllerClient.getAdminCommandExecution(executionId);
      Assert.assertFalse(response.isError());
      Assert.assertNotNull(response.getExecution());
    } finally {
      deleteStore(storeName);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanListStoresStatuses() {
    List<String> storeNames = new ArrayList<>();
    String storePrefix = "controllerClientCanListStoresStatusesTestStore";
    int storeCount = 2;
    for (int i = 0; i < storeCount; i++) {
      String storeName = Utils.getUniqueString(storePrefix);
      assertCommand(parentControllerClient.createNewStore(storeName, "owner", "\"string\"", "\"string\""));
      storeNames.add(storeName);
    }

    try {
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
        MultiStoreStatusResponse storeResponse = assertCommand(controllerClient.listStoresStatuses());
        // since all test cases share VeniceClusterWrapper, we get the total number of stores from the Wrapper.
        List<String> storesInCluster = new ArrayList<>(storeResponse.getStoreStatusMap().keySet());
        for (String storeName: storeNames) {
          Assert.assertTrue(
              storesInCluster.contains(storeName),
              "Result of listing store status should contain all stores we created.");
        }
        List<String> storeStatuses = storeResponse.getStoreStatusMap()
            .entrySet()
            .stream()
            .filter(e -> e.getKey().contains(storePrefix))
            .map(Map.Entry::getValue)
            .collect(Collectors.toList());
        Assert.assertEquals(storeStatuses.size(), storeCount);
        for (String status: storeStatuses) {
          Assert.assertEquals(
              status,
              StoreStatus.UNAVAILABLE.toString(),
              "Store should be unavailable because we have not created a version for this store. "
                  + storeResponse.getStoreStatusMap());
        }
        for (String expectedStore: storeNames) {
          Assert.assertTrue(
              storeResponse.getStoreStatusMap().containsKey(expectedStore),
              "Result of list store status should contain the store we created: " + expectedStore);
        }
      });
    } finally {
      storeNames.forEach(this::deleteStore);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanListFutureStoreVersions() {
    String clusterName = venice.getClusterNames()[0];
    List<String> storeNames = new ArrayList<>();
    try {
      String storeName = Utils.getUniqueString("testStore");
      NewStoreResponse newStoreResponse =
          assertCommand(parentControllerClient.createNewStore(storeName, "owner", "\"string\"", "\"string\""));
      storeNames.add(newStoreResponse.getName());
      MultiStoreStatusResponse storeResponse =
          assertCommand(parentControllerClient.getFutureVersions(clusterName, storeNames.get(0)));

      // There's no version for this store and no future version coming, so we expect an entry with
      // Store.NON_EXISTING_VERSION
      Assert.assertTrue(storeResponse.getStoreStatusMap().containsKey("dc-0"));
      Assert.assertEquals(storeResponse.getStoreStatusMap().get("dc-0"), String.valueOf(Store.NON_EXISTING_VERSION));
    } finally {
      storeNames.forEach(this::deleteStore);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanUpdateAllowList() {
    String clusterName = venice.getClusterNames()[0];
    Admin admin = venice.getChildRegions().get(0).getLeaderController(clusterName).getVeniceAdmin();

    String nodeId = Utils.getHelixNodeIdentifier(Utils.getHostName(), 34567);
    Assert
        .assertFalse(admin.getAllowlist(clusterName).contains(nodeId), nodeId + " has not been added into allowlist.");
    controllerClient.addNodeIntoAllowList(nodeId);
    Assert.assertTrue(admin.getAllowlist(clusterName).contains(nodeId), nodeId + " has been added into allowlist.");
    controllerClient.removeNodeFromAllowList(nodeId);
    Assert.assertFalse(admin.getAllowlist(clusterName).contains(nodeId), nodeId + " has been removed from allowlist.");
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanSetStore() {
    // mutable store metadata
    String owner = Utils.getUniqueString("owner");
    int partitionCount = 2;
    int current = 1;
    boolean enableReads = false;
    boolean enableWrite = true;
    boolean accessControlled = true;
    long storageQuotaInByte = 100L;
    long readQuotaInCU = 200L;
    int numVersionToPreserve = 100;

    String storeName = Utils.getUniqueString("test-store");
    assertCommand(parentControllerClient.createNewStore(storeName, "owner", "\"string\"", "\"string\""));
    VersionCreationResponse versionCreationResponse =
        parentControllerClient.emptyPush(storeName, Utils.getUniqueString(storeName), 1024);
    Assert.assertFalse(versionCreationResponse.isError(), versionCreationResponse.getError());
    // Disable writes at first and test could we enable writes again through the update store method.
    Assert.assertFalse(
        controllerClient.enableStoreReadWrites(storeName, false).isError(),
        "Disable writes should not fail.");

    UpdateStoreQueryParams queryParams = new UpdateStoreQueryParams().setOwner(owner)
        .setPartitionCount(partitionCount)
        .setCurrentVersion(current)
        .setEnableReads(enableReads)
        .setEnableWrites(enableWrite)
        .setStorageQuotaInByte(storageQuotaInByte)
        .setReadQuotaInCU(readQuotaInCU)
        .setAccessControlled(accessControlled)
        .setNumVersionsToPreserve(numVersionToPreserve);

    VeniceMultiClusterWrapper multiClusterWrapper = venice.getChildRegions().get(0);
    String clusterName = venice.getClusterNames()[0];
    VeniceClusterWrapper cluster = multiClusterWrapper.getClusters().get(clusterName);

    try {
      ControllerResponse response = controllerClient.updateStore(storeName, queryParams);

      Assert.assertFalse(response.isError(), response.getError());
      Store store = cluster.getLeaderVeniceController().getVeniceAdmin().getStore(cluster.getClusterName(), storeName);
      Assert.assertEquals(store.getOwner(), owner);
      Assert.assertEquals(store.getPartitionCount(), partitionCount);
      Assert.assertEquals(store.getCurrentVersion(), current);
      Assert.assertEquals(store.isEnableReads(), enableReads);
      Assert.assertEquals(store.isEnableWrites(), enableWrite);
      Assert.assertEquals(store.isAccessControlled(), accessControlled);
      Assert.assertEquals(store.getNumVersionsToPreserve(), numVersionToPreserve);

      enableWrite = false;
      accessControlled = !accessControlled;
      queryParams = new UpdateStoreQueryParams().setEnableWrites(enableWrite).setAccessControlled(accessControlled);
      Assert.assertFalse(
          controllerClient.updateStore(storeName, queryParams).isError(),
          "We should be able to disable store writes again.");

      store = cluster.getLeaderVeniceController().getVeniceAdmin().getStore(cluster.getClusterName(), storeName);
      Assert.assertEquals(store.isAccessControlled(), accessControlled);
    } finally {
      deleteStore(storeName);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanSetStoreMissingSomeFields() {
    String storeName = null;
    try {
      // partial metadata
      int partitionCount = 2;
      int current = 1;
      boolean enableReads = false;

      storeName = Utils.getUniqueString("test-store");
      assertCommand(parentControllerClient.createNewStore(storeName, "owner", "\"string\"", "\"string\""));
      VersionCreationResponse versionCreationResponse =
          parentControllerClient.emptyPush(storeName, Utils.getUniqueString(storeName), 1024);
      Assert.assertFalse(versionCreationResponse.isError(), versionCreationResponse.getError());
      ControllerResponse response = controllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setPartitionCount(partitionCount)
              .setCurrentVersion(current)
              .setEnableReads(enableReads));

      Assert.assertFalse(response.isError(), response.getError());
      StoreInfo store = controllerClient.getStore(storeName).getStore();
      Assert.assertEquals(store.getPartitionCount(), partitionCount);
      Assert.assertEquals(store.getCurrentVersion(), current);
      Assert.assertEquals(store.isEnableStoreReads(), enableReads);
    } finally {
      if (storeName != null) {
        deleteStore(storeName);
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void canCreateAHybridStore() {
    String storeName = Utils.getUniqueString("store");
    String owner = Utils.getUniqueString("owner");
    parentControllerClient.createNewStore(storeName, owner, "\"string\"", "\"string\"");
    try {
      parentControllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setHybridRewindSeconds(123L).setHybridOffsetLagThreshold(1515L));

      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
        StoreResponse storeResponse = controllerClient.getStore(storeName);
        HybridStoreConfig hybridStoreConfig = storeResponse.getStore().getHybridStoreConfig();
        Assert.assertNotNull(hybridStoreConfig);
        Assert.assertEquals(hybridStoreConfig.getRewindTimeInSeconds(), 123L);
        Assert.assertEquals(hybridStoreConfig.getOffsetLagThresholdToGoOnline(), 1515L);
      });
    } finally {
      deleteStore(storeName);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanGetStorageEngineOverheadRatio() {
    String storeName = Utils.getUniqueString("test-store");
    assertCommand(parentControllerClient.createNewStore(storeName, "owner", "\"string\"", "\"string\""));
    VersionCreationResponse versionCreationResponse =
        parentControllerClient.emptyPush(storeName, Utils.getUniqueString(storeName), 1024);
    Assert.assertFalse(versionCreationResponse.isError(), versionCreationResponse.getError());
    try {
      StorageEngineOverheadRatioResponse response = controllerClient.getStorageEngineOverheadRatio(storeName);

      Assert.assertFalse(response.isError(), response.getError());
      Assert.assertEquals(
          response.getStorageEngineOverheadRatio(),
          VeniceControllerWrapper.DEFAULT_STORAGE_ENGINE_OVERHEAD_RATIO);
    } finally {
      deleteStore(storeName);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanDeleteStore() {
    String storeName = Utils.getUniqueString("test-store");
    assertCommand(parentControllerClient.createNewStore(storeName, "owner", "\"string\"", "\"string\""));
    VersionCreationResponse versionCreationResponse =
        parentControllerClient.emptyPush(storeName, Utils.getUniqueString(storeName), 1024);
    Assert.assertFalse(versionCreationResponse.isError(), versionCreationResponse.getError());
    try {
      controllerClient.enableStoreReads(storeName, false);
      controllerClient.enableStoreWrites(storeName, false);

      TrackableControllerResponse response = controllerClient.deleteStore(storeName);
      Assert.assertFalse(response.isError(), response.getError());
      Assert.assertEquals(
          response.getExecutionId(),
          0,
          "The command executed in non-parent controller should have an execution id 0");

      StoreResponse storeResponse = controllerClient.getStore(storeName);
      Assert.assertTrue(storeResponse.isError(), "Store should already be deleted.");
    } finally {
      deleteStore(storeName);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanGetExecutionOfDeleteStore() {
    String clusterName = venice.getClusterNames()[0];

    String storeName = Utils.getUniqueString("controllerClientCanGetExecutionOfDeleteStore");
    parentController.getVeniceAdmin().createStore(clusterName, storeName, "test", "\"string\"", "\"string\"");

    parentController.getVeniceAdmin().incrementVersionIdempotent(clusterName, storeName, "test", 1, 1);

    try {
      parentControllerClient.enableStoreReads(storeName, false);
      parentControllerClient.enableStoreWrites(storeName, false);

      TrackableControllerResponse trackableControllerResponse = parentControllerClient.deleteStore(storeName);
      long executionId = trackableControllerResponse.getExecutionId();

      AdminCommandExecutionResponse response = parentControllerClient.getAdminCommandExecution(executionId);
      Assert.assertFalse(response.isError());
      AdminCommandExecution execution = response.getExecution();
      Assert.assertNotNull(execution);
    } finally {
      deleteStore(storeName);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientProvidesErrorWhenRequestingTopicForStoreThatDoesNotExist() throws IOException {
    String storeNameDoesNotExist = Utils.getUniqueString("no-store");
    String pushId = Utils.getUniqueString("no-store-push");

    VersionCreationResponse vcr = controllerClient.requestTopicForWrites(
        storeNameDoesNotExist,
        1L,
        Version.PushType.BATCH,
        pushId,
        true,
        true,
        false,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        false,
        -1);
    Assert.assertTrue(
        vcr.isError(),
        "Request topic for store that has not been created must return error, instead it returns: "
            + ObjectMapperFactory.getInstance().writeValueAsString(vcr));

    vcr = controllerClient.requestTopicForWrites(
        storeNameDoesNotExist,
        1L,
        Version.PushType.STREAM,
        pushId,
        true,
        false,
        false,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        false,
        -1);
    Assert.assertTrue(
        vcr.isError(),
        "Request topic for store that has not been created must return error, instead it returns: "
            + ObjectMapperFactory.getInstance().writeValueAsString(vcr));
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanEnableThrottling() {
    controllerClient.enableThrottling(false);
    Assert.assertFalse(controllerClient.getRoutersClusterConfig().getConfig().isThrottlingEnabled());
    controllerClient.enableThrottling(true);
    Assert.assertTrue(controllerClient.getRoutersClusterConfig().getConfig().isThrottlingEnabled());
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanEnableMaxCapacityProtection() {
    controllerClient.enableMaxCapacityProtection(false);
    Assert.assertFalse(controllerClient.getRoutersClusterConfig().getConfig().isMaxCapacityProtectionEnabled());
    controllerClient.enableMaxCapacityProtection(true);
    Assert.assertTrue(controllerClient.getRoutersClusterConfig().getConfig().isMaxCapacityProtectionEnabled());
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientCanDiscoverCluster() {
    String storeName = Utils.getUniqueString("controllerClientCanDiscoverCluster");
    controllerClient.createNewStore(storeName, "test", "\"string\"", "\"string\"");
    String clusterName = venice.getClusterNames()[0];
    try {
      Assert.assertEquals(
          ControllerClient
              .discoverCluster(
                  venice.getChildRegions().get(0).getControllerConnectString(),
                  storeName,
                  Optional.empty(),
                  1)
              .getCluster(),
          clusterName,
          "Should be able to find the cluster which the given store belongs to.");
    } finally {
      deleteStore(storeName);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerCanHandleLargePayload() throws IOException {
    String storeName = Utils.getUniqueString("controllerClientCanDiscoverCluster");
    String pushId = Utils.getUniqueString("no-store-push");

    byte[] largeDictionaryBytes = new byte[512 * ByteUtils.BYTES_PER_KB];
    Arrays.fill(largeDictionaryBytes, (byte) 1);

    String largeDictionary = EncodingUtils.base64EncodeToString(largeDictionaryBytes);

    parentControllerClient.createNewStore(storeName, "test", "\"string\"", "\"string\"");

    VersionCreationResponse vcr = parentControllerClient.requestTopicForWrites(
        storeName,
        1L,
        Version.PushType.BATCH,
        pushId,
        false,
        true,
        false,
        Optional.empty(),
        Optional.of(largeDictionary),
        Optional.empty(),
        false,
        -1);
    Assert.assertFalse(
        vcr.isError(),
        "Controller should allow large payload: " + ObjectMapperFactory.getInstance().writeValueAsString(vcr));
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerCanGetDeletableStoreTopics() {
    String storeName = Utils.getUniqueString("canGetDeletableStoreTopics");
    try {
      assertCommand(parentControllerClient.createNewStore(storeName, "test", "\"string\"", "\"string\""));
      String metaSystemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
      // Add some system store and RT topics in the mix to make sure the request can still return the right values.
      assertCommand(
          parentControllerClient
              .sendEmptyPushAndWait(metaSystemStoreName, "meta-store-push-1", 1024000L, 10 * Time.MS_PER_SECOND));

      // Store version topic v1 should be truncated after polling for completion by parent controller.
      assertCommand(
          parentControllerClient.sendEmptyPushAndWait(storeName, "push-1", 1024000L, 10 * Time.MS_PER_SECOND));

      assertCommand(
          parentControllerClient.sendEmptyPushAndWait(storeName, "push-2", 1024000L, 10 * Time.MS_PER_SECOND));

      assertCommand(parentControllerClient.deleteOldVersion(storeName, 1));
      // Wait for resource of v1 to be cleaned up since for child fabric we only consider a topic is deletable if its
      // resource is deleted.
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        StoreInfo storeInChildRegion = assertCommand(controllerClient.getStore(storeName)).getStore();
        Assert.assertFalse(storeInChildRegion.getVersion(1).isPresent());
      });
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        MultiStoreTopicsResponse childMultiStoreTopicResponse =
            assertCommand(controllerClient.getDeletableStoreTopics());
        Assert.assertTrue(childMultiStoreTopicResponse.getTopics().contains(Version.composeKafkaTopic(storeName, 1)));
        Assert.assertFalse(childMultiStoreTopicResponse.getTopics().contains(Version.composeKafkaTopic(storeName, 2)));
        Assert.assertFalse(
            childMultiStoreTopicResponse.getTopics().contains(Version.composeKafkaTopic(metaSystemStoreName, 1)));
        Assert.assertFalse(
            childMultiStoreTopicResponse.getTopics().contains(Utils.composeRealTimeTopic(metaSystemStoreName)));
      });
    } finally {
      deleteStore(storeName);
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void controllerClientReturns404ForNonexistentStoreQuery() {
    StoreResponse storeResponse = controllerClient.getStore("nonexistent");
    Assert.assertTrue(storeResponse.getError().contains("Http Status 404"));
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testDeleteKafkaTopic() {
    String clusterName = venice.getClusterNames()[0];
    String storeName = Utils.getUniqueString("controllerClientCanDeleteKafkaTopic");
    assertCommand(parentControllerClient.createNewStore(storeName, "owner", "\"string\"", "\"string\""));
    assertCommand(
        parentControllerClient.updateStore(
            storeName,
            new UpdateStoreQueryParams().setHybridRewindSeconds(1000).setHybridOffsetLagThreshold(1)));
    assertCommand(parentControllerClient.emptyPush(storeName, Utils.getUniqueString(storeName), 1));
    String topicToDelete = Version.composeKafkaTopic(storeName, 1);

    VeniceHelixAdmin childControllerAdmin =
        venice.getChildRegions().get(0).getLeaderController(clusterName).getVeniceHelixAdmin();
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Assert.assertTrue(
          childControllerAdmin.getTopicManager()
              .containsTopic(
                  venice.getChildRegions()
                      .get(0)
                      .getClusters()
                      .get(clusterName)
                      .getPubSubTopicRepository()
                      .getTopic(topicToDelete)));
      Assert.assertFalse(childControllerAdmin.isTopicTruncated(topicToDelete));
    });
    assertCommand(controllerClient.deleteKafkaTopic(topicToDelete));
    Assert.assertTrue(childControllerAdmin.isTopicTruncated(topicToDelete));
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testCleanupInstanceCustomizedStates() {
    String clusterName = venice.getClusterNames()[0];
    String storeName = Utils.getUniqueString("cleanupInstanceCustomizedStatesTest");
    VeniceHelixAdmin childControllerAdmin = venice.getChildRegions().get(0).getRandomController().getVeniceHelixAdmin();
    childControllerAdmin.createStore(clusterName, storeName, "test", "\"string\"", "\"string\"");
    Version version = childControllerAdmin.incrementVersionIdempotent(clusterName, storeName, "test", 1, 1);
    MultiStoreTopicsResponse response = assertCommand(controllerClient.cleanupInstanceCustomizedStates());
    Assert.assertNotNull(response.getTopics());
    for (String topic: response.getTopics()) {
      Assert.assertFalse(topic.endsWith("/" + version.kafkaTopicName()));
    }
  }

  private void deleteStore(String storeName) {
    parentControllerClient.enableStoreReadWrites(storeName, false);
    parentControllerClient.deleteStore(storeName);
  }
}
