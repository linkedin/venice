package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.AdminCommandExecution;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.controllerapi.MultiNodesStatusResponse;
import com.linkedin.venice.controllerapi.MultiNodeResponse;
import com.linkedin.venice.controllerapi.MultiReplicaResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.MultiStoreStatusResponse;
import com.linkedin.venice.controllerapi.MultiVersionResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.OwnerResponse;
import com.linkedin.venice.controllerapi.PartitionResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.controllerapi.routes.AdminCommandExecutionResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.InstanceStatus;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.StoreStatus;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.commons.cli.Option;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.message.BasicNameValuePair;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestAdminSparkServer {
  private VeniceClusterWrapper venice;
  private String routerUrl;
  private ControllerClient controllerClient;

  @BeforeClass
  public void setUp() {
    /*
     * Seems that Helix has limit on the number of resource each node is able to handle.
     * You might want to increase the number of SN in the future if new tests are kept adding
     * to this class. In the current, 2 nodes seems to be enough.
     *
     * A typical exception for the short of SN looks like "Failed to create helix resource."
     */

    venice = ServiceFactory.getVeniceCluster(1, 4, 1); //Controllers, Servers, Routers
    routerUrl = venice.getRandomRouterURL();
    controllerClient = new ControllerClient(venice.getClusterName(), routerUrl);
  }

  @AfterClass
  public void tearDown(){
    venice.close();
  }

  @Test
  public void controllerClientCanQueryNodesInCluster(){
    MultiNodeResponse nodeResponse = controllerClient.listStorageNodes();
    Assert.assertFalse(nodeResponse.isError(), nodeResponse.getError());
    Assert.assertEquals(nodeResponse.getNodes().length, 4, "Node count does not match");
  }

  @Test
  public void controllerClientCanQueryInstanceStatusInCluster() {
    MultiNodesStatusResponse nodeResponse = controllerClient.listInstancesStatuses();
    Assert.assertFalse(nodeResponse.isError(), nodeResponse.getError());
    Assert.assertEquals(nodeResponse.getInstancesStatusMap().size(), 4, "Node count does not match");
    Assert.assertEquals(nodeResponse.getInstancesStatusMap().values().iterator().next(),
        InstanceStatus.CONNECTED.toString(), "Node status does not match.");
  }

  @Test
  public void controllerClientCanQueryReplicasOnAStorageNode(){
    venice.getNewStoreVersion();
    MultiNodeResponse nodeResponse = controllerClient.listStorageNodes();
    String nodeId = nodeResponse.getNodes()[0];
    MultiReplicaResponse replicas = controllerClient.listStorageNodeReplicas(nodeId);
    Assert.assertFalse(replicas.isError(), replicas.getError());
  }

  @Test
  public void controllerClientCanQueryReplicasForTopic(){
    VersionCreationResponse versionCreationResponse = venice.getNewStoreVersion();
    Assert.assertFalse(versionCreationResponse.isError(), versionCreationResponse.getError());
    String kafkaTopic = venice.getNewStoreVersion().getKafkaTopic();
    Assert.assertNotNull(kafkaTopic, "venice.getNewStoreVersion() should not return a null topic name\n" + versionCreationResponse.toString());


    String store = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    int version = Version.parseVersionFromKafkaTopicName(kafkaTopic);
    MultiReplicaResponse response = controllerClient.listReplicas( store, version);
    Assert.assertFalse(response.isError(), response.getError());
    Assert.assertEquals(response.getReplicas().length, 10, "Replica count does not match"); /* 10 partitions, replication factor 1 */
  }

  @Test
  public void controllerClientCanCreateNewStore() throws IOException, ExecutionException, InterruptedException {
    String storeToCreate = "newTestStore123";
    String keySchema = "\"string\"";
    String valueSchema = "\"long\"";

    // create Store
    NewStoreResponse newStoreResponse =
        controllerClient.createNewStore(storeToCreate, "owner", "test", keySchema, valueSchema);
    Assert.assertFalse(newStoreResponse.isError(), "create new store should succeed for a store that doesn't exist");
    //if the store is not created by VeniceClusterWrapper, it has to be reported so other test cases will not be broken.
    venice.increaseStoreCount();

    NewStoreResponse duplicateNewStoreResponse =
        controllerClient.createNewStore(storeToCreate, "owner", "test", keySchema, valueSchema);
    Assert.assertTrue(duplicateNewStoreResponse.isError(), "create new store should fail for duplicate store creation");

    // ensure creating a duplicate store throws a http 409, status code isn't exposed in controllerClient
   CloseableHttpAsyncClient httpClient = SslUtils.getMinimalHttpClient(1,1, Optional.empty());
    httpClient.start();
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair(ControllerApiConstants.HOSTNAME, Utils.getHostName()));
    params.add(new BasicNameValuePair(ControllerApiConstants.CLUSTER, venice.getClusterName()));
    params.add(new BasicNameValuePair(ControllerApiConstants.NAME, storeToCreate));
    params.add(new BasicNameValuePair(ControllerApiConstants.OWNER, "owner"));
    params.add(new BasicNameValuePair(ControllerApiConstants.KEY_SCHEMA, keySchema));
    params.add(new BasicNameValuePair(ControllerApiConstants.VALUE_SCHEMA, valueSchema));
    final HttpPost post = new HttpPost(venice.getAllControllersURLs() + ControllerRoute.NEW_STORE.getPath());
    post.setEntity(new UrlEncodedFormEntity(params));
    HttpResponse duplicateStoreCreationHttpResponse = httpClient.execute(post, null).get();
    Assert.assertEquals(duplicateStoreCreationHttpResponse.getStatusLine().getStatusCode(), 409, IOUtils.toString(duplicateStoreCreationHttpResponse.getEntity().getContent()));
    httpClient.close();
  }

  @Test
  public void controllerClientGetKeySchema() {
    String storeToCreate = TestUtils.getUniqueString("newTestStore125");
    String keySchemaStr = "\"string\"";
    String valueSchemaStr = "\"long\"";
    // Get key schema from non-existed store
    SchemaResponse sr0 = controllerClient.getKeySchema(storeToCreate);
    Assert.assertTrue(sr0.isError());
    // Create Store
    NewStoreResponse newStoreResponse =
        controllerClient.createNewStore(storeToCreate, "owner", "test", keySchemaStr, valueSchemaStr);

    Assert.assertFalse(newStoreResponse.isError(), "create new store should succeed for a store that doesn't exist");
    SchemaResponse sr1 = controllerClient.getKeySchema(storeToCreate);
    Assert.assertEquals(sr1.getId(), 1);
    Assert.assertEquals(sr1.getSchemaStr(), keySchemaStr);
  }

  @Test
  public void controllerClientManageValueSchema() {
    String storeToCreate = TestUtils.getUniqueString("newTestStore");
    String keySchemaStr = "\"string\"";
    String schema1 = "{\n" +
        "           \"type\": \"record\",\n" +
        "           \"name\": \"KeyRecord\",\n" +
        "           \"fields\" : [\n" +
        "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n" +
        "               {\"name\": \"company\", \"type\": \"string\"},\n" +
        "               {\n" +
        "                 \"name\": \"Suit\", \n" +
        "                 \"type\": {\n" +
        "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"HEART\", \"CLUBS\"]\n" +
        "                }\n" +
        "              },\n" +
        "               {\"name\": \"salary\", \"type\": \"long\"}\n" +
        "           ]\n" +
        "        }";
    String schema2 = "{\n" +
        "           \"type\": \"record\",\n" +
        "           \"name\": \"KeyRecord\",\n" +
        "           \"fields\" : [\n" +
        "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n" +
        "               {\"name\": \"company\", \"type\": \"string\"},\n" +
        "               {\n" +
        "                 \"name\": \"Suit\", \n" +
        "                 \"type\": {\n" +
        "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"CLUBS\", \"HEART\"]\n" +
        "                } \n" +
        "              },\n" +
        "               {\"name\": \"salary\", \"type\": \"long\", \"default\": 123 }" +
        "           ]\n" +
        "        }";
    String schema3 = "abc";
    String schema4 = "\"string\"";

    // Add value schema to non-existed store
    SchemaResponse sr0 = controllerClient.addValueSchema(storeToCreate, schema1);
    Assert.assertTrue(sr0.isError());
    // Add value schema to an existing store
    NewStoreResponse newStoreResponse = controllerClient.createNewStore(storeToCreate, "owner", "test", keySchemaStr, schema1);
    Assert.assertFalse(newStoreResponse.isError(), "create new store should succeed for a store that doesn't exist");
    SchemaResponse sr1 = controllerClient.addValueSchema(storeToCreate, schema1);
    Assert.assertFalse(sr1.isError());
    Assert.assertEquals(sr1.getId(), 1);
    // Add same value schema
    SchemaResponse sr2 = controllerClient.addValueSchema(storeToCreate, schema1);
    Assert.assertFalse(sr2.isError());
    Assert.assertEquals(sr2.getId(), sr1.getId());
    // Add a new value schema
    SchemaResponse sr3 = controllerClient.addValueSchema(storeToCreate, schema2);
    Assert.assertFalse(sr3.isError());
    Assert.assertEquals(sr3.getId(), 2);
    // Add invalid schema
    SchemaResponse sr4 = controllerClient.addValueSchema(storeToCreate, schema3);
    Assert.assertTrue(sr4.isError());
    // Add incompatible schema
    SchemaResponse sr5 = controllerClient.addValueSchema(storeToCreate, schema4);
    Assert.assertTrue(sr5.isError());

    // Formatted schema string
    String formattedSchemaStr1 = Schema.parse(schema1).toString();
    String formattedSchemaStr2 = Schema.parse(schema2).toString();
    // Get schema by id
    SchemaResponse sr6 = controllerClient.getValueSchema(storeToCreate, 1);
    Assert.assertFalse(sr6.isError());
    Assert.assertEquals(sr6.getSchemaStr(), formattedSchemaStr1);
    SchemaResponse sr7 = controllerClient.getValueSchema(storeToCreate, 2);
    Assert.assertFalse(sr7.isError());
    Assert.assertEquals(sr7.getSchemaStr(), formattedSchemaStr2);
    // Get schema by non-existed schema id
    SchemaResponse sr8 = controllerClient.getValueSchema(storeToCreate, 3);
    Assert.assertTrue(sr8.isError());

    // Get value schema by schema
    SchemaResponse sr9 = controllerClient.getValueSchemaID(storeToCreate, schema1);
    Assert.assertFalse(sr9.isError());
    Assert.assertEquals(sr9.getId(), 1);
    SchemaResponse sr10 = controllerClient.getValueSchemaID(storeToCreate, schema2);
    Assert.assertFalse(sr10.isError());
    Assert.assertEquals(sr10.getId(), 2);
    SchemaResponse sr11 = controllerClient.getValueSchemaID(storeToCreate, schema3);
    Assert.assertTrue(sr11.isError());
    SchemaResponse sr12 = controllerClient.getValueSchemaID(storeToCreate, schema4);
    Assert.assertTrue(sr12.isError());

    // Get all value schema
    MultiSchemaResponse msr = controllerClient.getAllValueSchema(storeToCreate);
    Assert.assertFalse(msr.isError());
    MultiSchemaResponse.Schema[] schemas = msr.getSchemas();
    Assert.assertEquals(schemas.length, 2);
    Assert.assertEquals(schemas[0].getId(), 1);
    Assert.assertEquals(schemas[0].getSchemaStr(), formattedSchemaStr1);
    Assert.assertEquals(schemas[1].getId(), 2);
    Assert.assertEquals(schemas[1].getSchemaStr(), formattedSchemaStr2);
  }

  @Test
  public void controllerClientSchemaOperationsAgainstInvalidStore() {
    String schema1 = "\"string\"";
    // Verify getting operations against non-existed store
    String nonExistedStore = TestUtils.getUniqueString("test2434095i02");
    SchemaResponse sr1 = controllerClient.getValueSchema(nonExistedStore, 1);
    Assert.assertTrue(sr1.isError());
    SchemaResponse sr2 = controllerClient.getValueSchemaID(nonExistedStore, schema1);
    Assert.assertTrue(sr2.isError());
    MultiSchemaResponse msr1 = controllerClient.getAllValueSchema(nonExistedStore);
    Assert.assertTrue(msr1.isError());
  }

  @Test
  public void controllerClientCanGetStoreInfo(){
    String topic = venice.getNewStoreVersion().getKafkaTopic();
    String storeName = Version.parseStoreFromKafkaTopicName(topic);
    StoreResponse storeResponse = controllerClient.getStore(storeName);
    Assert.assertFalse(storeResponse.isError(), storeResponse.getError());

    StoreInfo store = storeResponse.getStore();
    Assert.assertEquals(store.getName(), storeName, "Store Info should have same store name as request");
    Assert.assertTrue(store.isEnableStoreWrites(), "New store should not be disabled");
    Assert.assertTrue(store.isEnableStoreReads(), "New store should not be disabled");
    List<Version> versions = store.getVersions();
    Assert.assertEquals(versions.size(), 1, "Store from new store-version should only have one version");
  }

  @Test
  public void controllerClientCanDisableStoresWrite()
      throws InterruptedException {
    String topic = venice.getNewStoreVersion().getKafkaTopic();
    String storeName = Version.parseStoreFromKafkaTopicName(topic);

    StoreInfo store = controllerClient.getStore(storeName).getStore();
    Assert.assertTrue(store.isEnableStoreWrites(), "Store should NOT be disabled after creating new store-version");

    ControllerResponse response = controllerClient.enableStoreWrites(storeName, false);
    Assert.assertFalse(response.isError(), response.getError());

    store = controllerClient.getStore(storeName).getStore();
    Assert.assertFalse(store.isEnableStoreWrites(), "Store should be disabled after setting disabled status to true");
  }

  @Test
  public void controllerClientCanDisableStoresRead()
      throws InterruptedException {
    String topic = venice.getNewStoreVersion().getKafkaTopic();

    String storeName = Version.parseStoreFromKafkaTopicName(topic);

    StoreInfo store = controllerClient.getStore(storeName).getStore();
    Assert.assertTrue(store.isEnableStoreReads(), "Store should NOT be disabled after creating new store-version");

    ControllerResponse response = controllerClient.enableStoreReads(storeName, false);
    Assert.assertFalse(response.isError(), response.getError());

    store = controllerClient.getStore(storeName).getStore();
    Assert.assertFalse(store.isEnableStoreReads(), "Store should be disabled after setting disabled status to true");
  }

  @Test
  public void controllerClientCanDisableStoresReadWrite()
      throws InterruptedException {
    String topic = venice.getNewStoreVersion().getKafkaTopic();

    String storeName = Version.parseStoreFromKafkaTopicName(topic);

    StoreInfo store = controllerClient.getStore(storeName).getStore();
    Assert.assertTrue(store.isEnableStoreReads(), "Store should NOT be disabled after creating new store-version");
    Assert.assertTrue(store.isEnableStoreWrites(), "Store should NOT be disabled after creating new store-version");

    ControllerResponse response = controllerClient.enableStoreReadWrites(storeName, false);
    Assert.assertFalse(response.isError(), response.getError());

    store = controllerClient.getStore(storeName).getStore();
    Assert.assertFalse(store.isEnableStoreReads(), "Store should be disabled after setting disabled status to true");
    Assert.assertFalse(store.isEnableStoreWrites(), "Store should be disabled after setting disabled status to true");
  }

  @Test
  public void controllerClientCanSetStoreMetadata() {
    String storeName = TestUtils.getUniqueString("store");
    String owner = TestUtils.getUniqueString("owner");
    int partitionCount = 2;

    venice.getNewStore(storeName);
    ControllerClient controllerClient = new ControllerClient(venice.getClusterName(), routerUrl);

    OwnerResponse ownerRes = controllerClient.setStoreOwner(storeName, owner);
    Assert.assertFalse(ownerRes.isError(), ownerRes.getError());
    Assert.assertEquals(ownerRes.getOwner(), owner);

    PartitionResponse partitionRes = controllerClient.setStorePartitionCount(storeName, String.valueOf(partitionCount));
    Assert.assertFalse(partitionRes.isError(), partitionRes.getError());
    Assert.assertEquals(partitionRes.getPartitionCount(), partitionCount);
  }

  @Test
  public void controllerClientCanQueryRemovability(){
    VeniceServerWrapper server = venice.getVeniceServers().get(0);
    String nodeId = Utils.getHelixNodeIdentifier(server.getPort());

    ControllerResponse response = controllerClient.isNodeRemovable(nodeId);
    Assert.assertFalse(response.isError(), response.getError());
  }

  @Test
  public void controllerClientCanDeleteAllVersion() {
    String storeName = "controllerClientCanDeleteAllVersion";
    venice.getNewStore(storeName);
    venice.getNewVersion(storeName, 100);

    controllerClient.enableStoreReads(storeName, false);
    controllerClient.enableStoreWrites(storeName, false);
    MultiVersionResponse response = controllerClient.deleteAllVersions(storeName);
    Assert.assertEquals(response.getExecutionId(), 0,
        "The command executed in non-parent controller should have an execution id 0");

    StoreInfo store = controllerClient.getStore(storeName).getStore();
    Assert.assertEquals(store.getVersions().size(), 0);
  }

  @Test
  public void controllerClientCanGetLastSucceedExecutionId() {
    Assert.assertEquals(controllerClient.getLastSucceedExecutionId().getLastSucceedExecutionId(),
        -1);
  }

  @Test
  public void controllerClientCanGetExecutionOfDeleteAllVersions()
      throws InterruptedException {
    String cluster = venice.getClusterName();
    ZkServerWrapper parentZk = ServiceFactory.getZkServer();
    VeniceControllerWrapper parentController =
        ServiceFactory.getVeniceParentController(cluster, parentZk.getAddress(), ServiceFactory.getKafkaBroker(),
            venice.getMasterVeniceController());
    String storeName = "controllerClientCanDeleteAllVersion";
    parentController.getVeniceAdmin().addStore(cluster, storeName, "test", "test", "\"string\"", "\"string\"");
    parentController.getVeniceAdmin().incrementVersion(cluster, storeName, 1, 1);

    ControllerClient controllerClient = new ControllerClient(cluster, parentController.getControllerUrl());
    controllerClient.enableStoreReads(storeName, false);
    controllerClient.enableStoreWrites(storeName, false);
    MultiVersionResponse multiVersionResponse = controllerClient.deleteAllVersions(storeName);
    long executionId = multiVersionResponse.getExecutionId();
    AdminCommandExecutionResponse response =
        controllerClient.getAdminCommandExecution(executionId);
    AdminCommandExecution execution = response.getExecution();
    // Command would not be executed in child controller because we don't have Kafka MM in the local box.
    Assert.assertFalse(execution.isSucceedInAllFabric());
  }

  @Test
  public void controllerClientCanListStoresStatuses() {
    List<String> storeNames = new ArrayList<>();
    int storeCount = 2;
    for (int i = 0; i < storeCount; i++) {
      storeNames.add(venice.getNewStore("testStore" + i, "test").getName());
    }

    MultiStoreStatusResponse storeResponse =
        controllerClient.listStoresStatuses(venice.getClusterName());
    Assert.assertFalse(storeResponse.isError());
    //since all test cases share VeniceClusterWrapper, we get the total number of stores from the Wrapper.
    Assert.assertEquals(storeResponse.getStoreStatusMap().size(), venice.getStoreCount(),
        "Result of listing store status should contain all stores we created.");
    for (String status : storeResponse.getStoreStatusMap().values()) {
      Assert.assertEquals(status, StoreStatus.UNAVAILABLE.toString(),
          "Store should be unavailable because we have not created a version for this store.");
    }
    for (String expectedStore : storeNames) {
      Assert.assertTrue(storeResponse.getStoreStatusMap().containsKey(expectedStore),
          "Result of list store status should contain the store we created: " + expectedStore);
    }
  }

  @Test
  public void controllerClientCanRemoveNodeFromCluster() {
    Admin admin = venice.getMasterVeniceController().getVeniceAdmin();
    VeniceServerWrapper server = venice.getVeniceServers().get(0);
    String nodeId = Utils.getHelixNodeIdentifier(server.getPort());
    // Trying to remove a live node.
    ControllerResponse response = controllerClient.removeNodeFromCluster(nodeId);
    Assert.assertTrue(response.isError(), "Node is still connected to cluster, could not be removed.");

    // Remove a disconnected node.
    venice.stopVeniceServer(server.getPort());
    response = controllerClient.removeNodeFromCluster(nodeId);
    Assert.assertFalse(response.isError(), "Node is already disconnected, could be removed.");
    Assert.assertFalse(admin.getStorageNodes(venice.getClusterName()).contains(nodeId),
        "Node should be removed from the cluster.");
  }

  @Test
  public void controllerClientCanUpdateWhiteList() {
    Admin admin = venice.getMasterVeniceController().getVeniceAdmin();

    String nodeId = Utils.getHelixNodeIdentifier(34567);
    Assert.assertFalse(admin.getWhitelist(venice.getClusterName()).contains(nodeId),
        nodeId + " has not been added into white list.");
    controllerClient.addNodeIntoWhiteList(nodeId);
    Assert.assertTrue(admin.getWhitelist(venice.getClusterName()).contains(nodeId),
        nodeId + " has been added into white list.");
    controllerClient.removeNodeFromWhiteList(nodeId);
    Assert.assertFalse(admin.getWhitelist(venice.getClusterName()).contains(nodeId),
        nodeId + " has been removed from white list.");
  }

  @Test
  public void controllerClientCanSetStore() {
    //mutable store metadata
    String owner = TestUtils.getUniqueString("owner");
    String principles = TestUtils.getUniqueString("principles");
    int partitionCount = 2;
    int current = 1;
    boolean enableReads = false;
    boolean enableWrite = true;

    String storeName = venice.getNewStoreVersion().getName();
    ControllerClient controllerClient = new ControllerClient(venice.getClusterName(), routerUrl);
    // Disable writes at first and test could we enable writes again through the update store method.
    Assert.assertFalse(controllerClient.enableStoreReadWrites(storeName, false).isError(),
        "Disable writes should not fail.");

    ControllerResponse response =
        controllerClient.updateStore(storeName, Optional.of(owner), Optional.of(principles), Optional.of(partitionCount),
            Optional.of(current), Optional.of(enableReads), Optional.of(enableWrite));

    Assert.assertFalse(response.isError(), response.getError());
    Store store = venice.getMasterVeniceController().getVeniceAdmin().getStore(venice.getClusterName(), storeName);
    Assert.assertEquals(store.getOwner(), owner);
    Assert.assertEquals(store.getPartitionCount(), partitionCount);
    Assert.assertEquals(store.getCurrentVersion(), current);
    Assert.assertEquals(store.isEnableReads(), enableReads);
    Assert.assertEquals(store.isEnableWrites(), enableWrite);

    enableWrite = false;
    Assert.assertFalse(controllerClient
            .updateStore(storeName, Optional.of(owner), Optional.of(principles), Optional.of(partitionCount),
                Optional.empty(), Optional.of(enableReads), Optional.of(enableWrite)).isError(),
        "We should be able to disable store writes again.");
  }

  @Test
  public void controllerClientCanSetStoreMissingSomeFields() {
    //partial metadata
    int partitionCount = 2;
    int current = 1;
    boolean enableReads = false;

    String storeName = venice.getNewStoreVersion().getName();
    ControllerClient controllerClient = new ControllerClient(venice.getClusterName(), routerUrl);

    ControllerResponse response =
        controllerClient.updateStore(storeName, Optional.empty(), Optional.empty(), Optional.of(partitionCount),
            Optional.of(current), Optional.of(enableReads), Optional.empty());

    Assert.assertFalse(response.isError(), response.getError());
    Store store = venice.getMasterVeniceController().getVeniceAdmin().getStore(venice.getClusterName(), storeName);
    Assert.assertEquals(store.getPartitionCount(), partitionCount);
    Assert.assertEquals(store.getCurrentVersion(), current);
    Assert.assertEquals(store.isEnableReads(), enableReads);
  }

}
