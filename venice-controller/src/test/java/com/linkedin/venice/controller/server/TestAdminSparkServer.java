package com.linkedin.venice.controller.server;

import com.linkedin.venice.controllerapi.AdminCommandExecution;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.MultiNodeResponse;
import com.linkedin.venice.controllerapi.MultiReplicaResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.MultiVersionResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.OwnerResponse;
import com.linkedin.venice.controllerapi.PartitionResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.controllerapi.routes.AdminCommandExecutionResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceControllerWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.TestUtils;
import java.util.List;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestAdminSparkServer {
  private VeniceClusterWrapper venice;
  private String routerUrl;

  @BeforeMethod // TODO figure out why tests are stepping on each other and switch this back to @BeforeClass
  public void setUp(){
    venice = ServiceFactory.getVeniceCluster();
    routerUrl = venice.getRandomRouterURL();
  }

  @AfterMethod
  public void tearDown(){
    venice.close();
  }

  @Test
  public void controllerClientCanQueryNodesInCluster(){
    MultiNodeResponse nodeResponse = ControllerClient.listStorageNodes(routerUrl, venice.getClusterName());
    Assert.assertFalse(nodeResponse.isError(), nodeResponse.getError());
    Assert.assertEquals(nodeResponse.getNodes().length, 1, "Node count does not match");
  }

  @Test
  public void controllerClientCanQueryReplicasOnAStorageNode(){
    venice.getNewStoreVersion();
    MultiNodeResponse nodeResponse = ControllerClient.listStorageNodes(routerUrl, venice.getClusterName());
    String nodeId = nodeResponse.getNodes()[0];
    MultiReplicaResponse replicas = ControllerClient.listStorageNodeReplicas(routerUrl, venice.getClusterName(), nodeId);
    Assert.assertFalse(replicas.isError(), replicas.getError());
    Assert.assertTrue(replicas.getReplicas().length >= 10, "Number of replicas is incorrect");
  }

  @Test
  public void controllerClientCanQueryReplicasForTopic(){
    VersionCreationResponse versionCreationResponse = venice.getNewStoreVersion();
    Assert.assertFalse(versionCreationResponse.isError(), versionCreationResponse.getError());
    String kafkaTopic = venice.getNewStoreVersion().getKafkaTopic();
    Assert.assertNotNull(kafkaTopic, "venice.getNewStoreVersion() should not return a null topic name\n" + versionCreationResponse.toString());


    String store = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    int version = Version.parseVersionFromKafkaTopicName(kafkaTopic);
    MultiReplicaResponse response = ControllerClient.listReplicas(routerUrl, venice.getClusterName(), store, version);
    Assert.assertFalse(response.isError(), response.getError());
    Assert.assertEquals(response.getReplicas().length, 10, "Replica count does not match"); /* 10 partitions, replication factor 1 */
  }

  @Test
  public void controllerClientCanCreateNewStore(){
    String storeToCreate = "newTestStore123";
    String keySchema = "\"string\"";
    String valueSchema = "\"long\"";
    String clusterName = venice.getClusterName();
    // create Store
    NewStoreResponse newStoreResponse = ControllerClient.createNewStore(routerUrl, clusterName,
        storeToCreate, "owner", keySchema, valueSchema);
    Assert.assertFalse(newStoreResponse.isError(), "create new store should succeed for a store that doesn't exist");
    Assert.assertEquals(ControllerClient.getKeySchema(routerUrl, clusterName, storeToCreate).getSchemaStr(), keySchema);
    Assert.assertEquals(ControllerClient.getValueSchema(routerUrl, clusterName, storeToCreate, 1).getSchemaStr(), valueSchema);
    NewStoreResponse duplicateNewStoreResponse = ControllerClient.createNewStore(routerUrl, venice.getClusterName(),
        storeToCreate, "owner", keySchema, valueSchema);
    Assert.assertTrue(duplicateNewStoreResponse.isError(), "create new store should fail for duplicate store creation");
  }

  @Test
  public void controllerClientGetKeySchema() {
    String storeToCreate = "newTestStore125";
    String clusterName = venice.getClusterName();
    String keySchemaStr = "\"string\"";
    String valueSchemaStr = "\"long\"";
    // Get key schema from non-existed store
    SchemaResponse sr0 = ControllerClient.getKeySchema(routerUrl, clusterName, storeToCreate);
    Assert.assertTrue(sr0.isError());
    // Create Store
    NewStoreResponse newStoreResponse = ControllerClient.createNewStore(routerUrl, clusterName,
        storeToCreate, "owner", keySchemaStr, valueSchemaStr);
    Assert.assertFalse(newStoreResponse.isError(), "create new store should succeed for a store that doesn't exist");
    SchemaResponse sr1 = ControllerClient.getKeySchema(routerUrl, clusterName, storeToCreate);
    Assert.assertEquals(sr1.getId(), 1);
    Assert.assertEquals(sr1.getSchemaStr(), keySchemaStr);
  }

  @Test
  public void controllerClientManageValueSchema() {
    String storeToCreate = TestUtils.getUniqueString("newTestStore");
    String clusterName = venice.getClusterName();
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
    SchemaResponse sr0 = ControllerClient.addValueSchema(routerUrl, clusterName, storeToCreate, schema1);
    Assert.assertTrue(sr0.isError());
    // Add value schema to an existing store
    NewStoreResponse newStoreResponse = ControllerClient.createNewStore(routerUrl, clusterName,
        storeToCreate, "owner", keySchemaStr, schema1);
    Assert.assertFalse(newStoreResponse.isError(), "create new store should succeed for a store that doesn't exist");
    SchemaResponse sr1 = ControllerClient.addValueSchema(routerUrl, clusterName, storeToCreate, schema1);
    Assert.assertFalse(sr1.isError());
    Assert.assertEquals(sr1.getId(), 1);
    // Add same value schema
    SchemaResponse sr2 = ControllerClient.addValueSchema(routerUrl, clusterName, storeToCreate, schema1);
    Assert.assertFalse(sr2.isError());
    Assert.assertEquals(sr2.getId(), sr1.getId());
    // Add a new value schema
    SchemaResponse sr3 = ControllerClient.addValueSchema(routerUrl, clusterName, storeToCreate, schema2);
    Assert.assertFalse(sr3.isError());
    Assert.assertEquals(sr3.getId(), 2);
    // Add invalid schema
    SchemaResponse sr4 = ControllerClient.addValueSchema(routerUrl, clusterName, storeToCreate, schema3);
    Assert.assertTrue(sr4.isError());
    // Add incompatible schema
    SchemaResponse sr5 = ControllerClient.addValueSchema(routerUrl, clusterName, storeToCreate, schema4);
    Assert.assertTrue(sr5.isError());

    // Formatted schema string
    String formattedSchemaStr1 = Schema.parse(schema1).toString();
    String formattedSchemaStr2 = Schema.parse(schema2).toString();
    // Get schema by id
    SchemaResponse sr6 = ControllerClient.getValueSchema(routerUrl, clusterName, storeToCreate, 1);
    Assert.assertFalse(sr6.isError());
    Assert.assertEquals(sr6.getSchemaStr(), formattedSchemaStr1);
    SchemaResponse sr7 = ControllerClient.getValueSchema(routerUrl, clusterName, storeToCreate, 2);
    Assert.assertFalse(sr7.isError());
    Assert.assertEquals(sr7.getSchemaStr(), formattedSchemaStr2);
    // Get schema by non-existed schema id
    SchemaResponse sr8 = ControllerClient.getValueSchema(routerUrl, clusterName, storeToCreate, 3);
    Assert.assertTrue(sr8.isError());

    // Get value schema by schema
    SchemaResponse sr9 = ControllerClient.getValueSchemaID(routerUrl, clusterName, storeToCreate, schema1);
    Assert.assertFalse(sr9.isError());
    Assert.assertEquals(sr9.getId(), 1);
    SchemaResponse sr10 = ControllerClient.getValueSchemaID(routerUrl, clusterName, storeToCreate, schema2);
    Assert.assertFalse(sr10.isError());
    Assert.assertEquals(sr10.getId(), 2);
    SchemaResponse sr11 = ControllerClient.getValueSchemaID(routerUrl, clusterName, storeToCreate, schema3);
    Assert.assertTrue(sr11.isError());
    SchemaResponse sr12 = ControllerClient.getValueSchemaID(routerUrl, clusterName, storeToCreate, schema4);
    Assert.assertTrue(sr12.isError());

    // Get all value schema
    MultiSchemaResponse msr = ControllerClient.getAllValueSchema(routerUrl, clusterName, storeToCreate);
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
    String clusterName = venice.getClusterName();
    String schema1 = "\"string\"";
    // Verify getting operations against non-existed store
    String nonExistedStore = "test2434095i02";
    SchemaResponse sr1 = ControllerClient.getValueSchema(routerUrl, clusterName, nonExistedStore, 1);
    Assert.assertTrue(sr1.isError());
    SchemaResponse sr2 = ControllerClient.getValueSchemaID(routerUrl, clusterName, nonExistedStore, schema1);
    Assert.assertTrue(sr2.isError());
    MultiSchemaResponse msr1 = ControllerClient.getAllValueSchema(routerUrl, clusterName, nonExistedStore);
    Assert.assertTrue(msr1.isError());
  }

  @Test
  public void controllerClientCanGetStoreInfo(){
    String topic = venice.getNewStoreVersion().getKafkaTopic();
    String storeName = Version.parseStoreFromKafkaTopicName(topic);
    StoreResponse storeResponse = ControllerClient.getStore(routerUrl, venice.getClusterName(), storeName);
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

    StoreInfo store = ControllerClient.getStore(routerUrl, venice.getClusterName(), storeName).getStore();
    Assert.assertTrue(store.isEnableStoreWrites(), "Store should NOT be disabled after creating new store-version");

    ControllerResponse response = ControllerClient.enableStoreWrites(routerUrl, venice.getClusterName(), storeName, false);
    Assert.assertFalse(response.isError(), response.getError());

    store = ControllerClient.getStore(routerUrl, venice.getClusterName(), storeName).getStore();
    Assert.assertFalse(store.isEnableStoreWrites(), "Store should be disabled after setting disabled status to true");
  }

  @Test
  public void controllerClientCanDisableStoresRead()
      throws InterruptedException {
    ControllerClient controllerClient = new ControllerClient(venice.getClusterName(), routerUrl);
    String topic = venice.getNewStoreVersion().getKafkaTopic();

    String storeName = Version.parseStoreFromKafkaTopicName(topic);

    StoreInfo store = controllerClient.getStore(venice.getClusterName(), storeName).getStore();
    Assert.assertTrue(store.isEnableStoreReads(), "Store should NOT be disabled after creating new store-version");

    ControllerResponse response = controllerClient.enableStoreReads(venice.getClusterName(), storeName, false);
    Assert.assertFalse(response.isError(), response.getError());

    store = controllerClient.getStore( venice.getClusterName(), storeName).getStore();
    Assert.assertFalse(store.isEnableStoreReads(), "Store should be disabled after setting disabled status to true");
  }

  @Test
  public void controllerClientCanDisableStoresReadWrite()
      throws InterruptedException {
    ControllerClient controllerClient = new ControllerClient(venice.getClusterName(), routerUrl);
    String topic = venice.getNewStoreVersion().getKafkaTopic();

    String storeName = Version.parseStoreFromKafkaTopicName(topic);

    StoreInfo store = controllerClient.getStore(venice.getClusterName(), storeName).getStore();
    Assert.assertTrue(store.isEnableStoreReads(), "Store should NOT be disabled after creating new store-version");
    Assert.assertTrue(store.isEnableStoreWrites(), "Store should NOT be disabled after creating new store-version");

    ControllerResponse response = controllerClient.enableStoreReadWrites(venice.getClusterName(), storeName, false);
    Assert.assertFalse(response.isError(), response.getError());

    store = controllerClient.getStore( venice.getClusterName(), storeName).getStore();
    Assert.assertFalse(store.isEnableStoreReads(), "Store should be disabled after setting disabled status to true");
    Assert.assertFalse(store.isEnableStoreWrites(), "Store should be disabled after setting disabled status to true");
  }

  @Test
  public void controllerClientCanQueryRemovability(){
    String topic = venice.getNewStoreVersion().getKafkaTopic();
    VeniceServerWrapper server = venice.getVeniceServers().get(0);
    String nodeId = server.getHost() + "_" + server.getPort();

    ControllerResponse response = ControllerClient.isNodeRemovable(routerUrl, venice.getClusterName(), nodeId);
    Assert.assertFalse(response.isError(), response.getError());
  }

  @Test
  public void controllerClientCanDeleteAllVersion() {
    String storeName = "controllerClientCanDeleteAllVersion";
    venice.getNewStore(storeName);
    venice.getNewVersion(storeName, 100);
    ControllerClient controllerClient = new ControllerClient(venice.getClusterName(), routerUrl);

    controllerClient.enableStoreReads(venice.getClusterName(), storeName, false);
    controllerClient.enableStoreWrites(venice.getClusterName(), storeName, false);
    MultiVersionResponse response = controllerClient.deleteAllVersions(venice.getClusterName(), storeName);
    Assert.assertEquals(response.getExecutionId(), 0,
        "The command executed in non-parent controller should have an execution id 0");

    StoreInfo store = controllerClient.getStore(venice.getClusterName(), storeName).getStore();
    Assert.assertEquals(store.getVersions().size(), 0);
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
    parentController.getVeniceAdmin().addStore(cluster, storeName, "test", "\"string\"", "\"string\"");
    parentController.getVeniceAdmin().incrementVersion(cluster, storeName, 1, 1);

    ControllerClient controllerClient = new ControllerClient(cluster, parentController.getControllerUrl());
    controllerClient.enableStoreReads(cluster, storeName, false);
    controllerClient.enableStoreWrites(cluster, storeName, false);
    MultiVersionResponse multiVersionResponse = controllerClient.deleteAllVersions(cluster, storeName);
    long executionId = multiVersionResponse.getExecutionId();
    AdminCommandExecutionResponse response =
        controllerClient.getAdminCommandExecution(cluster, executionId);
    AdminCommandExecution execution = response.getExecution();
    // Command would not be executed in child controller because we don't have Kafka MM in the local box.
    Assert.assertFalse(execution.isSucceedInAllFabric());
  }

  @Test
  public void controllerClientCanGetLastSucceedExecutionId() {
    ControllerClient controllerClient = new ControllerClient(venice.getClusterName(), routerUrl);
    Assert.assertEquals(controllerClient.getLastSucceedExecutionId(venice.getClusterName()).getLastSucceedExecutionId(),
        -1);
  }

  @Test
  public void controllerClientCanSetStoreMetadata() {
    String storeName = TestUtils.getUniqueString("store");
    String owner = TestUtils.getUniqueString("owner");
    int partitionCount = 2;

    venice.getNewStore(storeName);
    ControllerClient controllerClient = new ControllerClient(venice.getClusterName(), routerUrl);

    OwnerResponse ownerRes = controllerClient.setStoreOwner(venice.getClusterName(), storeName, owner);
    Assert.assertFalse(ownerRes.isError(), ownerRes.getError());
    Assert.assertEquals(ownerRes.getOwner(), owner);

    PartitionResponse partitionRes = controllerClient.setStorePartitionCount(venice.getClusterName(), storeName, String.valueOf(partitionCount));
    Assert.assertFalse(partitionRes.isError(), partitionRes.getError());
    Assert.assertEquals(partitionRes.getPartitionCount(), partitionCount);
  }
}
