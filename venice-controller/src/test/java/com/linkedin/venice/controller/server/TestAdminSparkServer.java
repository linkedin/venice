package com.linkedin.venice.controller.server;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.MultiNodeResponse;
import com.linkedin.venice.controllerapi.MultiReplicaResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
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
    routerUrl = "http://" + venice.getVeniceRouter().getAddress();
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
  public void controllerClientCanQueryReplicasOnAStrageNode(){
    String topic = venice.getNewStoreVersion().getKafkaTopic();
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
  public void controllerClientCanQueryNextVersion() throws InterruptedException {
    String kafkaTopic = venice.getNewStoreVersion().getKafkaTopic();
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    int currentVersion = Version.parseVersionFromKafkaTopicName(kafkaTopic);

    VersionResponse nextVersionResponse =
        ControllerClient.queryNextVersion(routerUrl, venice.getClusterName(), storeName);
    Assert.assertEquals(nextVersionResponse.getVersion(), currentVersion + 1);

    VersionResponse badVersionResponse =
        ControllerClient.queryNextVersion(routerUrl, venice.getClusterName(), "does-not-exist-"+storeName);
    Assert.assertTrue(badVersionResponse.isError());
  }

  @Test
  public void controllerClientCanReserveVersions() {
    String kafkaTopic = venice.getNewStoreVersion().getKafkaTopic();
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    int currentVersion = Version.parseVersionFromKafkaTopicName(kafkaTopic);

    VersionResponse badReservation =
        ControllerClient.reserveVersion(routerUrl, venice.getClusterName(), storeName, currentVersion);
    Assert.assertTrue(badReservation.isError(), "controller client should not allow reservation of current version");
    int reserveVersion = currentVersion + 1;
    VersionResponse goodReservation =
        ControllerClient.reserveVersion(routerUrl, venice.getClusterName(), storeName, reserveVersion);
    Assert.assertFalse(goodReservation.isError(), "should be able to reserve next version");

    VersionResponse afterReservationPeek =
        ControllerClient.queryNextVersion(routerUrl, venice.getClusterName(), storeName);
    Assert.assertEquals(afterReservationPeek.getVersion(), goodReservation.getVersion() + 1);
    VersionResponse doubleReservation =
        ControllerClient.reserveVersion(routerUrl, venice.getClusterName(), storeName, reserveVersion);
    Assert.assertTrue(doubleReservation.isError(), "controller client should not allow duplicate version reservation");
  }

  @Test
  public void controllerClientCanCreateNewStore(){
    String storeToCreate = "newTestStore123";
    // create Store
    NewStoreResponse newStoreResponse = ControllerClient.createNewStore(routerUrl, venice.getClusterName(),
        storeToCreate, "owner");
    Assert.assertFalse(newStoreResponse.isError(), "create new store should succeed for a store that doesn't exist");
    VersionResponse newVersionResponse = ControllerClient.queryNextVersion(routerUrl, venice.getClusterName(),
        storeToCreate);
    Assert.assertFalse(newVersionResponse.isError());
    NewStoreResponse duplicateNewStoreResponse = ControllerClient.createNewStore(routerUrl, venice.getClusterName(),
        storeToCreate, "owner");
    Assert.assertTrue(duplicateNewStoreResponse.isError(), "create new store should fail for duplicate store creation");
  }

  @Test
  public void controllerClientCreateKeySchema(){
    String storeToCreate = "newTestStore124";
    String clusterName = venice.getClusterName();
    String invalidKeySchemaStr = "\"abc\"";
    String validKeySchemaStr = "\"string\"";
    // Create key schema of non-existed store
    SchemaResponse sr0 = ControllerClient.initKeySchema(routerUrl, clusterName, storeToCreate, validKeySchemaStr);
    Assert.assertTrue(sr0.isError());
    // Create Store
    NewStoreResponse newStoreResponse = ControllerClient.createNewStore(routerUrl, clusterName,
        storeToCreate, "owner");
    Assert.assertFalse(newStoreResponse.isError(), "create new store should succeed for a store that doesn't exist");
    // Create key schema with invalid schema
    SchemaResponse sr1 = ControllerClient.initKeySchema(routerUrl, clusterName, storeToCreate, invalidKeySchemaStr);
    Assert.assertTrue(sr1.isError());
    // Create key schema with valid schema
    SchemaResponse sr2 = ControllerClient.initKeySchema(routerUrl, clusterName, storeToCreate, validKeySchemaStr);
    Assert.assertEquals(sr2.getSchemaStr(), validKeySchemaStr);
    // Create key schema multiple times with the same key schema
    SchemaResponse sr3 = ControllerClient.initKeySchema(routerUrl, clusterName, storeToCreate, validKeySchemaStr);
    Assert.assertFalse(sr3.isError());
    // Create key schema multiple times with different key schema
    String anotherValidKeySchemaStr = "\"long\"";
    SchemaResponse sr4 = ControllerClient.initKeySchema(routerUrl, clusterName, storeToCreate, anotherValidKeySchemaStr);
    Assert.assertTrue(sr4.isError());
  }

  @Test
  public void controllerClientGetKeySchema() {
    String storeToCreate = "newTestStore125";
    String clusterName = venice.getClusterName();
    // Get key schema from non-existed store
    SchemaResponse sr0 = ControllerClient.getKeySchema(routerUrl, clusterName, storeToCreate);
    Assert.assertTrue(sr0.isError());
    // Create Store
    NewStoreResponse newStoreResponse = ControllerClient.createNewStore(routerUrl, clusterName,
        storeToCreate, "owner");
    Assert.assertFalse(newStoreResponse.isError(), "create new store should succeed for a store that doesn't exist");
    SchemaResponse sr1 = ControllerClient.getKeySchema(routerUrl, clusterName, storeToCreate);
    // Key schema doesn't exist
    Assert.assertTrue(sr1.isError());
    // Create key schema with valid schema
    String validKeySchemaStr = "\"string\"";
    SchemaResponse sr2 = ControllerClient.initKeySchema(routerUrl, clusterName, storeToCreate, validKeySchemaStr);
    Assert.assertEquals(sr2.getSchemaStr(), validKeySchemaStr);
    SchemaResponse sr3 = ControllerClient.getKeySchema(routerUrl, clusterName, storeToCreate);
    Assert.assertEquals(sr3.getId(), 1);
    Assert.assertEquals(sr3.getSchemaStr(), validKeySchemaStr);
  }

  @Test
  public void controllerClientManageValueSchema() {
    String storeToCreate = "newTestStore126";
    String clusterName = venice.getClusterName();
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
        storeToCreate, "owner");
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
    Assert.assertFalse(store.isPaused(), "New store should not be paused");
    List<Version> versions = store.getVersions();
    Assert.assertEquals(versions.size(), 1, "Store from new store-version should only have one version");
  }

  @Test
  public void controllerClientCanPauseStores()
      throws InterruptedException {
    String topic = venice.getNewStoreVersion().getKafkaTopic();

    String storeName = Version.parseStoreFromKafkaTopicName(topic);

    StoreInfo store = ControllerClient.getStore(routerUrl, venice.getClusterName(), storeName).getStore();
    Assert.assertFalse(store.isPaused(), "Store should NOT be paused after creating new store-version");

    ControllerResponse response = ControllerClient.setPauseStatus(routerUrl, venice.getClusterName(), storeName, true);
    Assert.assertFalse(response.isError(), response.getError());

    store = ControllerClient.getStore(routerUrl, venice.getClusterName(), storeName).getStore();
    Assert.assertTrue(store.isPaused(), "Store should be paused after setting pause status to true");
  }

  @Test
  public void controllerClientCanQueryRemovability(){
    String topic = venice.getNewStoreVersion().getKafkaTopic();
    VeniceServerWrapper server = venice.getVeniceServers().get(0);
    String nodeId = server.getHost() + "_" + server.getPort();

    ControllerResponse response = ControllerClient.isNodeRemovable(routerUrl, venice.getClusterName(), nodeId);
    Assert.assertFalse(response.isError(), response.getError());
  }
}
