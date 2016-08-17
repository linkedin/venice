package com.linkedin.venice.controller.server;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiNodeResponse;
import com.linkedin.venice.controllerapi.MultiReplicaResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Version;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Created by mwise on 4/20/16.
 */
public class TestAdminSparkServer {
  private VeniceClusterWrapper venice;
  private String routerUrl;

  @BeforeClass
  public void setUp(){
    venice = ServiceFactory.getVeniceCluster();
    routerUrl = "http://" + venice.getVeniceRouter().getAddress();
  }

  @AfterClass
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
  public void controllerClientCanQueryReplicasForTopic(){
    String kafkaTopic = venice.getNewStoreVersion().getKafkaTopic();
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
  public void controllerClientCanReserverVersions() {
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
}
