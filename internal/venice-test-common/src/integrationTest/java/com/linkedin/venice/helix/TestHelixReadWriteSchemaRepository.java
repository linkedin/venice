package com.linkedin.venice.helix;

import static com.linkedin.venice.schema.SchemaData.DUPLICATE_VALUE_SCHEMA_CODE;

import com.linkedin.venice.exceptions.SchemaIncompatibilityException;
import com.linkedin.venice.exceptions.StoreKeySchemaExistException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.schema.GeneratedSchemaID;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.Optional;
import org.apache.avro.SchemaParseException;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestHelixReadWriteSchemaRepository {
  private String zkAddress;
  private ZkClient zkClient;
  private String cluster = "test-metadata-cluster";
  private String clusterPath = "/test-metadata-cluster";
  private String storesPath = "/Stores";
  private ZkServerWrapper zkServerWrapper;
  private HelixAdapterSerializer adapter = new HelixAdapterSerializer();

  HelixSchemaAccessor accessor;
  HelixReadWriteStoreRepository storeRepo;
  HelixReadWriteSchemaRepository schemaRepo;

  @BeforeMethod
  public void zkSetup() {
    zkServerWrapper = ServiceFactory.getZkServer();
    zkAddress = zkServerWrapper.getAddress();
    zkClient = ZkClientFactory.newZkClient(zkAddress);
    zkClient.setZkSerializer(adapter);
    zkClient.create(clusterPath, null, CreateMode.PERSISTENT);
    zkClient.create(clusterPath + storesPath, null, CreateMode.PERSISTENT);

    accessor = new HelixSchemaAccessor(zkClient, adapter, cluster);
    storeRepo = new HelixReadWriteStoreRepository(
        zkClient,
        adapter,
        cluster,
        Optional.empty(),
        new ClusterLockManager(cluster));
    storeRepo.refresh();
    schemaRepo = new HelixReadWriteSchemaRepository(storeRepo, zkClient, adapter, cluster, Optional.empty());
  }

  @AfterMethod
  public void zkCleanup() {
    zkClient.deleteRecursively(clusterPath);
    zkClient.close();
    zkServerWrapper.close();
  }

  private void createStore(String storeName) {
    Store store = new ZKStore(
        storeName,
        "abc@linkedin.com",
        10,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_ALL_REPLICAS,
        1);
    storeRepo.addStore(store);
  }

  @Test
  public void testSetKeySchema() {
    // Create store first
    String storeName = "test_store1";
    createStore(storeName);
    // Set key schema
    String keySchemaStr = "\"string\"";
    SchemaEntry keySchema = schemaRepo.initKeySchema(storeName, keySchemaStr);
    Assert.assertTrue(zkClient.exists(accessor.getKeySchemaPath(storeName)));
    Assert.assertNotNull(keySchema);
    Assert.assertEquals(keySchema.getId(), Integer.parseInt(HelixSchemaAccessor.KEY_SCHEMA_ID));
    Assert.assertEquals(keySchema.getSchema().toString(), keySchemaStr);
    Assert.assertEquals(schemaRepo.getKeySchema(storeName), keySchema);
    // Listener num should be 0 since it is a RW repo
    Assert.assertEquals(zkClient.numberOfListeners(), 0);
  }

  @Test(expectedExceptions = VeniceNoStoreException.class)
  public void testSetKeySchemaToInvalidStore() {
    String storeName = "test_store1";
    String keySchemaStr = "\"string\"";
    Assert.assertNull(schemaRepo.initKeySchema(storeName, keySchemaStr));
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void testSetKeySchemaWithInvalidSchema() {
    String storeName = "test_store1";
    createStore(storeName);
    String invalidKeySchemaStr = "abc";
    schemaRepo.initKeySchema(storeName, invalidKeySchemaStr);
  }

  @Test
  public void testSetKeySchemaMultipleTimesWithSameSchema() {
    String storeName = "test_store1";
    String keySchemaStr = "\"long\"";
    createStore(storeName);
    schemaRepo.initKeySchema(storeName, keySchemaStr);
    Assert.assertNotNull(schemaRepo.getKeySchema(storeName));
    schemaRepo.initKeySchema(storeName, keySchemaStr);
  }

  @Test(expectedExceptions = StoreKeySchemaExistException.class)
  public void testSetKeySchemaMultipleTimes() {
    String storeName = "test_store1";
    String keySchemaStr = "\"long\"";
    createStore(storeName);
    schemaRepo.initKeySchema(storeName, keySchemaStr);
    Assert.assertNotNull(schemaRepo.getKeySchema(storeName));
    String newKeySchemaStr = "\"string\"";
    schemaRepo.initKeySchema(storeName, newKeySchemaStr);
  }

  @Test
  public void testGetValueSchemaId() {
    String storeName = "test_store1";
    String valueSchemaStr = "\"long\"";
    createStore(storeName);
    schemaRepo.addValueSchema(storeName, valueSchemaStr);
    Assert.assertEquals(1, schemaRepo.getValueSchemaId(storeName, valueSchemaStr));
    Assert.assertEquals(SchemaData.INVALID_VALUE_SCHEMA_ID, schemaRepo.getValueSchemaId(storeName, "\"string\""));
    Assert.assertTrue(schemaRepo.hasValueSchema(storeName, 1));
    Assert.assertFalse(schemaRepo.hasValueSchema(storeName, 2));
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void testGetValueSchemaIdByInvalidSchemaStr() {
    String storeName = "test_store1";
    String valueSchemaStr = "\"abc\"";
    createStore(storeName);
    schemaRepo.getValueSchemaId(storeName, valueSchemaStr);
  }

  @Test(expectedExceptions = VeniceNoStoreException.class)
  public void testGetValueSchemaIdByInvalidStore() {
    String storeName = "test_store1";
    String valueSchemaStr = "\"long\"";
    schemaRepo.getValueSchemaId(storeName, valueSchemaStr);
  }

  @Test
  public void testAddValueSchema() {
    String storeName = "test_store1";
    String valueSchemaStr1 = "{\n" + "           \"type\": \"record\",\n" + "           \"name\": \"KeyRecord\",\n"
        + "           \"fields\" : [\n"
        + "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n"
        + "               {\"name\": \"company\", \"type\": \"string\"},\n" + "               {\n"
        + "                 \"name\": \"Suit\", \n" + "                 \"type\": {\n"
        + "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"HEART\", \"CLUBS\"]\n"
        + "                }\n" + "              },\n" + "               {\"name\": \"salary\", \"type\": \"long\"}\n"
        + "           ]\n" + "        }";
    String valueSchemaStr2 = "{\n" + "           \"type\": \"record\",\n" + "           \"name\": \"KeyRecord\",\n"
        + "           \"fields\" : [\n"
        + "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n"
        + "               {\"name\": \"company\", \"type\": \"string\"},\n" + "               {\n"
        + "                 \"name\": \"Suit\", \n" + "                 \"type\": {\n"
        + "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"CLUBS\", \"HEART\"]\n"
        + "                } \n" + "              },\n"
        + "               {\"name\": \"salary\", \"type\": \"long\", \"default\": 123 }" + "           ]\n"
        + "        }";
    createStore(storeName);
    SchemaEntry valueSchema1 = schemaRepo.addValueSchema(storeName, valueSchemaStr1);
    Assert.assertEquals(valueSchema1.getId(), 1);
    SchemaEntry valueSchema2 = schemaRepo.addValueSchema(storeName, valueSchemaStr2);
    Assert.assertEquals(valueSchema2.getId(), 2);
    Assert.assertNotNull(schemaRepo.getValueSchema(storeName, 1));
    Assert.assertNotNull(schemaRepo.getValueSchema(storeName, 2));
    Assert.assertEquals(schemaRepo.getValueSchemas(storeName).size(), 2);
    Assert.assertEquals(schemaRepo.getValueSchemaId(storeName, valueSchemaStr1), 1);
    Assert.assertEquals(schemaRepo.getValueSchemaId(storeName, valueSchemaStr2), 2);
  }

  @Test
  public void testAddDuplicateValueSchema() {
    String storeName = "test_store1";
    String valueSchemaStr1 = "{\n" + "           \"type\": \"record\",\n" + "           \"name\": \"KeyRecord\",\n"
        + "           \"fields\" : [\n"
        + "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n"
        + "               {\"name\": \"company\", \"type\": \"string\"},\n" + "               {\n"
        + "                 \"name\": \"Suit\", \n" + "                 \"type\": {\n"
        + "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"HEART\", \"CLUBS\"]\n"
        + "                }\n" + "              },\n" + "               {\"name\": \"salary\", \"type\": \"long\"}\n"
        + "           ]\n" + "        }";
    createStore(storeName);
    SchemaEntry entry1 = schemaRepo.addValueSchema(storeName, valueSchemaStr1);
    // Add the same value schema
    SchemaEntry entry2 = schemaRepo.addValueSchema(storeName, valueSchemaStr1);
    Assert.assertEquals(entry2, entry1);
  }

  @Test(expectedExceptions = SchemaIncompatibilityException.class)
  public void testAddInCompatibleValueSchema() {
    String storeName = "test_store1";
    String valueSchemaStr1 = "{\n" + "           \"type\": \"record\",\n" + "           \"name\": \"KeyRecord\",\n"
        + "           \"fields\" : [\n"
        + "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n"
        + "               {\"name\": \"company\", \"type\": \"string\"},\n" + "               {\n"
        + "                 \"name\": \"Suit\", \n" + "                 \"type\": {\n"
        + "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"HEART\", \"CLUBS\"]\n"
        + "                }\n" + "              },\n" + "               {\"name\": \"salary\", \"type\": \"long\"}\n"
        + "           ]\n" + "        }";
    String valueSchemaStr2 = "{\n" + "           \"type\": \"record\",\n" + "           \"name\": \"KeyRecord\",\n"
        + "           \"fields\" : [\n"
        + "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n"
        + "               {\"name\": \"company\", \"type\": \"string\"},\n" + "               {\n"
        + "                 \"name\": \"Suit\", \n" + "                 \"type\": {\n"
        + "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"CLUBS\", \"HEART\"]\n"
        + "                } \n" + "              }\n" + "           ]\n" + "        }";
    createStore(storeName);
    SchemaEntry valueSchema1 = schemaRepo.addValueSchema(storeName, valueSchemaStr1);
    Assert.assertEquals(valueSchema1.getId(), 1);
    schemaRepo.addValueSchema(storeName, valueSchemaStr2);
  }

  @Test
  public void testAddDuplicateValueSchemaDocUpdate() {
    String storeName = "test_store1";
    String valueSchemaStr1 = "{\n" + "           \"type\": \"record\",\n" + "           \"name\": \"KeyRecord\",\n"
        + "           \"fields\" : [\n"
        + "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n"
        + "               {\"name\": \"company\", \"type\": \"string\"},\n" + "               {\n"
        + "                 \"name\": \"Suit\", \n" + "                 \"type\": {\n"
        + "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"HEART\", \"CLUBS\"]\n"
        + "                }\n" + "              },\n"
        + "               {\"name\": \"salary\", \"type\": \"long\", \"default\" : 123}\n" + "           ]\n"
        + "        }";
    String valueSchemaStr2 = "{\n" + "           \"type\": \"record\",\n" + "           \"name\": \"KeyRecord\",\n"
        + "           \"fields\" : [\n"
        + "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field updated\"},\n"
        + "               {\"name\": \"company\", \"type\": \"string\"},\n" + "               {\n"
        + "                 \"name\": \"Suit\", \n" + "                 \"type\": {\n"
        + "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"HEART\", \"CLUBS\"]\n"
        + "                }\n" + "              },\n"
        + "               {\"name\": \"salary\", \"type\": \"long\", \"default\" : 123}\n" + "           ]\n"
        + "        }";
    createStore(storeName);
    SchemaEntry entry1 = schemaRepo.addValueSchema(storeName, valueSchemaStr1);
    // Add the same value schema
    SchemaEntry entry2 = schemaRepo.addValueSchema(storeName, valueSchemaStr2);
    SchemaEntry entry3 = schemaRepo.addValueSchema(storeName, valueSchemaStr1);

    Assert.assertNotEquals(entry2.getId(), entry1.getId());
    Assert.assertEquals(entry3.getId(), DUPLICATE_VALUE_SCHEMA_CODE);
    Assert.assertEquals(schemaRepo.getValueSchemas(storeName).size(), 2);
    Assert.assertEquals(2, schemaRepo.getValueSchemaId(storeName, valueSchemaStr2));

    Assert.assertEquals(entry2.getSchema().getField("name").doc(), "name field updated");
  }

  @Test
  public void testAddDuplicateValueSchemaDefaultValue() {
    String storeName = "test_store1";
    String valueSchemaStr1 = "{\n" + "           \"type\": \"record\",\n" + "           \"name\": \"KeyRecord\",\n"
        + "           \"fields\" : [\n"
        + "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n"
        + "               {\"name\": \"company\", \"type\": \"string\"},\n" + "               {\n"
        + "                 \"name\": \"Suit\", \n" + "                 \"type\": {\n"
        + "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"HEART\", \"CLUBS\"]\n"
        + "                }\n" + "              },\n"
        + "               {\"name\": \"salary\", \"type\": \"long\", \"default\" : 123}\n" + "           ]\n"
        + "        }";
    String valueSchemaStr2 = "{\n" + "           \"type\": \"record\",\n" + "           \"name\": \"KeyRecord\",\n"
        + "           \"fields\" : [\n"
        + "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n"
        + "               {\"name\": \"company\", \"type\": \"string\"},\n" + "               {\n"
        + "                 \"name\": \"Suit\", \n" + "                 \"type\": {\n"
        + "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"HEART\", \"CLUBS\"]\n"
        + "                }\n" + "              },\n"
        + "               {\"name\": \"salary\", \"type\": \"long\", \"default\" : 1234}\n" + "           ]\n"
        + "        }";
    createStore(storeName);
    SchemaEntry entry1 = schemaRepo.addValueSchema(storeName, valueSchemaStr1);
    // Add the same value schema
    SchemaEntry entry2 = schemaRepo.addValueSchema(storeName, valueSchemaStr2);
    SchemaEntry entry3 = schemaRepo.addValueSchema(storeName, valueSchemaStr1);

    Assert.assertNotEquals(entry2.getId(), entry1.getId());
    Assert.assertEquals(entry3.getId(), DUPLICATE_VALUE_SCHEMA_CODE);
    Assert.assertEquals(schemaRepo.getValueSchemas(storeName).size(), 2);
    Assert.assertEquals(2, schemaRepo.getValueSchemaId(storeName, valueSchemaStr2));
    Assert.assertEquals(1, schemaRepo.getValueSchemaId(storeName, valueSchemaStr1));
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void testAddInvalidValueSchema() {
    String storeName = "test_store1";
    String valueSchemaStr1 = "{\n" + "           \"type\": \"record\",\n" + "           \"name\": \"KeyRecord\",\n"
        + "           \"fields\" : [\n"
        + "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n"
        + "               {\"name\": \"company\", \"type\": \"string\"},\n" + "               {\n"
        + "                 \"name\": \"Suit\", \n" + "                 \"type\": {\n"
        + "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"HEART\", \"CLUBS\"]\n"
        + "                }\n" + "              },\n" + "               {\"name\": \"salary\", \"type\": \"long1\"}\n"
        + "           ]\n" + "        }";
    createStore(storeName);
    schemaRepo.addValueSchema(storeName, valueSchemaStr1);
  }

  @Test(expectedExceptions = VeniceNoStoreException.class)
  public void testAddValueSchemaToInvalidStore() {
    String storeName = "test_store1";
    String valueSchemaStr1 = "{\n" + "           \"type\": \"record\",\n" + "           \"name\": \"KeyRecord\",\n"
        + "           \"fields\" : [\n"
        + "               {\"name\": \"name\", \"type\": \"string\", \"doc\": \"name field\"},\n"
        + "               {\"name\": \"company\", \"type\": \"string\"},\n" + "               {\n"
        + "                 \"name\": \"Suit\", \n" + "                 \"type\": {\n"
        + "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"HEART\", \"CLUBS\"]\n"
        + "                }\n" + "              },\n" + "               {\"name\": \"salary\", \"type\": \"long\"}\n"
        + "           ]\n" + "        }";
    schemaRepo.addValueSchema(storeName, valueSchemaStr1);
  }

  @Test
  public void testAddValueSchemaWithSchemaId() {
    String storeName = "test_store1";
    String valueSchemaStr = "\"long\"";
    int valueSchemaId = 10;
    createStore(storeName);
    schemaRepo.addValueSchema(storeName, valueSchemaStr, valueSchemaId);
    SchemaEntry valueSchemaEntry = schemaRepo.getValueSchema(storeName, valueSchemaId);
    Assert.assertEquals(valueSchemaEntry.getSchema().toString(), valueSchemaStr);
    // Add the same schema with different schema id
    int newValueSchemaId = 11;
    schemaRepo.addValueSchema(storeName, valueSchemaStr, newValueSchemaId);
    valueSchemaEntry = schemaRepo.getValueSchema(storeName, newValueSchemaId);
    Assert.assertEquals(valueSchemaEntry.getSchema().toString(), valueSchemaStr);
  }

  @Test
  public void testGetValueSchemaIdWithSimilarSchema() {
    String storeName = "test_store1";
    String valueSchemaStr1 = "{" + "\"fields\": ["
        + "   {\"default\": \"\", \"doc\": \"test field\", \"name\": \"testField1\", \"type\": \"string\"},"
        + "   {\"default\": 0, \"doc\": \"test field two\", \"name\": \"testField2\", \"type\": \"float\"}" + "   ],"
        + " \"name\": \"testObject\", \"type\": \"record\"" + "}";

    String valueSchemaStr2 = "{" + "\"fields\": ["
        + "   {\"default\": \"\", \"doc\": \"test field\", \"name\": \"testField1\", \"type\": \"string\"},"
        + "   {\"default\": -1, \"doc\": \"test field two\", \"name\": \"testField2\", \"type\": \"float\"}" + "   ],"
        + " \"name\": \"testObject\", \"type\": \"record\"" + "}";
    createStore(storeName);
    schemaRepo.addValueSchema(storeName, valueSchemaStr1);
    schemaRepo.addValueSchema(storeName, valueSchemaStr2);
    int schemaId = schemaRepo.getValueSchemaId(storeName, valueSchemaStr2);
    Assert.assertEquals(schemaId, 2, "getValueSchemaId did not get the correct schema which is an exact match");
  }

  @Test
  public void testGetDerivedSchemaAvroString() {
    String storeName = "test_store1";
    String valueSchemaStr1 = "{" + "\"fields\": ["
        + "   {\"default\": \"\", \"doc\": \"test field\", \"name\": \"testField1\", \"type\": \"string\"},"
        + "   {\"default\": 0, \"doc\": \"test field two\", \"name\": \"testField2\", \"type\": \"float\"}" + "   ],"
        + " \"name\": \"testObject\", \"type\": \"record\"" + "}";

    String derivedSchemaStr = "{" + "\"fields\": ["
        + "   {\"default\": \"\", \"doc\": \"test field\", \"name\": \"testField1\", \"type\": \"string\"},"
        + "   {\"default\": -1, \"doc\": \"test field two\", \"name\": \"testField2\", \"type\": \"float\"}" + "   ],"
        + " \"name\": \"testObject\", \"type\": \"record\"" + "}";
    String derivedSchemaAvroStr = "{" + "\"fields\": ["
        + "   {\"default\": \"\", \"doc\": \"test field\", \"name\": \"testField1\", \"type\": \"string\", \"avro.java.string\": \"string\"},"
        + "   {\"default\": -1, \"doc\": \"test field two\", \"name\": \"testField2\", \"type\": \"float\"}" + "   ],"
        + " \"name\": \"testObject\", \"type\": \"record\"" + "}";
    createStore(storeName);
    schemaRepo.addValueSchema(storeName, valueSchemaStr1);
    schemaRepo.addDerivedSchema(storeName, derivedSchemaStr, 1);
    GeneratedSchemaID schemaId = schemaRepo.getDerivedSchemaId(storeName, derivedSchemaAvroStr);
    Assert.assertEquals(
        schemaId.getGeneratedSchemaVersion(),
        1,
        "getValueSchemaId did not get the correct schema which is an exact match");
  }
}
