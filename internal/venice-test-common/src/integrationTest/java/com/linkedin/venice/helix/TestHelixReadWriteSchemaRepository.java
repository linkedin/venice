package com.linkedin.venice.helix;

import static com.linkedin.venice.schema.SchemaData.DUPLICATE_VALUE_SCHEMA_CODE;
import static com.linkedin.venice.zk.VeniceZkPaths.STORES;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.exceptions.SchemaIncompatibilityException;
import com.linkedin.venice.exceptions.StoreKeySchemaExistException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.ValueSchemaCreatedListener;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.schema.GeneratedSchemaID;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.avro.SchemaParseException;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestHelixReadWriteSchemaRepository {
  private String zkAddress;
  private ZkClient zkClient;
  private String cluster = "test-metadata-cluster";
  private String clusterPath = "/test-metadata-cluster";
  private String storesPath = "/" + STORES;
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
    schemaRepo = new HelixReadWriteSchemaRepository(storeRepo, zkClient, adapter, cluster, Optional.empty(), 9);
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
    Assert.assertEquals(schemaRepo.addValueSchema(storeName, valueSchemaStr, valueSchemaId).getId(), valueSchemaId);
    Assert.assertEquals(schemaRepo.getValueSchemas(storeName).size(), 1);
    // Add the same schema with different schema id
    int newValueSchemaId = 11;
    schemaRepo.addValueSchema(storeName, valueSchemaStr, newValueSchemaId);
    valueSchemaEntry = schemaRepo.getValueSchema(storeName, newValueSchemaId);
    Assert.assertEquals(valueSchemaEntry.getSchema().toString(), valueSchemaStr);
  }

  @DataProvider
  public Object[][] repositoryInstances() {
    return new Object[][] { { false }, { true } };
  }

  @Test(dataProvider = "repositoryInstances", timeOut = 15_000)
  public void testConcurrentValueSchemaRegistrationWithStaleIds(boolean separateRepositories) throws Exception {
    String storeName = "concurrent_value_schema";
    String schema = "\"string\"";
    createStore(storeName);
    HelixReadWriteSchemaRepository otherRepo =
        separateRepositories ? new HelixReadWriteSchemaRepository(storeRepo, Optional.empty(), accessor) : schemaRepo;
    int firstId =
        schemaRepo.preCheckValueSchemaAndGetNextAvailableId(storeName, schema, DirectionalSchemaCompatibilityType.NONE);
    int secondId =
        otherRepo.preCheckValueSchemaAndGetNextAvailableId(storeName, schema, DirectionalSchemaCompatibilityType.NONE);
    Assert.assertEquals(firstId, 1);
    Assert.assertEquals(secondId, 1);
    ValueSchemaCreatedListener listener = mock(ValueSchemaCreatedListener.class);
    schemaRepo.registerValueSchemaCreatedListener(listener);
    otherRepo.registerValueSchemaCreatedListener(listener);

    List<SchemaEntry> results = registerConcurrently(
        () -> schemaRepo.addValueSchema(storeName, schema, firstId),
        () -> otherRepo.addValueSchema(storeName, schema, secondId));

    results.forEach(result -> Assert.assertEquals(result.getId(), 1));
    Assert.assertEquals(schemaRepo.getValueSchemas(storeName).size(), 1);
    verify(listener, times(1)).handleValueSchemaCreated(any(Store.class), any(SchemaEntry.class));
  }

  @Test(dataProvider = "repositoryInstances", timeOut = 15_000)
  public void testConcurrentDerivedSchemaRegistration(boolean separateRepositories) throws Exception {
    String storeName = "concurrent_derived_schema";
    String schema = "\"string\"";
    createStore(storeName);
    schemaRepo.addValueSchema(storeName, schema);
    HelixReadWriteSchemaRepository first;
    HelixReadWriteSchemaRepository second;
    if (separateRepositories) {
      CyclicBarrier reads = new CyclicBarrier(2);
      HelixSchemaAccessor coordinatedAccessor = spy(accessor);
      doAnswer(invocation -> {
        Object snapshot = invocation.callRealMethod();
        reads.await(5, TimeUnit.SECONDS);
        return snapshot;
      }).when(coordinatedAccessor).getAllDerivedSchemas(storeName);
      first = new HelixReadWriteSchemaRepository(storeRepo, Optional.empty(), coordinatedAccessor);
      second = new HelixReadWriteSchemaRepository(storeRepo, Optional.empty(), coordinatedAccessor);
    } else {
      first = schemaRepo;
      second = schemaRepo;
    }

    List<DerivedSchemaEntry> results = registerConcurrently(
        () -> first.addDerivedSchema(storeName, schema, 1),
        () -> second.addDerivedSchema(storeName, schema, 1));

    for (DerivedSchemaEntry result: results) {
      Assert.assertEquals(result.getValueSchemaID(), 1);
      Assert.assertTrue(result.getId() == 1 || result.getId() == DUPLICATE_VALUE_SCHEMA_CODE);
      Assert.assertEquals(result.getSchema().toString(), schema);
    }
    Assert.assertEquals(schemaRepo.getDerivedSchemas(storeName).size(), 1);
  }

  @Test(timeOut = 15_000)
  public void testConcurrentDifferentDerivedSchemasReceiveDistinctIds() throws Exception {
    String storeName = "different_derived_schemas";
    createStore(storeName);
    schemaRepo.addValueSchema(storeName, "\"string\"");

    List<DerivedSchemaEntry> results = registerConcurrently(
        () -> schemaRepo.addDerivedSchema(storeName, "\"string\"", 1),
        () -> schemaRepo.addDerivedSchema(storeName, "\"long\"", 1));

    Assert.assertNotEquals(results.get(0).getId(), results.get(1).getId());
    Assert.assertEquals(schemaRepo.getDerivedSchemas(storeName).size(), 2);
  }

  @DataProvider
  public Object[][] schemaContent() {
    String record =
        "{\"type\":\"record\",\"name\":\"R\",\"doc\":\"old\",\"fields\":[{\"name\":\"f\",\"type\":\"string\"}]}";
    String reorderedRecord =
        "{\"fields\":[{\"type\":\"string\",\"name\":\"f\"}],\"doc\":\"old\",\"name\":\"R\",\"type\":\"record\"}";
    String enumSchema = "{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"A\",\"B\"],\"default\":\"A\"}";
    String aliasedRecord = record.replace("\"doc\":\"old\"", "\"doc\":\"old\",\"aliases\":[\"OldR\"]");
    return new Object[][] { { false, record, reorderedRecord, record.replace("old", "new") },
        { true, record, reorderedRecord, record.replace("old", "new") },
        { false, aliasedRecord, aliasedRecord, aliasedRecord.replace("OldR", "OtherR") },
        { true, aliasedRecord, aliasedRecord, aliasedRecord.replace("OldR", "OtherR") },
        { false, enumSchema, enumSchema, enumSchema.replace("\"default\":\"A\"", "\"default\":\"B\"") },
        { true, enumSchema, enumSchema, enumSchema.replace("\"default\":\"A\"", "\"default\":\"B\"") } };
  }

  @Test(dataProvider = "schemaContent", timeOut = 15_000)
  public void testIdempotentSchemaCreationPreservesContent(
      boolean derived,
      String original,
      String equivalent,
      String conflicting) {
    String storeName = "schema_content";
    createStore(storeName);
    addSchema(derived, storeName, original);
    addSchema(derived, storeName, equivalent);

    Assert.assertThrows(VeniceException.class, () -> addSchema(derived, storeName, conflicting));

    SchemaEntry stored =
        derived ? accessor.getDerivedSchema(storeName, "1-1") : accessor.getValueSchema(storeName, "1");
    Assert.assertEquals(stored.getSchema().toString(), new SchemaEntry(1, original).getSchema().toString());
  }

  private void addSchema(boolean derived, String storeName, String schema) {
    if (derived) {
      accessor.addDerivedSchema(storeName, new DerivedSchemaEntry(1, 1, schema));
    } else {
      accessor.addValueSchema(storeName, new SchemaEntry(1, schema));
    }
  }

  private <T> List<T> registerConcurrently(Callable<T> first, Callable<T> second) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CyclicBarrier start = new CyclicBarrier(2);
    try {
      Future<T> firstResult = executor.submit(() -> {
        start.await(5, TimeUnit.SECONDS);
        return first.call();
      });
      Future<T> secondResult = executor.submit(() -> {
        start.await(5, TimeUnit.SECONDS);
        return second.call();
      });
      return Arrays.asList(firstResult.get(5, TimeUnit.SECONDS), secondResult.get(5, TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
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
