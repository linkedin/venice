package com.linkedin.venice.helix;

import static com.linkedin.venice.zk.VeniceZkPaths.STORES;

import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestHelixReadOnlySchemaRepository {
  private String zkAddress;
  private ZkClient zkClient;
  private String cluster = "test-metadata-cluster";
  private String clusterPath = "/test-metadata-cluster";
  private String storesPath = "/" + STORES;
  private ZkServerWrapper zkServerWrapper;
  private HelixAdapterSerializer adapter = new HelixAdapterSerializer();

  HelixReadWriteStoreRepository storeRWRepo;
  HelixReadOnlyStoreRepository storeRORepo;
  HelixReadWriteSchemaRepository schemaRWRepo;
  HelixReadOnlySchemaRepository schemaRORepo;

  @BeforeMethod
  public void zkSetup() {
    zkServerWrapper = ServiceFactory.getZkServer();
    zkAddress = zkServerWrapper.getAddress();
    zkClient = ZkClientFactory.newZkClient(zkAddress);
    zkClient.setZkSerializer(adapter);
    zkClient.create(clusterPath, null, CreateMode.PERSISTENT);
    zkClient.create(clusterPath + storesPath, null, CreateMode.PERSISTENT);

    storeRWRepo = new HelixReadWriteStoreRepository(
        zkClient,
        adapter,
        cluster,
        Optional.empty(),
        new ClusterLockManager(cluster));
    storeRWRepo.refresh();
    storeRORepo = new HelixReadOnlyStoreRepository(zkClient, adapter, cluster, 1, 1000);
    storeRORepo.refresh();
    schemaRWRepo = new HelixReadWriteSchemaRepository(storeRWRepo, zkClient, adapter, cluster, Optional.empty());
    schemaRORepo = new HelixReadOnlySchemaRepository(storeRORepo, zkClient, adapter, cluster, 1, 1000);
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
    storeRWRepo.addStore(store);
    TestUtils.waitForNonDeterministicCompletion(3, TimeUnit.SECONDS, () -> storeRORepo.hasStore(storeName));
  }

  @Test
  public void testGetKeySchema() {
    String storeName = "test_store1";
    // create store first
    createStore(storeName);
    Assert.assertNull(schemaRORepo.getKeySchema(storeName));

    // Query key schema again after setting up key schema
    String keySchemaStr = "\"string\"";
    schemaRWRepo.initKeySchema(storeName, keySchemaStr);
    TestUtils
        .waitForNonDeterministicCompletion(3, TimeUnit.SECONDS, () -> schemaRORepo.getKeySchema(storeName) != null);
    SchemaEntry keySchema = schemaRORepo.getKeySchema(storeName);
    Assert.assertNotNull(keySchema);
    Assert.assertEquals(keySchema.getId(), Integer.parseInt(HelixSchemaAccessor.KEY_SCHEMA_ID));
    Assert.assertEquals(keySchema.getSchema().toString(), keySchemaStr);
  }

  @Test(expectedExceptions = VeniceNoStoreException.class)
  public void testGetKeySchemaByInvalidStore() {
    String storeName = "test_store1";
    schemaRORepo.getKeySchema(storeName);
  }

  @Test
  public void testGetValueSchemaId() {
    String storeName = "test_store1";
    String valueSchemaStr = "\"string\"";
    createStore(storeName);
    schemaRWRepo.addValueSchema(storeName, valueSchemaStr);
    TestUtils.waitForNonDeterministicCompletion(
        3,
        TimeUnit.SECONDS,
        () -> schemaRORepo.getValueSchemas(storeName).size() == 1);
    Assert
        .assertNotEquals(SchemaData.INVALID_VALUE_SCHEMA_ID, schemaRORepo.getValueSchemaId(storeName, valueSchemaStr));
    Assert.assertTrue(schemaRORepo.hasValueSchema(storeName, 1));
    Assert.assertFalse(schemaRORepo.hasValueSchema(storeName, 2));
  }

  @Test(expectedExceptions = VeniceNoStoreException.class)
  public void testGetValueSchemaIdByInvalidStore() throws InterruptedException {
    String storeName = "test_store1";
    String valueSchemaStr = "\"string\"";
    schemaRORepo.getValueSchemaId(storeName, valueSchemaStr);
  }

  @Test
  public void testGetValueSchema() {
    String storeName = "test_store1";
    createStore(storeName);
    Assert.assertNull(schemaRORepo.getValueSchema(storeName, 1));
    // Add new value schema
    String valueSchemaStr1 = "{\n" + "\t\"type\": \"record\",\n" + "\t\"name\": \"key\",\n" + "\t\"fields\": [\n"
        + "\t\t{\"type\": \"string\", \"name\": \"id\"}\n" + "\t]\n" + "}";
    String valueSchemaStr2 = "{\n" + "\t\"type\": \"record\",\n" + "\t\"name\": \"key\",\n" + "\t\"fields\": [\n"
        + "\t\t{\"type\": \"string\", \"name\": \"id\"},\n"
        + "\t\t{\"type\": [\"string\",\"null\"], \"name\": \"stuff\", \"default\": \"null\", \"doc\": \"new field\"}\n"
        + "\t]\n" + "}";

    schemaRWRepo.addValueSchema(storeName, valueSchemaStr1);
    schemaRWRepo.addValueSchema(storeName, valueSchemaStr2);
    TestUtils.waitForNonDeterministicCompletion(
        3,
        TimeUnit.SECONDS,
        () -> schemaRORepo.getValueSchemas(storeName).size() == 2);

    SchemaEntry valueSchema1 = schemaRORepo.getValueSchema(storeName, 1);
    Assert.assertNotNull(valueSchema1);
    Assert.assertEquals(valueSchema1.getSchema().toString(), Schema.parse(valueSchemaStr1).toString());
    SchemaEntry valueSchema2 = schemaRORepo.getValueSchema(storeName, 2);
    Assert.assertNotNull(valueSchema2);
    Assert.assertEquals(valueSchema2.getSchema().toString(), Schema.parse(valueSchemaStr2).toString());
    Assert.assertNull(schemaRORepo.getValueSchema(storeName, 3));

    // After clear, we should be able to get schema info as well
    schemaRORepo.clear();
    Collection<SchemaEntry> valueSchemas = schemaRORepo.getValueSchemas(storeName);
    Assert.assertEquals(valueSchemas.size(), 2);
    Assert.assertTrue(valueSchemas.contains(valueSchema1));
    Assert.assertTrue(valueSchemas.contains(valueSchema2));

    // After removing the store, we should not be able to get schema for it anymore
    Assert.assertNotNull(schemaRORepo.getValueSchema(storeName, 1));
    storeRWRepo.deleteStore(storeName);
    TestUtils.waitForNonDeterministicCompletion(3, TimeUnit.SECONDS, () -> !storeRORepo.hasStore(storeName));
    try {
      schemaRORepo.getValueSchema(storeName, 1);
      Assert.assertTrue(false);
    } catch (VeniceNoStoreException e) {
      Assert.assertTrue(true);
    }
  }

  @Test(expectedExceptions = VeniceNoStoreException.class)
  public void testGetValueSchemaByInvalidStore() {
    String storeName = "test_store1";
    schemaRORepo.getValueSchema(storeName, 1);
  }

  @Test
  public void testStoreDeletion() {
    String storeName = "test_store1";
    createStore(storeName);
    // Add new value schema
    String valueSchemaStr1 = "{\n" + "\t\"type\": \"record\",\n" + "\t\"name\": \"key\",\n" + "\t\"fields\": [\n"
        + "\t\t{\"type\": \"string\", \"name\": \"id\"}\n" + "\t]\n" + "}";

    schemaRWRepo.addValueSchema(storeName, valueSchemaStr1);
    TestUtils.waitForNonDeterministicCompletion(
        3,
        TimeUnit.SECONDS,
        () -> schemaRORepo.getValueSchemas(storeName).size() == 1);
    SchemaEntry valueSchema1 = schemaRORepo.getValueSchema(storeName, 1);
    Assert.assertNotNull(valueSchema1);

    // Delete store, then add the same store
    storeRWRepo.deleteStore(storeName);
    // TODO:If we execute deleteStore and createStore without sleep, the RO store repo will
    // only receive one notification for store creation.
    TestUtils.waitForNonDeterministicCompletion(3, TimeUnit.SECONDS, () -> !storeRORepo.hasStore(storeName));
    createStore(storeName);
    // The legacy value schema should not be there
    Assert.assertNull(schemaRORepo.getValueSchema(storeName, 1));
  }
}
