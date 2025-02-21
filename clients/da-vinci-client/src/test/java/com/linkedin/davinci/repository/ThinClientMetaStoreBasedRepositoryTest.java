package com.linkedin.davinci.repository;

import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_CLUSTER_NAME;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_STORE_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.meta.ReadOnlyStore;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreClusterConfig;
import com.linkedin.venice.systemstore.schemas.StoreKeySchemas;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.systemstore.schemas.StoreValueSchemas;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ThinClientMetaStoreBasedRepositoryTest {
  private static final Logger LOGGER = LogManager.getLogger(ThinClientMetaStoreBasedRepositoryTest.class);
  private Random RANDOM;

  private Store store;
  private final String CLUSTER_NAME = "CLUSTER_NAME";

  // Mock schemas
  private static final String INT_KEY_SCHEMA = "\"int\"";
  private static final String VALUE_SCHEMA_1 = "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"TestValue\",\n"
      + "  \"fields\": [\n" + "   {\"name\": \"test_field1\", \"type\": \"string\"}\n" + "  ]\n" + "}";
  private static final String VALUE_SCHEMA_2 = "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"TestValue\",\n"
      + "  \"fields\": [\n" + "   {\"name\": \"test_field1\", \"type\": \"string\"},\n"
      + "   {\"name\": \"test_field2\", \"type\": \"int\", \"default\": 0}\n" + "  ]\n" + "}";

  @BeforeClass
  public void setupRandom() {
    long seed = System.nanoTime();
    RANDOM = new Random(seed);
    LOGGER.info("Random seed set: {}", seed);
  }

  @BeforeMethod
  public void beforeMethodSetup() {

    // Store
    setUpTestStore();
  }

  @Test
  public void testFetchStoreConfigFromRemote() {

    // Mock ThinClientMetaStoreBasedRepository
    ThinClientMetaStoreBasedRepository thinClientMetaStoreBasedRepository = getMockThinClientMetaStoreBasedRepository();

    // Test FetchStoreConfigFromRemote
    when(thinClientMetaStoreBasedRepository.fetchStoreConfigFromRemote(store.getName())).thenCallRealMethod();
    StoreConfig storeConfig = thinClientMetaStoreBasedRepository.fetchStoreConfigFromRemote(store.getName());
    Assert.assertNotNull(storeConfig);
    Assert.assertEquals(storeConfig.getStoreName(), store.getName());
    Assert.assertEquals(storeConfig.getCluster(), CLUSTER_NAME);
  }

  @Test
  public void testGetSchemaData() {

    // Mock ThinClientMetaStoreBasedRepository
    ThinClientMetaStoreBasedRepository thinClientMetaStoreBasedRepository = getMockThinClientMetaStoreBasedRepository();

    // Test GetSchemaData
    when(thinClientMetaStoreBasedRepository.getSchemaData(store.getName())).thenCallRealMethod();
    SchemaData schemaData = thinClientMetaStoreBasedRepository.getSchemaData(store.getName());
    Assert.assertNotNull(schemaData);
    Assert.assertEquals(schemaData.getStoreName(), store.getName());
    Assert.assertEquals(schemaData.getKeySchema().getSchemaStr(), INT_KEY_SCHEMA);
    Assert.assertEquals(schemaData.getValueSchema(1).getSchemaStr(), VALUE_SCHEMA_1);
    Assert.assertEquals(schemaData.getValueSchema(2).getSchemaStr(), VALUE_SCHEMA_2);
  }

  private ThinClientMetaStoreBasedRepository getMockThinClientMetaStoreBasedRepository() {
    ThinClientMetaStoreBasedRepository thinClientMetaStoreBasedRepository =
        mock(ThinClientMetaStoreBasedRepository.class);

    // schemaMap
    thinClientMetaStoreBasedRepository.schemaMap = new VeniceConcurrentHashMap<>();

    // StoreMetaValue
    StoreMetaValue storeMetaValue = new StoreMetaValue();
    storeMetaValue.setStoreProperties(new ReadOnlyStore(store).cloneStoreProperties());

    // Key Schema
    Map<CharSequence, CharSequence> storeKeySchemas = new VeniceConcurrentHashMap<>();
    storeKeySchemas.put("0", INT_KEY_SCHEMA);
    storeMetaValue.setStoreKeySchemas(new StoreKeySchemas(storeKeySchemas));

    // Value Schemas
    Map<CharSequence, CharSequence> storeValueSchemas = new VeniceConcurrentHashMap<>();
    storeValueSchemas.put("1", VALUE_SCHEMA_1);
    storeValueSchemas.put("2", VALUE_SCHEMA_2);

    storeMetaValue.setStoreValueSchemas(new StoreValueSchemas(storeValueSchemas));

    // Store Cluster Config
    StoreClusterConfig storeClusterConfig = new StoreClusterConfig();
    storeClusterConfig.setStoreName(store.getName());
    storeClusterConfig.setCluster(CLUSTER_NAME);
    storeMetaValue.setStoreClusterConfig(storeClusterConfig);

    // Mock getStoreMetaValue
    StoreMetaKey storeMetaKey;
    storeMetaKey = MetaStoreDataType.STORE_KEY_SCHEMAS
        .getStoreMetaKey(Collections.singletonMap(KEY_STRING_STORE_NAME, store.getName()));
    when(thinClientMetaStoreBasedRepository.getStoreMetaValue(store.getName(), storeMetaKey))
        .thenReturn(storeMetaValue);
    storeMetaKey = MetaStoreDataType.STORE_VALUE_SCHEMAS
        .getStoreMetaKey(Collections.singletonMap(KEY_STRING_STORE_NAME, store.getName()));
    when(thinClientMetaStoreBasedRepository.getStoreMetaValue(store.getName(), storeMetaKey))
        .thenReturn(storeMetaValue);
    storeMetaKey = MetaStoreDataType.STORE_PROPERTIES.getStoreMetaKey(new HashMap<String, String>() {
      {
        put(KEY_STRING_STORE_NAME, store.getName());
        put(KEY_STRING_CLUSTER_NAME, CLUSTER_NAME);
      }
    });
    when(thinClientMetaStoreBasedRepository.getStoreMetaValue(store.getName(), storeMetaKey))
        .thenReturn(storeMetaValue);
    storeMetaKey = MetaStoreDataType.STORE_CLUSTER_CONFIG
        .getStoreMetaKey(Collections.singletonMap(KEY_STRING_STORE_NAME, store.getName()));
    when(thinClientMetaStoreBasedRepository.getStoreMetaValue(store.getName(), storeMetaKey))
        .thenReturn(storeMetaValue);

    return thinClientMetaStoreBasedRepository;
  }

  private void setUpTestStore() {
    store = TestUtils.populateZKStore(
        (ZKStore) TestUtils.createTestStore(
            Long.toString(RANDOM.nextLong()),
            Long.toString(RANDOM.nextLong()),
            System.currentTimeMillis()),
        RANDOM);
  }
}
