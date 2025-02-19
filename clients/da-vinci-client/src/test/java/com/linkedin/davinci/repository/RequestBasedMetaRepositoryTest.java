package com.linkedin.davinci.repository;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.meta.ReadOnlyStore;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.metadata.response.StorePropertiesResponseRecord;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serializer.FastAvroSerializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.systemstore.schemas.StoreKeySchemas;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.systemstore.schemas.StoreValueSchemas;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class RequestBasedMetaRepositoryTest {
  private static final Logger LOGGER = LogManager.getLogger(RequestBasedMetaRepositoryTest.class);
  private Random RANDOM;

  private Store store;
  private static final String D2_SERVICE_NAME = "D2_SERVICE_NAME";
  private StorePropertiesResponseRecord MOCK_STORE_PROPERTIES_RESPONSE_RECORD;

  // Mock schemas
  private static final String INT_KEY_SCHEMA = "\"int\"";
  private static final String VALUE_SCHEMA_1 = "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"TestValue\",\n"
      + "  \"fields\": [\n" + "   {\"name\": \"test_field1\", \"type\": \"string\"}\n" + "  ]\n" + "}";
  private static final String VALUE_SCHEMA_2 = "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"TestValue\",\n"
      + "  \"fields\": [\n" + "   {\"name\": \"test_field1\", \"type\": \"string\"},\n"
      + "   {\"name\": \"test_field2\", \"type\": \"int\", \"default\": 0}\n" + "  ]\n" + "}";

  @BeforeClass
  public void beforeClassSetup() {
    long seed = System.nanoTime();
    RANDOM = new Random(seed);
    LOGGER.info("Random seed set: {}", seed);
  }

  @BeforeMethod
  public void beforeMethodSetup() {

    // Store
    setupTestStore();

    // StorePropertiesResponseRecord
    setupTestStorePropertiesResponse();
  }

  @Test
  public void testRequestBasedMetaRepositoryFetchStoreConfigFromRemote() {

    // Mock RequestBasedMetaRepository
    RequestBasedMetaRepository requestBasedMetaRepository = getMockRequestBasedMetaRepository();
    when(requestBasedMetaRepository.fetchStoreConfigFromRemote(store.getName())).thenCallRealMethod();

    // Test FetchStoreConfigFromRemote
    StoreConfig storeConfig = requestBasedMetaRepository.fetchStoreConfigFromRemote(store.getName());
    Assert.assertNotNull(storeConfig);
    Assert.assertEquals(storeConfig.getCluster(), D2_SERVICE_NAME);
    Assert.assertEquals(storeConfig.getStoreName(), store.getName());
    Assert.assertNull(storeConfig.getMigrationDestCluster());
    Assert.assertNull(storeConfig.getMigrationSrcCluster());
  }

  @Test
  public void testRequestBasedMetaRepositoryFetchStoreFromRemote() {

    // Mock RequestBasedMetaRepository
    RequestBasedMetaRepository requestBasedMetaRepository = getMockRequestBasedMetaRepository();
    when(requestBasedMetaRepository.fetchStoreConfigFromRemote(store.getName())).thenCallRealMethod();

    // Test FetchStoreConfigFromRemote
    StoreConfig storeConfig = requestBasedMetaRepository.fetchStoreConfigFromRemote(store.getName());
    Assert.assertNotNull(storeConfig);
    Assert.assertEquals(storeConfig.getCluster(), D2_SERVICE_NAME);
    Assert.assertEquals(storeConfig.getStoreName(), store.getName());
    Assert.assertEquals(storeConfig.getMigrationDestCluster(), null);
    Assert.assertEquals(storeConfig.getMigrationSrcCluster(), null);
  }

  @Test
  public void testRequestBasedMetaRepositoryFetchAndCacheStorePropertiesResponseRecord() {

    // Mock RequestBasedMetaRepository
    RequestBasedMetaRepository requestBasedMetaRepository = getMockRequestBasedMetaRepository();
    when(requestBasedMetaRepository.fetchAndCacheStorePropertiesResponseRecord(store.getName())).thenCallRealMethod();

    // Test FetchAndCacheStorePropertiesResponseRecord
    StorePropertiesResponseRecord record =
        requestBasedMetaRepository.fetchAndCacheStorePropertiesResponseRecord(store.getName());
    Assert.assertNotNull(record);
    Assert.assertNotNull(record.getStoreMetaValue());
    Assert.assertNotNull(record.getStoreMetaValue().getStoreProperties());
    Assert.assertEquals(record.getStoreMetaValue().getStoreProperties().getName().toString(), store.getName());
    Assert.assertEquals(record.getStoreMetaValue().getStoreProperties().getOwner().toString(), store.getOwner());
    Assert
        .assertEquals(record.getStoreMetaValue().getStoreProperties().getVersions().size(), store.getVersions().size());
    Assert.assertEquals(record.getStoreMetaValue().getStoreKeySchemas().getKeySchemaMap().size(), 1);
    Assert.assertEquals(record.getStoreMetaValue().getStoreValueSchemas().getValueSchemaMap().size(), 2);
  }

  @Test
  public void testRequestBasedMetaRepositoryGetMaxValueSchemaId() {

    // Mock RequestBasedMetaRepository
    RequestBasedMetaRepository requestBasedMetaRepository = getMockRequestBasedMetaRepository();
    when(requestBasedMetaRepository.getMaxValueSchemaId(store.getName())).thenCallRealMethod();

    // Test GetMaxValueSchemaId
    int schemaId = requestBasedMetaRepository.getMaxValueSchemaId(store.getName());
    Assert.assertEquals(schemaId, SchemaData.UNKNOWN_SCHEMA_ID);

    // Put schema
    SchemaEntry schemaEntryKey = new SchemaEntry(0, INT_KEY_SCHEMA);
    SchemaEntry schemaEntryValue1 = new SchemaEntry(1, VALUE_SCHEMA_1);
    SchemaEntry schemaEntryValue2 = new SchemaEntry(2, VALUE_SCHEMA_2);
    SchemaData schemaData = new SchemaData(store.getName(), schemaEntryKey);
    schemaData.addValueSchema(schemaEntryValue1);
    schemaData.addValueSchema(schemaEntryValue2);
    requestBasedMetaRepository.schemaMap.put(store.getName(), schemaData);

    // Test GetMaxValueSchemaId
    schemaId = requestBasedMetaRepository.getMaxValueSchemaId(store.getName());
    Assert.assertEquals(schemaId, 2);
  }

  @Test
  public void testRequestBasedMetaRepositoryCacheStoreSchema() {

    // Mock RequestBasedMetaRepository
    RequestBasedMetaRepository requestBasedMetaRepository = getMockRequestBasedMetaRepository();
    doCallRealMethod().when(requestBasedMetaRepository)
        .cacheStoreSchema(store.getName(), MOCK_STORE_PROPERTIES_RESPONSE_RECORD);

    requestBasedMetaRepository.cacheStoreSchema(store.getName(), MOCK_STORE_PROPERTIES_RESPONSE_RECORD);
    SchemaData schemaData = requestBasedMetaRepository.storeSchemaMap.get(store.getName());
    Assert.assertNotNull(schemaData);
    Assert.assertEquals(schemaData.getKeySchema().getSchemaStr(), INT_KEY_SCHEMA);
    Assert.assertEquals(schemaData.getValueSchema(1).getSchemaStr(), VALUE_SCHEMA_1);
    Assert.assertEquals(schemaData.getValueSchema(2).getSchemaStr(), VALUE_SCHEMA_2);
  }

  private RequestBasedMetaRepository getMockRequestBasedMetaRepository() {
    RequestBasedMetaRepository requestBasedMetaRepository = mock(RequestBasedMetaRepository.class);

    // Schema Map
    requestBasedMetaRepository.storeSchemaMap = new VeniceConcurrentHashMap<>();
    requestBasedMetaRepository.schemaMap = new VeniceConcurrentHashMap<>();

    // Mock D2TransportClient
    try {
      D2TransportClient d2TransportClient = getMockD2TransportClient();
      when(requestBasedMetaRepository.getD2TransportClient(store.getName())).thenReturn(d2TransportClient);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }

    // Mock max value schema id
    when(requestBasedMetaRepository.getMaxValueSchemaId(store.getName())).thenReturn(SchemaData.UNKNOWN_SCHEMA_ID);

    return requestBasedMetaRepository;
  }

  private D2TransportClient getMockD2TransportClient()
      throws InterruptedException, java.util.concurrent.ExecutionException {

    // Mock D2
    D2TransportClient d2TransportClient = mock(D2TransportClient.class);
    when(d2TransportClient.getServiceName()).thenReturn(D2_SERVICE_NAME);

    // Mock request
    String mockURL = QueryAction.STORE_PROPERTIES.toString().toLowerCase() + "/" + store.getName();
    TransportClientResponse mockResponse = mock(TransportClientResponse.class);
    CompletableFuture<TransportClientResponse> completableFuture = mock(CompletableFuture.class);
    RecordSerializer<StorePropertiesResponseRecord> recordSerializer =
        new FastAvroSerializer<>(StorePropertiesResponseRecord.SCHEMA$, null);
    when(completableFuture.get()).thenReturn(mockResponse);
    when(mockResponse.getBody()).thenReturn(recordSerializer.serialize(MOCK_STORE_PROPERTIES_RESPONSE_RECORD));
    when(d2TransportClient.get(mockURL)).thenReturn(completableFuture);

    return d2TransportClient;
  }

  private void setupTestStore() {
    store = TestUtils.populateZKStore(
        (ZKStore) TestUtils.createTestStore(
            Long.toString(RANDOM.nextLong()),
            Long.toString(RANDOM.nextLong()),
            System.currentTimeMillis()),
        RANDOM);
  }

  private void setupTestStorePropertiesResponse() {
    StorePropertiesResponseRecord record = new StorePropertiesResponseRecord();

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

    record.setStoreMetaValue(storeMetaValue);

    MOCK_STORE_PROPERTIES_RESPONSE_RECORD = record;
  }
}
