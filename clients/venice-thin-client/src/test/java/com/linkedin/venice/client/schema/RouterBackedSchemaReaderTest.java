package com.linkedin.venice.client.schema;

import static com.linkedin.venice.utils.TestWriteUtils.loadFileAsStringQuietlyWithErrorLogged;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.controllerapi.MultiSchemaIdResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.StoreJSONSerializer;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RouterBackedSchemaReaderTest {
  private static final ObjectMapper MAPPER = ObjectMapperFactory.getInstance();
  private static final int TIMEOUT = 3;
  private static final ICProvider mockICProvider = new ICProvider() {
    @Override
    public <T> T call(String traceContext, Callable<T> callable) throws Exception {
      return callable.call();
    }
  };
  private static final StoreJSONSerializer STORE_SERIALIZER = new StoreJSONSerializer();

  private static final String storeName = "test_store";
  private static final String clusterName = "test-cluster";

  private static final Schema KEY_SCHEMA = AvroCompatibilityHelper.parse("\"string\"");
  private static final Schema VALUE_SCHEMA_1 =
      AvroCompatibilityHelper.parse(loadFileAsStringQuietlyWithErrorLogged("RecordValueSchema1.avsc"));
  private static final Schema VALUE_SCHEMA_2 =
      AvroCompatibilityHelper.parse(loadFileAsStringQuietlyWithErrorLogged("RecordValueSchema2.avsc"));
  private static final Schema VALUE_SCHEMA_3 =
      AvroCompatibilityHelper.parse(loadFileAsStringQuietlyWithErrorLogged("RecordValueSchema3.avsc"));
  private static final Schema VALUE_SCHEMA_4 =
      AvroCompatibilityHelper.parse(loadFileAsStringQuietlyWithErrorLogged("RecordValueSchema4.avsc"));

  private static final Schema UPDATE_SCHEMA_1 =
      WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(VALUE_SCHEMA_1);
  private static final Schema UPDATE_SCHEMA_2 =
      WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(VALUE_SCHEMA_2);
  private static final Schema UPDATE_SCHEMA_3 =
      WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(VALUE_SCHEMA_3);
  private static final Schema UPDATE_SCHEMA_4 =
      WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(VALUE_SCHEMA_4);

  @Test
  public void testGetKeySchema() throws IOException, ExecutionException, InterruptedException, VeniceClientException {
    AbstractAvroStoreClient mockClient = getMockStoreClient(false);

    try (SchemaReader schemaReader = new RouterBackedSchemaReader(() -> mockClient)) {
      Schema schema = schemaReader.getKeySchema();
      Assert.assertEquals(schema.toString(), KEY_SCHEMA.toString());
      Schema cachedSchema = schemaReader.getKeySchema();
      Assert.assertEquals(cachedSchema, schema);
      // Must be the same Schema instance
      Assert.assertSame(schema, cachedSchema);
      Mockito.verify(mockClient, Mockito.timeout(TIMEOUT).times(1)).getRaw(Mockito.anyString());
    }
  }

  @Test(expectedExceptions = VeniceClientException.class)
  public void testGetKeySchemaWhenNotExists()
      throws IOException, ExecutionException, InterruptedException, VeniceClientException {
    String storeName = "test_store";
    AbstractAvroStoreClient mockClient = mock(AbstractAvroStoreClient.class);
    Mockito.doReturn(storeName).when(mockClient).getStoreName();
    CompletableFuture<byte[]> mockFuture = mock(CompletableFuture.class);
    Mockito.doReturn(null).when(mockFuture).get();
    Mockito.doReturn(mockFuture).when(mockClient).getRaw("key_schema/" + storeName);
    try (SchemaReader schemaReader = new RouterBackedSchemaReader(() -> mockClient)) {
      schemaReader.getKeySchema();
    }
  }

  @Test(expectedExceptions = VeniceClientException.class)
  public void testGetKeySchemaWhenServerError()
      throws ExecutionException, InterruptedException, VeniceClientException, IOException {
    String storeName = "test_store";
    AbstractAvroStoreClient mockClient = mock(AbstractAvroStoreClient.class);
    Mockito.doReturn(storeName).when(mockClient).getStoreName();
    CompletableFuture<byte[]> mockFuture = mock(CompletableFuture.class);
    Mockito.doThrow(new ExecutionException(new VeniceClientException("Server error"))).when(mockFuture).get();
    Mockito.doReturn(mockFuture).when(mockClient).getRaw("key_schema/" + storeName);
    try (SchemaReader schemaReader = new RouterBackedSchemaReader(() -> mockClient)) {
      schemaReader.getKeySchema();
    }
  }

  @Test
  public void testGetValueSchema() throws IOException, ExecutionException, InterruptedException, VeniceClientException {
    AbstractAvroStoreClient mockClient = getMockStoreClient(false);
    try (SchemaReader schemaReader = new RouterBackedSchemaReader(() -> mockClient)) {
      Schema schema = schemaReader.getValueSchema(1);
      Assert.assertEquals(schema.toString(), VALUE_SCHEMA_1.toString());
      Schema cachedSchema = schemaReader.getValueSchema(1);
      Mockito.verify(mockClient, Mockito.timeout(TIMEOUT).times(1)).getRaw(Mockito.anyString());
      Assert.assertEquals(cachedSchema, schema);
      // Must be the same Schema instance
      Assert.assertSame(schema, cachedSchema);
      Assert.assertEquals(schemaReader.getLatestValueSchema().toString(), VALUE_SCHEMA_2.toString());
      Mockito.verify(mockClient, Mockito.timeout(TIMEOUT).times(3)).getRaw(Mockito.anyString());
    }
  }

  @Test
  public void testGetValueSchemaWhenNotExists()
      throws IOException, ExecutionException, InterruptedException, VeniceClientException {
    AbstractAvroStoreClient mockClient = getMockStoreClient(false);

    try (SchemaReader schemaReader = new RouterBackedSchemaReader(() -> mockClient)) {
      Schema schema1 = schemaReader.getValueSchema(1);
      Assert.assertEquals(schema1.toString(), VALUE_SCHEMA_1.toString());
      Mockito.verify(mockClient, Mockito.timeout(TIMEOUT).times(1)).getRaw(Mockito.anyString());

      // If a missing schema is requested, we should not query it twice.
      Schema schema = schemaReader.getValueSchema(3);
      Assert.assertNull(schema);
      Mockito.verify(mockClient, Mockito.timeout(TIMEOUT).times(2)).getRaw(Mockito.anyString());
      Schema cachedSchema = schemaReader.getValueSchema(3);
      Assert.assertNull(cachedSchema);
      Mockito.verify(mockClient, Mockito.timeout(TIMEOUT).times(3)).getRaw(Mockito.anyString());

      Schema newSchema = schemaReader.getValueSchema(1);
      Assert.assertEquals(newSchema.toString(), VALUE_SCHEMA_1.toString());
      Mockito.verify(mockClient, Mockito.timeout(TIMEOUT).times(3)).getRaw(Mockito.anyString());

      Assert.assertEquals(schemaReader.getLatestValueSchema().toString(), VALUE_SCHEMA_2.toString());
      Mockito.verify(mockClient, Mockito.timeout(TIMEOUT).times(5)).getRaw(Mockito.anyString());
    }
  }

  @Test
  public void testGetValueSchemaWhenServerError()
      throws ExecutionException, InterruptedException, VeniceClientException, IOException {
    String storeName = "test_store";
    String keySchemaStr = "\"string\"";
    int valueSchemaId = 1;
    AbstractAvroStoreClient mockClient = mock(AbstractAvroStoreClient.class);
    Mockito.doReturn(storeName).when(mockClient).getStoreName();
    // setup key schema
    SchemaResponse schemaResponse = new SchemaResponse();
    schemaResponse.setId(1);
    schemaResponse.setSchemaStr(keySchemaStr);
    CompletableFuture<byte[]> mockFuture = mock(CompletableFuture.class);
    Mockito.doReturn(MAPPER.writeValueAsBytes(schemaResponse)).when(mockFuture).get();
    Mockito.doReturn(mockFuture).when(mockClient).getRaw("key_schema/" + storeName);
    Mockito.doThrow(new ExecutionException(new VeniceClientException("Server error"))).when(mockFuture).get();
    Mockito.doReturn(mockFuture).when(mockClient).getRaw("value_schema/" + storeName + "/" + valueSchemaId);
    // It should not throw error as we catch the exception.
    try (SchemaReader schemaReader = new RouterBackedSchemaReader(() -> mockClient)) {
      Assert.assertNull(schemaReader.getValueSchema(valueSchemaId));
    }
  }

  @Test
  public void testGetLatestValueSchema()
      throws IOException, ExecutionException, InterruptedException, VeniceClientException {
    AbstractAvroStoreClient mockClient = getMockStoreClient(false);

    try (SchemaReader schemaReader = new RouterBackedSchemaReader(() -> mockClient)) {
      Assert.assertEquals(schemaReader.getLatestValueSchema().toString(), VALUE_SCHEMA_2.toString());
      Mockito.verify(mockClient, Mockito.timeout(TIMEOUT).times(3)).getRaw(Mockito.anyString());
      Assert.assertEquals(schemaReader.getValueSchema(1).toString(), VALUE_SCHEMA_1.toString());
      Mockito.verify(mockClient, Mockito.timeout(TIMEOUT).times(3)).getRaw(Mockito.anyString());
      Assert.assertEquals(schemaReader.getValueSchema(2).toString(), VALUE_SCHEMA_2.toString());
      Mockito.verify(mockClient, Mockito.timeout(TIMEOUT).times(3)).getRaw(Mockito.anyString());
      schemaReader.getLatestValueSchema();
      Mockito.verify(mockClient, Mockito.timeout(TIMEOUT).times(3)).getRaw(Mockito.anyString());
    }
  }

  @Test
  public void testGetValueSchemaId()
      throws IOException, ExecutionException, InterruptedException, VeniceClientException {
    AbstractAvroStoreClient mockClient = getMockStoreClient(false);

    final Schema sameCanonicalSchemaAsValueSchema2 =
        AvroCompatibilityHelper.parse(loadFileAsStringQuietlyWithErrorLogged("SameCanonicalAsRecordValueSchema2.avsc"));

    final Schema invalidValueSchema =
        AvroCompatibilityHelper.parse(loadFileAsStringQuietlyWithErrorLogged("testSchemaWithNamespace.avsc"));

    try (SchemaReader schemaReader = new RouterBackedSchemaReader(() -> mockClient)) {
      Assert.assertEquals(schemaReader.getValueSchemaId(VALUE_SCHEMA_1), 1);
      Assert.assertEquals(schemaReader.getValueSchemaId(VALUE_SCHEMA_2), 2);
      Assert.assertEquals(schemaReader.getValueSchemaId(sameCanonicalSchemaAsValueSchema2), 2);
      VeniceClientException e =
          Assert.expectThrows(VeniceClientException.class, () -> schemaReader.getValueSchemaId(invalidValueSchema));
      Assert.assertTrue(e.getMessage().contains("Could not find schema"));
    }
  }

  @Test
  public void testGetUpdateSchema()
      throws IOException, ExecutionException, InterruptedException, VeniceClientException {
    AbstractAvroStoreClient storeClient = getMockStoreClient(true);

    try (SchemaReader schemaReader =
        new RouterBackedSchemaReader(() -> storeClient, Optional.empty(), Optional.empty())) {
      Assert.assertEquals(schemaReader.getUpdateSchema(1), UPDATE_SCHEMA_1);
      Assert.assertEquals(schemaReader.getUpdateSchema(2), UPDATE_SCHEMA_2);
      Assert.assertNull(schemaReader.getUpdateSchema(3));
      Assert.assertNull(schemaReader.getUpdateSchema(4));
    }
  }

  @Test
  public void testRefreshValueAndUpdateSchemas() throws IOException, ExecutionException, InterruptedException {
    AbstractAvroStoreClient storeClient = getMockStoreClient(true);

    try (SchemaReader schemaReader =
        new RouterBackedSchemaReader(() -> storeClient, Optional.empty(), Optional.empty(), Duration.ofSeconds(1))) {
      Assert.assertEquals(schemaReader.getValueSchema(1), VALUE_SCHEMA_1);
      Assert.assertEquals(schemaReader.getValueSchema(2), VALUE_SCHEMA_2);
      Assert.assertNull(schemaReader.getValueSchema(3));
      Assert.assertNull(schemaReader.getValueSchema(4));
      Assert.assertEquals(schemaReader.getUpdateSchema(1), UPDATE_SCHEMA_1);
      Assert.assertEquals(schemaReader.getUpdateSchema(2), UPDATE_SCHEMA_2);
      Assert.assertNull(schemaReader.getUpdateSchema(3));
      Assert.assertNull(schemaReader.getUpdateSchema(4));
      Assert.assertEquals(schemaReader.getLatestValueSchema(), VALUE_SCHEMA_2);
      Assert.assertEquals(schemaReader.getLatestUpdateSchema().getSchema(), UPDATE_SCHEMA_2);
      Assert.assertEquals(schemaReader.getLatestUpdateSchema().getValueSchemaID(), 2);

      // Register 2 new value schemas with one of them being a new superset schema
      configureSchemaResponseMocks(
          storeClient,
          Arrays.asList(VALUE_SCHEMA_1, VALUE_SCHEMA_2, VALUE_SCHEMA_3, VALUE_SCHEMA_4),
          3,
          Arrays.asList(UPDATE_SCHEMA_1, UPDATE_SCHEMA_2, UPDATE_SCHEMA_3, UPDATE_SCHEMA_4),
          true,
          0);
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        Assert.assertEquals(schemaReader.getValueSchema(1), VALUE_SCHEMA_1);
        Assert.assertEquals(schemaReader.getValueSchema(2), VALUE_SCHEMA_2);
        Assert.assertEquals(schemaReader.getValueSchema(3), VALUE_SCHEMA_3);
        Assert.assertEquals(schemaReader.getValueSchema(4), VALUE_SCHEMA_4);
        Assert.assertEquals(schemaReader.getUpdateSchema(1), UPDATE_SCHEMA_1);
        Assert.assertEquals(schemaReader.getUpdateSchema(2), UPDATE_SCHEMA_2);
        Assert.assertEquals(schemaReader.getUpdateSchema(3), UPDATE_SCHEMA_3);
        Assert.assertEquals(schemaReader.getUpdateSchema(4), UPDATE_SCHEMA_4);
        Assert.assertEquals(schemaReader.getLatestValueSchema(), VALUE_SCHEMA_3);
        Assert.assertEquals(schemaReader.getLatestUpdateSchema().getSchema(), UPDATE_SCHEMA_3);
        Assert.assertEquals(schemaReader.getLatestUpdateSchema().getValueSchemaID(), 3);
      });
    }
  }

  @Test
  public void testConcurrentRefreshValueAndUpdateSchemas()
      throws IOException, ExecutionException, InterruptedException {
    AbstractAvroStoreClient storeClient = getMockStoreClient(true, 500);

    try (SchemaReader schemaReader =
        new RouterBackedSchemaReader(() -> storeClient, Optional.empty(), Optional.empty(), Duration.ofMinutes(2))) {
      // This will create 2 threads that will try to refresh the schemas concurrently. When querying Venice backend for
      // schemas, one thread will sleep for 500 ms. In that time, we expect the other thread to also start waiting to
      // acquire the lock.
      CompletableFuture<DerivedSchemaEntry> future1 =
          CompletableFuture.supplyAsync(() -> schemaReader.getLatestUpdateSchema());
      CompletableFuture<DerivedSchemaEntry> future2 =
          CompletableFuture.supplyAsync(() -> schemaReader.getLatestUpdateSchema());
      Assert.assertNotNull(future1.get());
      Assert.assertNotNull(future2.get());

      /**
       * 'getRaw' Should be called 4 times:
       * 1. Fetch value schemas on start up, which takes 2 + 1 = 3 individual call.
       * 2. Fetch update schemas in one of the futures
       */
      Mockito.verify(storeClient, Mockito.timeout(TIMEOUT).times(5)).getRaw(Mockito.anyString());
    }
  }

  @Test
  public void testGetLatestValueSchemaWithSupersetSchema() throws Exception {
    AbstractAvroStoreClient mockClient = getMockStoreClient(false);
    configureSchemaResponseMocks(
        mockClient,
        Arrays.asList(VALUE_SCHEMA_1, VALUE_SCHEMA_2, VALUE_SCHEMA_3),
        1,
        Collections.emptyList(),
        false,
        0);

    try (SchemaReader schemaReader = new RouterBackedSchemaReader(
        () -> mockClient,
        Optional.empty(),
        Optional.of(schema -> schema.toString().equals(VALUE_SCHEMA_2.toString())),
        mockICProvider)) {
      Assert.assertEquals(schemaReader.getValueSchema(1).toString(), VALUE_SCHEMA_1.toString());
      Assert.assertEquals(schemaReader.getValueSchema(2).toString(), VALUE_SCHEMA_2.toString());
      // If a preferredSchemaFilter is specified, the latest schema must be a preferred schema; even if it is not the
      // superset schema or the one with the max id
      Assert.assertEquals(schemaReader.getLatestValueSchema().toString(), VALUE_SCHEMA_2.toString());
      schemaReader.getLatestValueSchema();
      Mockito.verify(mockClient, Mockito.timeout(TIMEOUT).times(4)).getRaw(Mockito.anyString());
    }

    try (SchemaReader schemaReader =
        new RouterBackedSchemaReader(() -> mockClient, Optional.empty(), Optional.of(schema -> true), mockICProvider)) {
      Assert.assertEquals(schemaReader.getValueSchema(1).toString(), VALUE_SCHEMA_1.toString());
      Assert.assertEquals(schemaReader.getValueSchema(2).toString(), VALUE_SCHEMA_2.toString());
      // If a preferredSchemaFilter is specified, and the superset schema is a preferred schema, the latest schema must
      // be the superset schema
      Assert.assertEquals(schemaReader.getLatestValueSchema().toString(), VALUE_SCHEMA_1.toString());
    }

    try (SchemaReader schemaReader =
        new RouterBackedSchemaReader(() -> mockClient, Optional.empty(), Optional.empty(), mockICProvider)) {
      Assert.assertEquals(schemaReader.getValueSchema(1).toString(), VALUE_SCHEMA_1.toString());
      Assert.assertEquals(schemaReader.getValueSchema(2).toString(), VALUE_SCHEMA_2.toString());
      // If a preferredSchemaFilter is not specified, the latest schema must be the superset schema
      Assert.assertEquals(schemaReader.getLatestValueSchema().toString(), VALUE_SCHEMA_1.toString());
    }
  }

  @Test
  public void testGetSchemasOnStartup()
      throws IOException, ExecutionException, InterruptedException, VeniceClientException {
    AbstractAvroStoreClient mockClient = getMockStoreClient(false);

    try (SchemaReader schemaReader =
        new RouterBackedSchemaReader(() -> mockClient, Optional.empty(), Optional.empty(), Duration.ofMinutes(2))) {
      // Schemas should be fetched on startup
      Mockito.verify(mockClient, Mockito.timeout(TIMEOUT).times(3)).getRaw(Mockito.anyString());
      Assert.assertNotNull(schemaReader.getLatestValueSchema());
      // Should not be checked again
      Mockito.verify(mockClient, Mockito.timeout(TIMEOUT).times(4)).getRaw(Mockito.anyString());
    }
  }

  @Test
  public void testGetLatestValueSchemaWhenNoValueSchema()
      throws IOException, ExecutionException, InterruptedException, VeniceClientException {
    AbstractAvroStoreClient mockClient = getMockStoreClient(false);
    configureSchemaResponseMocks(
        mockClient,
        Collections.emptyList(),
        SchemaData.INVALID_VALUE_SCHEMA_ID,
        Collections.emptyList(),
        false,
        0);

    try (SchemaReader schemaReader = new RouterBackedSchemaReader(() -> mockClient)) {
      Assert.assertThrows(VeniceClientException.class, () -> schemaReader.getLatestValueSchema());
      Mockito.verify(mockClient, Mockito.timeout(TIMEOUT).times(1)).getRaw(Mockito.anyString());
    }
  }

  @Test(expectedExceptions = VeniceClientException.class)
  public void testGetLatestValueSchemaWhenServerError()
      throws ExecutionException, InterruptedException, VeniceClientException, IOException {
    String storeName = "test_store";
    String keySchemaStr = "\"string\"";
    // setup key schema
    SchemaResponse schemaResponse = new SchemaResponse();
    schemaResponse.setId(1);
    schemaResponse.setSchemaStr(keySchemaStr);
    AbstractAvroStoreClient mockClient = mock(AbstractAvroStoreClient.class);
    Mockito.doReturn(storeName).when(mockClient).getStoreName();
    CompletableFuture<byte[]> mockFuture = mock(CompletableFuture.class);
    Mockito.doReturn(MAPPER.writeValueAsBytes(schemaResponse)).when(mockFuture).get();
    Mockito.doReturn(mockFuture).when(mockClient).getRaw("key_schema/" + storeName);
    Mockito.doThrow(new ExecutionException(new VeniceClientException("Server error"))).when(mockFuture).get();
    Mockito.doReturn(mockFuture).when(mockClient).getRaw("value_schema/" + storeName);
    try (SchemaReader schemaReader = new RouterBackedSchemaReader(() -> mockClient)) {
      schemaReader.getLatestValueSchema();
    }
  }

  @Test
  public void testGetSchemaWithAnExtraFieldInResponse() throws Exception {
    String storeName = "test_store";
    String keySchemaStr = "\"string\"";
    // Create a response with an extra field.
    SchemaResponseWithExtraField schemaResponse = new SchemaResponseWithExtraField();
    schemaResponse.setId(1);
    schemaResponse.setSchemaStr(keySchemaStr);
    schemaResponse.setExtraField(100);

    AbstractAvroStoreClient mockClient = mock(AbstractAvroStoreClient.class);
    Mockito.doReturn(storeName).when(mockClient).getStoreName();
    CompletableFuture<byte[]> mockFuture = mock(CompletableFuture.class);
    Mockito.doReturn(MAPPER.writeValueAsBytes(schemaResponse)).when(mockFuture).get();
    Mockito.doReturn(mockFuture).when(mockClient).getRaw(Mockito.anyString());
    try (SchemaReader schemaReader = new RouterBackedSchemaReader(() -> mockClient)) {
    } catch (VeniceClientException e) {
      Assert.fail("The unrecognized field should be ignored.");
    }
  }

  @Test(enabled = false)
  public void testGetMultiSchemaBackwardCompat() throws Exception {
    String storeName = "test_store";
    String valueSchemaStr = "\"string\"";
    // Create a repsonse with an extra field.
    MultiSchemaResponse multiSchemaResponse = new MultiSchemaResponse();
    MultiSchemaResponse.Schema[] schemas = new MultiSchemaResponse.Schema[1];
    MultiSchemaResponse.Schema schema1 = new MultiSchemaResponse.Schema();
    schema1.setId(1);
    schema1.setSchemaStr(valueSchemaStr);
    schemas[0] = schema1;
    multiSchemaResponse.setSchemas(schemas);
    multiSchemaResponse.setSuperSetSchemaId(10);

    AbstractAvroStoreClient mockClient = mock(AbstractAvroStoreClient.class);
    Mockito.doReturn(storeName).when(mockClient).getStoreName();
    CompletableFuture<byte[]> mockFuture = mock(CompletableFuture.class);
    Mockito.doReturn(MAPPER.writeValueAsBytes(multiSchemaResponse)).when(mockFuture).get();
    Mockito.doReturn(mockFuture).when(mockClient).getRaw(Mockito.anyString());
    try {
      MultiSchemaResponseWithExtraField schemaResponse =
          MAPPER.readValue(MAPPER.writeValueAsBytes(multiSchemaResponse), MultiSchemaResponseWithExtraField.class);
      schemaResponse.getSuperSetSchemaId();
    } catch (VeniceClientException e) {
      Assert.fail("The unrecognized field should be ignored.");
    }
  }

  private static class SchemaResponseWithExtraField extends SchemaResponse {
    private int extraField;

    public int getExtraField() {
      return extraField;
    }

    public void setExtraField(int extraField) {
      this.extraField = extraField;
    }
  }

  private class MultiSchemaResponseWithExtraField extends MultiSchemaResponse {
    private int extraField;

    public int getExtraField() {
      return extraField;
    }

    public void setExtraField(int extraField) {
      this.extraField = extraField;
    }
  }

  private AbstractAvroStoreClient getMockStoreClient(boolean updateEnabled)
      throws IOException, ExecutionException, InterruptedException {
    return getMockStoreClient(updateEnabled, 0);
  }

  private AbstractAvroStoreClient getMockStoreClient(boolean updateEnabled, int delayInResponseMs)
      throws IOException, ExecutionException, InterruptedException {
    int partitionCount = 10;
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    Version version = new VersionImpl(storeName, 1, "test-job-id");
    version.setPartitionCount(partitionCount);

    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(
        1000,
        1000,
        -1,
        DataReplicationPolicy.NON_AGGREGATE,
        BufferReplayPolicy.REWIND_FROM_EOP);

    ZKStore store = new ZKStore(
        storeName,
        "test-owner",
        System.currentTimeMillis(),
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1,
        1000,
        1000,
        hybridStoreConfig,
        partitionerConfig,
        3);
    store.setPartitionCount(partitionCount);
    store.setVersions(Collections.singletonList(version));
    store.setWriteComputationEnabled(updateEnabled);

    AbstractAvroStoreClient storeClient = mock(AbstractAvroStoreClient.class);
    Mockito.doReturn(storeName).when(storeClient).getStoreName();

    VersionCreationResponse versionCreationResponse = new VersionCreationResponse();
    versionCreationResponse.setPartitions(partitionCount);
    versionCreationResponse.setPartitionerClass(partitionerConfig.getPartitionerClass());
    versionCreationResponse.setPartitionerParams(partitionerConfig.getPartitionerParams());
    versionCreationResponse.setKafkaBootstrapServers("localhost:9092");
    versionCreationResponse.setKafkaTopic(Utils.getRealTimeTopicName(store));
    versionCreationResponse.setEnableSSL(false);

    CompletableFuture<byte[]> requestTopicFuture = mock(CompletableFuture.class);
    Mockito.doReturn(MAPPER.writeValueAsBytes(versionCreationResponse)).when(requestTopicFuture).get();
    Mockito.doReturn(requestTopicFuture).when(storeClient).getRaw("request_topic/" + storeName);

    CompletableFuture<byte[]> storeStateFuture = mock(CompletableFuture.class);
    Mockito.doReturn(STORE_SERIALIZER.serialize(store, null)).when(storeStateFuture).get();
    Mockito.doReturn(storeStateFuture).when(storeClient).getRaw("store_state/" + storeName);

    configureSchemaResponseMocks(
        storeClient,
        Arrays.asList(VALUE_SCHEMA_1, VALUE_SCHEMA_2),
        2,
        Arrays.asList(UPDATE_SCHEMA_1, UPDATE_SCHEMA_2),
        updateEnabled,
        delayInResponseMs);

    return storeClient;
  }

  private void configureSchemaResponseMocks(
      AbstractAvroStoreClient storeClient,
      List<Schema> valueSchemas,
      int supersetSchemaId,
      List<Schema> updateSchemas,
      boolean updateEnabled,
      int delayInResponseMs) {
    MultiSchemaIdResponse multiSchemaIdResponse = new MultiSchemaIdResponse();
    if (supersetSchemaId > 0) {
      multiSchemaIdResponse.setSuperSetSchemaId(supersetSchemaId);
    }
    Set<Integer> schemaIdSet = new HashSet<>();
    for (int i = 1; i <= valueSchemas.size(); i++) {
      schemaIdSet.add(i);
    }
    multiSchemaIdResponse.setSchemaIdSet(schemaIdSet);
    doAnswer(invocation -> getResponseWithDelay(MAPPER.writeValueAsBytes(multiSchemaIdResponse), delayInResponseMs))
        .when(storeClient)
        .getRaw(eq("all_value_schema_ids/" + storeName));

    String keySchemaStr = KEY_SCHEMA.toString();
    SchemaResponse keySchemaResponse = new SchemaResponse();
    keySchemaResponse.setId(1);
    keySchemaResponse.setSchemaStr(keySchemaStr);

    doAnswer(invocation -> getResponseWithDelay(MAPPER.writeValueAsBytes(keySchemaResponse), delayInResponseMs))
        .when(storeClient)
        .getRaw(eq("key_schema/" + storeName));

    MultiSchemaResponse.Schema[] valueSchemaArr = new MultiSchemaResponse.Schema[valueSchemas.size()];
    for (int i = 0; i < valueSchemas.size(); i++) {
      MultiSchemaResponse.Schema valueSchema = new MultiSchemaResponse.Schema();
      valueSchema.setId(i + 1);
      valueSchema.setSchemaStr(valueSchemas.get(i).toString());

      valueSchemaArr[i] = valueSchema;
    }

    MultiSchemaResponse multiSchemaResponse = new MultiSchemaResponse();
    multiSchemaResponse.setSchemas(valueSchemaArr);
    multiSchemaResponse.setCluster(clusterName);
    if (supersetSchemaId > 0) {
      multiSchemaResponse.setSuperSetSchemaId(supersetSchemaId);
    }

    doAnswer(invocation -> getResponseWithDelay(MAPPER.writeValueAsBytes(multiSchemaResponse), delayInResponseMs))
        .when(storeClient)
        .getRaw(eq("value_schema/" + storeName));

    for (int i = 0; i < valueSchemas.size(); i++) {
      SchemaResponse valueSchemaResponse = new SchemaResponse();
      valueSchemaResponse.setId(i + 1);
      valueSchemaResponse.setSchemaStr(valueSchemas.get(i).toString());
      doAnswer(invocation -> getResponseWithDelay(MAPPER.writeValueAsBytes(valueSchemaResponse), delayInResponseMs))
          .when(storeClient)
          .getRaw(eq("value_schema/" + storeName + "/" + (i + 1)));
    }

    if (updateEnabled) {
      MultiSchemaResponse allUpdateSchemaResponse = new MultiSchemaResponse();
      allUpdateSchemaResponse.setCluster(clusterName);
      allUpdateSchemaResponse.setName(storeName);

      MultiSchemaResponse.Schema[] multiSchemas = new MultiSchemaResponse.Schema[updateSchemas.size()];
      for (int i = 0; i < updateSchemas.size(); i++) {
        SchemaResponse updateSchemaResponse = new SchemaResponse();
        updateSchemaResponse.setCluster(clusterName);
        updateSchemaResponse.setName(storeName);
        updateSchemaResponse.setId(i + 1);
        updateSchemaResponse.setDerivedSchemaId(1);
        updateSchemaResponse.setSchemaStr(updateSchemas.get(i).toString());

        doAnswer(invocation -> getResponseWithDelay(MAPPER.writeValueAsBytes(updateSchemaResponse), delayInResponseMs))
            .when(storeClient)
            .getRaw(eq("update_schema/" + storeName + "/" + (i + 1)));

        MultiSchemaResponse.Schema schema = new MultiSchemaResponse.Schema();
        schema.setId(i + 1);
        schema.setDerivedSchemaId(1);
        schema.setSchemaStr(updateSchemas.get(i).toString());
        multiSchemas[i] = schema;
      }
      allUpdateSchemaResponse.setSchemas(multiSchemas);

      doAnswer(invocation -> getResponseWithDelay(MAPPER.writeValueAsBytes(allUpdateSchemaResponse), delayInResponseMs))
          .when(storeClient)
          .getRaw(eq("update_schema/" + storeName));
    } else {
      for (int i = 0; i < updateSchemas.size(); i++) {
        SchemaResponse noUpdateSchemaResponse = new SchemaResponse();
        noUpdateSchemaResponse
            .setError("Update schema doesn't exist for value schema id: " + (i + 1) + " of store: " + storeName);

        doAnswer(
            invocation -> getResponseWithDelay(MAPPER.writeValueAsBytes(noUpdateSchemaResponse), delayInResponseMs))
                .when(storeClient)
                .getRaw(eq("update_schema/" + storeName + "/" + (i + 1)));
      }

      MultiSchemaResponse allUpdateSchemaResponse = new MultiSchemaResponse();
      allUpdateSchemaResponse.setCluster(clusterName);
      allUpdateSchemaResponse.setName(storeName);

      MultiSchemaResponse.Schema[] multiSchemas = new MultiSchemaResponse.Schema[0];
      allUpdateSchemaResponse.setSchemas(multiSchemas);

      doAnswer(invocation -> getResponseWithDelay(MAPPER.writeValueAsBytes(allUpdateSchemaResponse), delayInResponseMs))
          .when(storeClient)
          .getRaw(eq("update_schema/" + storeName));
    }
  }

  private CompletableFuture<byte[]> getResponseWithDelay(byte[] body, long delayInResponseMs) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        if (delayInResponseMs > 0) {
          Utils.sleep(delayInResponseMs);
        }
        return body;
      } catch (Throwable t) {
        return null;
      }
    });
  }
}
