package com.linkedin.venice.client.store;

import static com.linkedin.venice.VeniceConstants.VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME;
import static com.linkedin.venice.client.schema.RouterBackedSchemaReader.TYPE_ALL_VALUE_SCHEMA_IDS;
import static com.linkedin.venice.client.schema.RouterBackedSchemaReader.TYPE_VALUE_SCHEMA;
import static com.linkedin.venice.client.schema.RouterBasedStoreSchemaFetcher.TYPE_KEY_SCHEMA;
import static com.linkedin.venice.client.stats.BasicClientStats.CLIENT_METRIC_ENTITIES;
import static com.linkedin.venice.client.store.AbstractAvroStoreClient.TYPE_STORAGE;
import static com.linkedin.venice.stats.ClientType.THIN_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.client.store.transport.TransportClientStreamingCallback;
import com.linkedin.venice.client.utils.StoreClientTestUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compute.protocol.response.ComputeResponseRecordV1;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.stats.ClientType;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.TestUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AbstractAvroStoreClientTest {
  private static class SimpleStoreClient<K, V> extends AbstractAvroStoreClient<K, V> {
    private final TransportClient transportClient;
    private final String storeName;
    private final boolean overrideGetSchemaReader;

    public SimpleStoreClient(
        TransportClient transportClient,
        String storeName,
        boolean needSchemaReader,
        Executor deserializationExecutor) {
      this(transportClient, storeName, needSchemaReader, deserializationExecutor, true);
    }

    public SimpleStoreClient(
        TransportClient transportClient,
        String storeName,
        boolean needSchemaReader,
        Executor deserializationExecutor,
        boolean overrideGetSchemaReader) {
      this(
          transportClient,
          needSchemaReader,
          overrideGetSchemaReader,
          ClientConfig.defaultGenericClientConfig(storeName).setDeserializationExecutor(deserializationExecutor));
    }

    public SimpleStoreClient(
        TransportClient transportClient,
        boolean needSchemaReader,
        boolean overrideGetSchemaReader,
        ClientConfig clientConfig) {
      super(transportClient, needSchemaReader, clientConfig);
      this.transportClient = transportClient;
      this.storeName = clientConfig.getStoreName();
      this.overrideGetSchemaReader = overrideGetSchemaReader;
    }

    @Override
    protected AbstractAvroStoreClient<K, V> getStoreClientForSchemaReader() {
      return new SimpleStoreClient<>(
          transportClient,
          storeName,
          false,
          AbstractAvroStoreClient.getDefaultDeserializationExecutor());
    }

    @Override
    public RecordDeserializer<V> getDataRecordDeserializer(int schemaId) throws VeniceClientException {
      if (schemaId != 1) {
        throw new VeniceUnsupportedOperationException("schemaId: " + schemaId);
      }
      return FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(VALUE_SCHEMA, VALUE_SCHEMA);
    }

    @Override
    public SchemaReader getSchemaReader() {
      if (overrideGetSchemaReader) {
        SchemaReader mockSchemaReader = mock(SchemaReader.class);
        doReturn(Schema.create(Schema.Type.STRING)).when(mockSchemaReader).getKeySchema();
        doReturn(1).when(mockSchemaReader).getLatestValueSchemaId();
        doReturn(VALUE_SCHEMA).when(mockSchemaReader).getValueSchema(1);
        return mockSchemaReader;
      } else {
        return super.getSchemaReader();
      }
    }
  }

  private static final Schema VALUE_SCHEMA = Schema.parse(
      "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"record_schema\",\n" + "  \"fields\": [\n" + "    {\n"
          + "      \"name\": \"int_field\",\n" + "      \"type\": \"int\",\n" + "      \"default\": 0,\n"
          + "      \"doc\": \"doc for int_field\"\n" + "    },\n"
          + "    { \"name\": \"float_field\", \"type\": \"float\", \"doc\": \"doc for float_field\" },\n" + "    {\n"
          + "      \"name\": \"record_field\",\n" + "      \"namespace\": \"com.linkedin.test\",\n"
          + "      \"type\": {\n" + "        \"name\": \"Record1\",\n" + "        \"type\": \"record\",\n"
          + "        \"fields\": [\n" + "          {\n" + "            \"name\": \"nested_field1\",\n"
          + "            \"type\": \"double\",\n" + "            \"doc\": \"doc for nested field\"\n" + "          }\n"
          + "        ]\n" + "      }\n" + "    },\n" + "    {\n" + "      \"name\": \"float_array_field1\",\n"
          + "      \"type\": { \"type\": \"array\", \"items\": \"float\" }\n" + "    },\n" + "    {\n"
          + "      \"name\": \"float_array_field2\",\n"
          + "      \"type\": { \"type\": \"array\", \"items\": \"float\" }\n" + "    },\n"
          + "    { \"name\": \"int_array_field2\", \"type\": { \"type\": \"array\", \"items\": \"int\" } }\n" + "  ]\n"
          + "}\n");

  private static final RecordSerializer<GenericRecord> valueSerializer =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(VALUE_SCHEMA);

  private static final Set<String> keys = new HashSet<>();
  static {
    keys.add("key1");
    keys.add("key2");
  }

  private static final List<Float> dotProductParam = Arrays.asList(0.1f, 0.2f);
  private static final List<Float> cosineSimilarityParam = Arrays.asList(0.3f, 0.4f);
  private static final List<Float> hadamardProductParam = Arrays.asList(0.5f, 0.6f);

  private static class ParameterizedComputeTransportClient extends TransportClient {
    private final Map<String, String> headerMap;
    private final Optional<byte[]> responseBody;
    private final Optional<VeniceClientException> completedException;

    public ParameterizedComputeTransportClient(
        Optional<byte[]> responseBody,
        Optional<VeniceClientException> completedException) {
      this.responseBody = responseBody;
      this.completedException = completedException;
      this.headerMap = new HashMap<>();
      this.headerMap.put(
          HttpConstants.VENICE_SCHEMA_ID,
          Integer.toString(ReadAvroProtocolDefinition.COMPUTE_RESPONSE_V1.getProtocolVersion()));
    }

    @Override
    public CompletableFuture<TransportClientResponse> get(String requestPath, Map<String, String> headers) {
      return null;
    }

    @Override
    public CompletableFuture<TransportClientResponse> post(
        String requestPath,
        Map<String, String> headers,
        byte[] requestBody) {
      return null;
    }

    @Override
    public void streamPost(
        String requestPath,
        Map<String, String> headers,
        byte[] requestBody,
        TransportClientStreamingCallback callback,
        int keyCount) {
      Map<String, String> headerMap = new HashMap<>();
      headerMap.put(
          HttpConstants.VENICE_SCHEMA_ID,
          Integer.toString(ReadAvroProtocolDefinition.COMPUTE_RESPONSE_V1.getProtocolVersion()));
      callback.onHeaderReceived(headerMap);
      responseBody.ifPresent(body -> callback.onDataReceived(ByteBuffer.wrap(body)));
      callback.onCompletion(completedException);
    }

    @Override
    public void close() throws IOException {
    }
  };

  @Test
  public void testCompute() throws ExecutionException, InterruptedException {
    // Mock a transport client response
    String resultSchemaStr = "{" + "  \"type\": \"record\",        "
        + "  \"name\": \"test_store_VeniceComputeResult\",       " + "  \"doc\": \"\",                          "
        + "  \"fields\": [        "
        + "         { \"name\": \"int_field\", \"type\": \"int\", \"doc\": \"\", \"default\": 0 },             "
        + "         { \"name\": \"dot_product_for_float_array_field1\", \"type\": [\"null\",\"float\"], \"doc\": \"\", \"default\": null },           "
        + "         { \"name\": \"cosine_similarity_for_float_array_field2\", \"type\": [\"null\",\"float\"], \"doc\": \"\", \"default\": null },           "
        + "         { \"name\": \"hadamard_product_for_float_array_field1\", \"type\":[\"null\",{\"type\":\"array\",\"items\":\"float\"}],\"doc\":\"\",\"default\":null },           "
        + "         { \"name\": \"__veniceComputationError__\", \"type\": { \"type\": \"map\", \"values\": \"string\" }, \"doc\": \"\", \"default\": { } }        "
        + "  ]       " + " }       ";
    Schema resultSchema = Schema.parse(resultSchemaStr);
    RecordSerializer<GenericRecord> resultSerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(resultSchema);

    List<Float> hadamardProductResult = Arrays.asList(3.1f, 4.1f);
    List<ComputeResponseRecordV1> responseRecordV1List = new ArrayList<>();
    GenericRecord result1 = new GenericData.Record(resultSchema);
    result1.put("int_field", 1);
    result1.put("dot_product_for_float_array_field1", 1.1f);
    result1.put("cosine_similarity_for_float_array_field2", 2.1f);
    result1.put("hadamard_product_for_float_array_field1", hadamardProductResult);
    result1.put(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME, Collections.emptyMap());
    ComputeResponseRecordV1 record1 = new ComputeResponseRecordV1();
    record1.keyIndex = 0;
    record1.value = ByteBuffer.wrap(resultSerializer.serialize(result1));

    List<Float> hadamardProductResult2 = Arrays.asList(3.2f, 4.2f);
    GenericRecord result2 = new GenericData.Record(resultSchema);
    result2.put("int_field", 2);
    result2.put("dot_product_for_float_array_field1", 1.2f);
    result2.put("cosine_similarity_for_float_array_field2", 2.2f);
    result2.put("hadamard_product_for_float_array_field1", hadamardProductResult2);
    result2.put(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME, Collections.emptyMap());
    ComputeResponseRecordV1 record2 = new ComputeResponseRecordV1();
    record2.keyIndex = 1;
    record2.value = ByteBuffer.wrap(resultSerializer.serialize(result2));
    responseRecordV1List.add(record1);
    responseRecordV1List.add(record2);

    RecordSerializer<ComputeResponseRecordV1> computeResponseSerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(ComputeResponseRecordV1.SCHEMA$);
    byte[] serializedResponse = computeResponseSerializer.serializeObjects(responseRecordV1List);

    TransportClient mockTransportClient =
        new ParameterizedComputeTransportClient(Optional.of(serializedResponse), Optional.empty());

    String storeName = "test_store";
    SimpleStoreClient<String, GenericRecord> storeClient = new SimpleStoreClient<>(
        mockTransportClient,
        storeName,
        true,
        AbstractAvroStoreClient.getDefaultDeserializationExecutor());
    VeniceMetricsRepository metricsRepository = getVeniceMetricsRepository(THIN_CLIENT, CLIENT_METRIC_ENTITIES, true);
    ClientStats stats =
        ClientStats.getClientStats(metricsRepository, storeName, RequestType.COMPUTE, null, ClientType.THIN_CLIENT);
    ClientStats streamingStats = ClientStats
        .getClientStats(metricsRepository, storeName, RequestType.COMPUTE_STREAMING, null, ClientType.THIN_CLIENT);
    CompletableFuture<Map<String, ComputeGenericRecord>> computeFuture =
        storeClient.compute(Optional.of(stats), Optional.of(streamingStats), 0)
            .project("int_field")
            .dotProduct("float_array_field1", dotProductParam, "dot_product_for_float_array_field1")
            .cosineSimilarity("float_array_field2", cosineSimilarityParam, "cosine_similarity_for_float_array_field2")
            .hadamardProduct("float_array_field1", hadamardProductParam, "hadamard_product_for_float_array_field1")
            .execute(keys);
    Map<String, ComputeGenericRecord> computeResult = computeFuture.get();
    Assert.assertEquals(computeResult.size(), 2);
    Assert.assertNotNull(computeResult.get("key1"));
    ComputeGenericRecord resultForKey1 = computeResult.get("key1");
    Assert.assertEquals(resultForKey1.getValueSchema(), VALUE_SCHEMA);
    Assert.assertEquals(resultForKey1.get("int_field"), 1);
    Assert.assertEquals(resultForKey1.get("dot_product_for_float_array_field1"), 1.1f);
    Assert.assertEquals(resultForKey1.get("cosine_similarity_for_float_array_field2"), 2.1f);
    Assert.assertEquals(resultForKey1.get("hadamard_product_for_float_array_field1"), hadamardProductResult);
    Assert.assertNotNull(computeResult.get("key2"));
    ComputeGenericRecord resultForKey2 = computeResult.get("key2");
    Assert.assertEquals(resultForKey2.getValueSchema(), VALUE_SCHEMA);
    Assert.assertEquals(resultForKey2.get("int_field"), 2);
    Assert.assertEquals(resultForKey2.get("dot_product_for_float_array_field1"), 1.2f);
    Assert.assertEquals(resultForKey2.get("cosine_similarity_for_float_array_field2"), 2.2f);
    Assert.assertEquals(resultForKey2.get("hadamard_product_for_float_array_field1"), hadamardProductResult2);
  }

  @Test
  public void testComputeFailure() throws ExecutionException, InterruptedException {
    // Mock a transport client response
    String resultSchemaStr = "{" + "  \"type\": \"record\",        "
        + "  \"name\": \"test_store_VeniceComputeResult\",       " + "  \"doc\": \"\",                          "
        + "  \"fields\": [        "
        + "         { \"name\": \"int_field\", \"type\": \"int\", \"doc\": \"\", \"default\": 0 },             "
        + "         { \"name\": \"dot_product_for_float_array_field1\", \"type\": [\"null\",\"float\"], \"doc\": \"\", \"default\": null },           "
        + "         { \"name\": \"cosine_similarity_for_float_array_field2\", \"type\": [\"null\",\"float\"], \"doc\": \"\", \"default\": null },           "
        + "         { \"name\": \"__veniceComputationError__\", \"type\": { \"type\": \"map\", \"values\": \"string\" }, \"doc\": \"\", \"default\": { } }        "
        + "  ]       " + " }       ";
    Schema resultSchema = Schema.parse(resultSchemaStr);
    RecordSerializer<GenericRecord> resultSerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(resultSchema);
    List<ComputeResponseRecordV1> responseRecordV1List = new ArrayList<>();
    GenericRecord result1 = new GenericData.Record(resultSchema);
    result1.put("int_field", 1);
    result1.put("dot_product_for_float_array_field1", 0f);
    result1.put("cosine_similarity_for_float_array_field2", 0f);
    Map<String, String> computationErrorMap = new HashMap<>();
    computationErrorMap.put("dot_product_for_float_array_field1", "array length are different");
    computationErrorMap.put("cosine_similarity_for_float_array_field2", "NullPointerException");
    result1.put(VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME, computationErrorMap);
    ComputeResponseRecordV1 record1 = new ComputeResponseRecordV1();
    record1.keyIndex = 0;
    record1.value = ByteBuffer.wrap(resultSerializer.serialize(result1));
    responseRecordV1List.add(record1);

    RecordSerializer<ComputeResponseRecordV1> computeResponseSerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(ComputeResponseRecordV1.SCHEMA$);

    byte[] serializedResponse = computeResponseSerializer.serializeObjects(responseRecordV1List);
    TransportClient mockTransportClient =
        new ParameterizedComputeTransportClient(Optional.of(serializedResponse), Optional.empty());
    String storeName = "test_store";
    SimpleStoreClient<String, GenericRecord> storeClient = new SimpleStoreClient<>(
        mockTransportClient,
        storeName,
        true,
        AbstractAvroStoreClient.getDefaultDeserializationExecutor());
    VeniceMetricsRepository metricsRepository = getVeniceMetricsRepository(THIN_CLIENT, CLIENT_METRIC_ENTITIES, true);
    ClientStats stats =
        ClientStats.getClientStats(metricsRepository, storeName, RequestType.COMPUTE, null, ClientType.THIN_CLIENT);
    ClientStats streamingStats = ClientStats
        .getClientStats(metricsRepository, storeName, RequestType.COMPUTE_STREAMING, null, ClientType.THIN_CLIENT);
    CompletableFuture<Map<String, ComputeGenericRecord>> computeFuture =
        storeClient.compute(Optional.of(stats), Optional.of(streamingStats), 0)
            .project("int_field")
            .dotProduct("float_array_field1", dotProductParam, "dot_product_for_float_array_field1")
            .cosineSimilarity("float_array_field2", cosineSimilarityParam, "cosine_similarity_for_float_array_field2")
            .execute(keys);
    Map<String, ComputeGenericRecord> computeResult = computeFuture.get();
    Assert.assertEquals(computeResult.size(), 1);
    Assert.assertNotNull(computeResult.get("key1"));
    GenericRecord resultForKey1 = computeResult.get("key1");
    Assert.assertEquals(1, resultForKey1.get("int_field"));
    try {
      resultForKey1.get("dot_product_for_float_array_field1");
      Assert.fail("An exception should be thrown when retrieving a failed computation result");
    } catch (VeniceException e) {
      String errorMsgFromVenice =
          "computing this field: dot_product_for_float_array_field1, error message: array length are different";
      Assert.assertTrue(
          e.getMessage().contains(errorMsgFromVenice),
          "Error message doesn't contain: [" + errorMsgFromVenice + "], and received message is :" + e.getMessage());
    } catch (Exception e) {
      Assert.fail("Only VeniceException should be thrown");
    }
    try {
      resultForKey1.get("cosine_similarity_for_float_array_field2");
      Assert.fail("An exception should be thrown for the failed cosine similarity computation");
    } catch (VeniceException e) {
      String errorMsgFromVenice =
          "computing this field: cosine_similarity_for_float_array_field2, error message: NullPointerException";
      Assert.assertTrue(
          e.getMessage().contains(errorMsgFromVenice),
          "Error message doesn't contain: [" + errorMsgFromVenice + "], and received message is :" + e.getMessage());
    } catch (Exception e) {
      Assert.fail("Only VeniceException should be thrown");
    }
    Assert.assertNull(computeResult.get("key2"));
  }

  @Test(timeOut = 3000, expectedExceptions = ExecutionException.class, expectedExceptionsMessageRegExp = ".*mock_exception.*")
  public void testComputeReceiveNon200Response() throws ExecutionException, InterruptedException {
    TransportClient mockTransportClient = new ParameterizedComputeTransportClient(
        Optional.empty(),
        Optional.of(new VeniceClientException("mock_exception")));

    String storeName = "test_store";
    SimpleStoreClient<String, GenericRecord> storeClient = new SimpleStoreClient<>(
        mockTransportClient,
        storeName,
        true,
        AbstractAvroStoreClient.getDefaultDeserializationExecutor());
    CompletableFuture<Map<String, ComputeGenericRecord>> computeFuture =
        storeClient.compute().project("int_field").execute(keys);
    computeFuture.get();
  }

  @Test
  public void testMultiGet() throws ExecutionException, InterruptedException {
    // Mock a transport client response
    Schema recordFieldSchema = VALUE_SCHEMA.getField("record_field").schema();
    List<MultiGetResponseRecordV1> responseRecordV1List = new ArrayList<>();

    GenericRecord recordFieldValue1 = new GenericData.Record(recordFieldSchema);
    recordFieldValue1.put("nested_field1", 5.1d);

    GenericRecord result1 = new GenericData.Record(VALUE_SCHEMA);
    result1.put("int_field", 1);
    result1.put("float_field", 1.1f);
    result1.put("record_field", recordFieldValue1);
    result1.put("float_array_field1", Arrays.asList(0.1f, 0.2f));
    result1.put("float_array_field2", Arrays.asList(1.1f, 1.2f));
    result1.put("int_array_field2", Arrays.asList(10, 12));

    MultiGetResponseRecordV1 record1 = new MultiGetResponseRecordV1();
    record1.keyIndex = 0;
    record1.value = ByteBuffer.wrap(valueSerializer.serialize(result1));
    record1.schemaId = 1;

    GenericRecord recordFieldValue2 = new GenericData.Record(recordFieldSchema);
    recordFieldValue2.put("nested_field1", 7.1d);

    GenericRecord result2 = new GenericData.Record(VALUE_SCHEMA);
    result2.put("int_field", 2);
    result2.put("float_field", 2.2f);
    result2.put("record_field", recordFieldValue2);
    result2.put("float_array_field1", Arrays.asList(0.9f, 0.7f));
    result2.put("float_array_field2", Arrays.asList(1.2f, 1.5f));
    result2.put("int_array_field2", Arrays.asList(15, 13));
    MultiGetResponseRecordV1 record2 = new MultiGetResponseRecordV1();
    record2.keyIndex = 1;
    record2.value = ByteBuffer.wrap(valueSerializer.serialize(result2));
    record2.schemaId = 1;
    responseRecordV1List.add(record1);
    responseRecordV1List.add(record2);

    RecordSerializer<MultiGetResponseRecordV1> computeResponseSerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(MultiGetResponseRecordV1.SCHEMA$);
    byte[] serializedResponse = computeResponseSerializer.serializeObjects(responseRecordV1List);

    TransportClient mockTransportClient =
        new ParameterizedComputeTransportClient(Optional.of(serializedResponse), Optional.empty());

    String storeName = "test_store";
    SimpleStoreClient<String, GenericRecord> storeClient = new SimpleStoreClient<>(
        mockTransportClient,
        storeName,
        true,
        AbstractAvroStoreClient.getDefaultDeserializationExecutor());

    CompletableFuture<Map<String, GenericRecord>> requestFuture = storeClient.batchGet(keys);
    Map<String, GenericRecord> result = requestFuture.get();
    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get("key1"), result1);
    Assert.assertEquals(result.get("key2"), result2);
  }

  @Test
  public void testSingleKeyBatchGetUsesSingleGet() throws Exception {
    GenericRecord recordFieldValue = new GenericData.Record(VALUE_SCHEMA.getField("record_field").schema());
    recordFieldValue.put("nested_field1", 5.1d);

    GenericRecord expectedValue = new GenericData.Record(VALUE_SCHEMA);
    expectedValue.put("int_field", 1);
    expectedValue.put("float_field", 1.1f);
    expectedValue.put("record_field", recordFieldValue);
    expectedValue.put("float_array_field1", Collections.emptyList());
    expectedValue.put("float_array_field2", Collections.emptyList());
    expectedValue.put("int_array_field2", Collections.emptyList());

    AtomicInteger getCount = new AtomicInteger();
    AtomicInteger streamPostCount = new AtomicInteger();
    TransportClient mockTransportClient = new TransportClient() {
      @Override
      public CompletableFuture<TransportClientResponse> get(String requestPath, Map<String, String> headers) {
        getCount.incrementAndGet();
        return CompletableFuture.completedFuture(
            new TransportClientResponse(1, CompressionStrategy.NO_OP, valueSerializer.serialize(expectedValue)));
      }

      @Override
      public CompletableFuture<TransportClientResponse> post(
          String requestPath,
          Map<String, String> headers,
          byte[] requestBody) {
        CompletableFuture<TransportClientResponse> result = new CompletableFuture<>();
        result.completeExceptionally(new AssertionError("One-key batch-get should not use post"));
        return result;
      }

      @Override
      public void streamPost(
          String requestPath,
          Map<String, String> headers,
          byte[] requestBody,
          TransportClientStreamingCallback callback,
          int keyCount) {
        streamPostCount.incrementAndGet();
        Assert.fail("One-key batch-get should not use streamPost");
      }

      @Override
      public void close() {
      }
    };

    SimpleStoreClient<String, GenericRecord> storeClient = new SimpleStoreClient<>(
        mockTransportClient,
        "test_store",
        true,
        AbstractAvroStoreClient.getDefaultDeserializationExecutor());

    Map<String, GenericRecord> result = storeClient.batchGet(Collections.singleton("key1")).get();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get("key1"), expectedValue);
    Assert.assertEquals(getCount.get(), 1);
    Assert.assertEquals(streamPostCount.get(), 0);
  }

  @Test
  public void testStoreStart() {
    String storeName = "test_store_start";
    TransportClient mockTransportClient = new TransportClient() {
      private final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
      int retryCnt = 0;
      int totalFailedRetryCnt = 50;

      @Override
      public CompletableFuture<TransportClientResponse> get(String requestPath, Map<String, String> headers) {
        CompletableFuture<TransportClientResponse> result = new CompletableFuture<>();

        if (requestPath.contains(TYPE_KEY_SCHEMA)) {
          if (++retryCnt <= totalFailedRetryCnt) {
            // Fail
            result.completeExceptionally(
                new VeniceException("Fake request failure for key schema request for try: " + retryCnt));
          } else {
            SchemaResponse keySchemaResponse = new SchemaResponse();
            keySchemaResponse.setSchemaStr("\"string\"");
            keySchemaResponse.setId(1);
            try {
              result.complete(
                  new TransportClientResponse(
                      -1,
                      CompressionStrategy.NO_OP,
                      OBJECT_MAPPER.writeValueAsBytes(keySchemaResponse)));
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
          return result;
        } else if (requestPath.contains(TYPE_ALL_VALUE_SCHEMA_IDS)) {
          Map<Integer, String> valueSchemaEntries = new HashMap<>();
          valueSchemaEntries.put(1, VALUE_SCHEMA.toString());
          try {
            result.complete(
                new TransportClientResponse(
                    -1,
                    CompressionStrategy.NO_OP,
                    StoreClientTestUtils.constructMultiSchemaIdResponseInBytes(storeName, valueSchemaEntries)));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        } else if (requestPath.contains(TYPE_VALUE_SCHEMA)) {
          try {
            result.complete(
                new TransportClientResponse(
                    -1,
                    CompressionStrategy.NO_OP,
                    StoreClientTestUtils.constructSchemaResponseInBytes(storeName, 1, VALUE_SCHEMA.toString())));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        } else if (requestPath.contains(TYPE_STORAGE)) {
          // Not found
          result.complete(null);
        } else {
          result.completeExceptionally(new VeniceException("Fake request failure for path: " + requestPath));
        }
        return result;
      }

      @Override
      public CompletableFuture<TransportClientResponse> post(
          String requestPath,
          Map<String, String> headers,
          byte[] requestBody) {
        return null;
      }

      @Override
      public void streamPost(
          String requestPath,
          Map<String, String> headers,
          byte[] requestBody,
          TransportClientStreamingCallback callback,
          int keyCount) {

      }

      @Override
      public void close() throws IOException {

      }
    };
    SimpleStoreClient<String, GenericRecord> storeClient = new SimpleStoreClient<>(
        mockTransportClient,
        storeName,
        true,
        AbstractAvroStoreClient.getDefaultDeserializationExecutor(),
        false);
    storeClient.start();

    VeniceClientException thrownExceptionDuringStart =
        Assert.expectThrows(VeniceClientException.class, () -> storeClient.startWithExceptionThrownWhenFail());
    Assert.assertTrue(thrownExceptionDuringStart.getMessage().contains("Failed to initialize Venice client for store"));

    String testKey = "test_key";

    VeniceException thrownException = Assert.expectThrows(VeniceException.class, () -> storeClient.get(testKey));
    Assert.assertTrue(thrownException.getMessage().contains("Failed to initialize Venice client for store"));

    // Retry for enough time, the store client should recover by the store init happening in the async thread
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      try {
        storeClient.get(testKey).get();
      } catch (Exception e) {
        Assert.fail("Failed to get key: " + testKey);
      }
    });

    storeClient.close();
  }

  // -------- D2 service-name discovery tests --------

  /**
   * {@code discoverD2Service} is idempotent — repeated calls don't re-fetch from the discovery
   * endpoint. The {@code isServiceDiscovered} short-circuit prevents periodic-refresh code paths
   * from inadvertently re-querying the discovery endpoint after the initial resolution.
   *
   * <p>We mock the transport's {@code get()} (not {@code D2ServiceDiscovery} itself) — the real
   * {@code D2ServiceDiscovery.find()} runs against the mocked transport and we count how many
   * times it makes the discovery HTTP call, which equals how many times {@code discoverD2Service}
   * actually re-fetched.
   */
  @Test
  public void testDiscoverD2ServiceIsIdempotent() throws Exception {
    D2TransportClient mockTransport = mock(D2TransportClient.class);
    String d2ServiceName = "venice-router-cluster-stable";
    when(mockTransport.getServiceName()).thenReturn(d2ServiceName);

    D2ServiceDiscoveryResponse response = new D2ServiceDiscoveryResponse();
    response.setD2Service(d2ServiceName);
    byte[] body = ObjectMapperFactory.getInstance().writeValueAsBytes(response);
    when(
        mockTransport
            .get(org.mockito.ArgumentMatchers.contains("discover_cluster"), org.mockito.ArgumentMatchers.any()))
                .thenReturn(
                    CompletableFuture.completedFuture(new TransportClientResponse(0, CompressionStrategy.NO_OP, body)));

    SimpleStoreClient<String, String> client = new SimpleStoreClient<>(
        mockTransport,
        "test_store",
        false,
        AbstractAvroStoreClient.getDefaultDeserializationExecutor(),
        false);

    client.discoverD2Service(false);
    client.discoverD2Service(false);
    client.discoverD2Service(false);

    // The real D2ServiceDiscovery.find() makes one transport.get() call per invocation.
    // The isServiceDiscovered short-circuit means find() runs exactly once across all 3 calls.
    verify(mockTransport, org.mockito.Mockito.times(1))
        .get(org.mockito.ArgumentMatchers.contains("discover_cluster"), org.mockito.ArgumentMatchers.any());
  }

  // -------- Push-based cluster-name listener propagation --------

  /**
   * {@code discoverD2Service} fetches a {@link D2ServiceDiscoveryResponse} that already carries
   * both the D2 service name and the Venice cluster name. The cluster value is forwarded directly
   * to the wired listener — no second discovery RPC is issued — which is what flips the metric
   * dimension from the bootstrap {@code "unknown_cluster"} sentinel to the real cluster on first request.
   */
  @Test
  public void testDiscoverD2ServicePushesClusterToWiredListener() throws Exception {
    String resolvedCluster = "venice-cluster-resolved";
    String resolvedD2Service = "venice-router-cluster-A-d2";

    D2TransportClient mockTransport = mock(D2TransportClient.class);
    D2ServiceDiscoveryResponse response = new D2ServiceDiscoveryResponse();
    response.setD2Service(resolvedD2Service);
    response.setCluster(resolvedCluster);
    byte[] body = ObjectMapperFactory.getInstance().writeValueAsBytes(response);
    when(
        mockTransport
            .get(org.mockito.ArgumentMatchers.contains("discover_cluster"), org.mockito.ArgumentMatchers.any()))
                .thenReturn(
                    CompletableFuture.completedFuture(new TransportClientResponse(0, CompressionStrategy.NO_OP, body)));

    SimpleStoreClient<String, String> client = new SimpleStoreClient<>(
        mockTransport,
        "test_store",
        false,
        AbstractAvroStoreClient.getDefaultDeserializationExecutor(),
        false);

    CompletableFuture<String> received = new CompletableFuture<>();
    client.setClusterNameChangeListener(received::complete);

    client.discoverD2Service(false);

    // Listener fires synchronously from inside discoverD2Service — short timeout guards against
    // future refactors that change the path to async.
    org.testng.Assert.assertEquals(received.get(1, TimeUnit.SECONDS), resolvedCluster);
  }

  /**
   * On a 301-redirect-driven migration the {@code Location} header carries only the new D2
   * authority — the cluster has to be re-resolved. The notifier wired into the transport does
   * that resolution (via {@link com.linkedin.venice.client.store.D2ServiceDiscovery}) and feeds
   * the result to the same listener that initial discovery uses.
   */
  @Test
  public void testRedirectNotifierResolvesAndPushesClusterToListener() throws Exception {
    String resolvedCluster = "venice-cluster-after-migration";

    D2TransportClient mockTransport = mock(D2TransportClient.class);
    D2ServiceDiscoveryResponse response = new D2ServiceDiscoveryResponse();
    response.setCluster(resolvedCluster);
    byte[] body = ObjectMapperFactory.getInstance().writeValueAsBytes(response);
    when(
        mockTransport
            .get(org.mockito.ArgumentMatchers.contains("discover_cluster"), org.mockito.ArgumentMatchers.any()))
                .thenReturn(
                    CompletableFuture.completedFuture(new TransportClientResponse(0, CompressionStrategy.NO_OP, body)));

    SimpleStoreClient<String, String> client = new SimpleStoreClient<>(
        mockTransport,
        "test_store",
        false,
        AbstractAvroStoreClient.getDefaultDeserializationExecutor(),
        false);

    CompletableFuture<String> received = new CompletableFuture<>();
    client.setClusterNameChangeListener(received::complete);

    org.mockito.ArgumentCaptor<Runnable> captor = org.mockito.ArgumentCaptor.forClass(Runnable.class);
    verify(mockTransport).setRedirectNotifier(captor.capture());

    // Fire the notifier as the 301 handler would. The notifier's body dispatches via runAsync, so
    // we wait for the listener to receive the resolved cluster.
    captor.getValue().run();

    org.testng.Assert.assertEquals(received.get(5, TimeUnit.SECONDS), resolvedCluster);
  }

  /**
   * The cluster-name change hook is D2-specific (the 301-redirect detection lives in
   * {@link D2TransportClient}). For non-D2 transports the wiring is a no-op — the listener is
   * silently dropped rather than triggering on a transport that has no migration concept.
   */
  @Test
  public void testSetClusterNameChangeListenerNoOpForNonD2Transport() {
    TransportClient nonD2Transport = mock(TransportClient.class);
    SimpleStoreClient<String, String> client = new SimpleStoreClient<>(
        nonD2Transport,
        "test_store",
        false,
        AbstractAvroStoreClient.getDefaultDeserializationExecutor(),
        false);

    client.setClusterNameChangeListener(name -> {});
  }

  /**
   * The redirect notifier's body re-resolves cluster via {@code D2ServiceDiscovery.find()} since
   * a 301 redirect's {@code Location} header carries no cluster info. If discovery fails (network
   * partition, controller down, store deleted), the listener must not be invoked with stale or
   * partial state, and the failure must not propagate out of the async task — the contract is
   * "log loudly, leave {@code venice.cluster.name} stale until next migration."
   */
  @Test
  public void testSetClusterNameChangeListenerSwallowsDiscoveryFailure() throws Exception {
    D2TransportClient mockTransport = mock(D2TransportClient.class);
    // Make every discovery attempt fail. find(retryOnFailure=true) does up to 10 attempts before
    // throwing ServiceDiscoveryException; we return a future that completes exceptionally each time.
    CompletableFuture<TransportClientResponse> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new IOException("simulated discovery outage"));
    when(
        mockTransport
            .get(org.mockito.ArgumentMatchers.contains("discover_cluster"), org.mockito.ArgumentMatchers.any()))
                .thenReturn(failedFuture);

    SimpleStoreClient<String, String> client = new SimpleStoreClient<>(
        mockTransport,
        "test_store",
        false,
        AbstractAvroStoreClient.getDefaultDeserializationExecutor(),
        false);

    java.util.concurrent.atomic.AtomicReference<String> received = new java.util.concurrent.atomic.AtomicReference<>();
    client.setClusterNameChangeListener(received::set);

    org.mockito.ArgumentCaptor<Runnable> captor = org.mockito.ArgumentCaptor.forClass(Runnable.class);
    verify(mockTransport).setRedirectNotifier(captor.capture());

    // Fire the notifier as the 301 handler would; its body dispatches the discovery + listener
    // call via CompletableFuture.runAsync on the common pool. Drain the pool to deterministically
    // wait for that task (and find()'s ~1s of internal retry) to finish before asserting.
    captor.getValue().run();
    boolean drained =
        java.util.concurrent.ForkJoinPool.commonPool().awaitQuiescence(5, java.util.concurrent.TimeUnit.SECONDS);
    org.testng.Assert.assertTrue(drained, "common pool did not quiesce within timeout");
    org.testng.Assert.assertNull(received.get(), "listener must not be invoked when discovery fails");
  }

  /**
   * The discovery response carries cluster information separately from the D2 service name. When
   * a controller responds with a {@code null} cluster (older controllers, partial response), the
   * upper layer must not fire the listener with {@code null} — emitting a {@code null} into the
   * cluster dimension would NPE inside OTel attribute construction. The listener stays untouched
   * and the dimension remains on its current value.
   */
  @Test
  public void testDiscoverD2ServiceDoesNotFireListenerWhenResponseClusterIsNull() throws Exception {
    D2TransportClient mockTransport = mock(D2TransportClient.class);
    D2ServiceDiscoveryResponse response = new D2ServiceDiscoveryResponse();
    response.setD2Service("venice-router-cluster-d2");
    // No cluster set — simulates an older/partial controller response.
    byte[] body = ObjectMapperFactory.getInstance().writeValueAsBytes(response);
    when(
        mockTransport
            .get(org.mockito.ArgumentMatchers.contains("discover_cluster"), org.mockito.ArgumentMatchers.any()))
                .thenReturn(
                    CompletableFuture.completedFuture(new TransportClientResponse(0, CompressionStrategy.NO_OP, body)));

    SimpleStoreClient<String, String> client = new SimpleStoreClient<>(
        mockTransport,
        "test_store",
        false,
        AbstractAvroStoreClient.getDefaultDeserializationExecutor(),
        false);

    java.util.concurrent.atomic.AtomicReference<String> received = new java.util.concurrent.atomic.AtomicReference<>();
    client.setClusterNameChangeListener(received::set);

    client.discoverD2Service(false);

    Assert.assertNull(received.get(), "listener must not be fired with a null cluster");
  }

  /**
   * If the wired listener throws on invocation (a bug in the cluster fan-out, an OTel attribute
   * builder failure, etc.), {@code AbstractAvroStoreClient} must contain the failure inside its
   * {@code fireClusterListener} helper so {@code discoverD2Service} can complete — otherwise a
   * single buggy listener would block service discovery and the entire store client would be
   * unable to come up.
   */
  @Test
  public void testDiscoverD2ServiceSwallowsListenerRuntimeException() throws Exception {
    String resolvedCluster = "venice-cluster-resolved";
    String resolvedD2Service = "venice-router-cluster-d2";

    D2TransportClient mockTransport = mock(D2TransportClient.class);
    D2ServiceDiscoveryResponse response = new D2ServiceDiscoveryResponse();
    response.setD2Service(resolvedD2Service);
    response.setCluster(resolvedCluster);
    byte[] body = ObjectMapperFactory.getInstance().writeValueAsBytes(response);
    when(
        mockTransport
            .get(org.mockito.ArgumentMatchers.contains("discover_cluster"), org.mockito.ArgumentMatchers.any()))
                .thenReturn(
                    CompletableFuture.completedFuture(new TransportClientResponse(0, CompressionStrategy.NO_OP, body)));

    SimpleStoreClient<String, String> client = new SimpleStoreClient<>(
        mockTransport,
        "test_store",
        false,
        AbstractAvroStoreClient.getDefaultDeserializationExecutor(),
        false);

    java.util.concurrent.atomic.AtomicInteger callCount = new java.util.concurrent.atomic.AtomicInteger();
    client.setClusterNameChangeListener(name -> {
      callCount.incrementAndGet();
      throw new RuntimeException("simulated listener bug");
    });

    // Must not propagate out — the listener exception is swallowed inside fireClusterListener.
    client.discoverD2Service(false);

    Assert.assertEquals(callCount.get(), 1, "listener was invoked exactly once before the throw");
    // discoverD2Service still completes — calling it again is a no-op gated by isServiceDiscovered.
    client.discoverD2Service(false);
    Assert.assertEquals(callCount.get(), 1, "second call is short-circuited by isServiceDiscovered");
  }

  /**
   * The external-storage deserialization re-entry seam is intentionally Fast Client only. Thin-client instances
   * inherit the throwing default on {@link AvroGenericStoreClient}, so a thin-client caller wiring up the seam by
   * mistake gets a loud failure rather than silently going down a path that lacks version-aware compressor support.
   */
  @Test
  public void testDecompressAndDeserializeNotSupportedOnThinClient() {
    TransportClient mockTransportClient = mock(TransportClient.class);
    SimpleStoreClient<String, GenericRecord> client = new SimpleStoreClient<>(
        mockTransportClient,
        "test-store",
        true,
        AbstractAvroStoreClient.getDefaultDeserializationExecutor());

    try {
      client.decompressAndDeserialize(ByteBuffer.allocate(0), 1, "k1");
      Assert.fail("expected UnsupportedOperationException — thin-client does not implement this seam");
    } catch (UnsupportedOperationException expected) {
      // expected
    }
  }
}
