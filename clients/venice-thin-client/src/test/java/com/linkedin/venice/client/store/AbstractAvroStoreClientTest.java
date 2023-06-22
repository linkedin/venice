package com.linkedin.venice.client.store;

import static com.linkedin.venice.client.schema.RouterBackedSchemaReader.*;
import static com.linkedin.venice.client.store.AbstractAvroStoreClient.*;
import static com.linkedin.venice.client.store.D2ServiceDiscovery.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.client.store.transport.TransportClientStreamingCallback;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compute.protocol.response.ComputeResponseRecordV1;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.TestUtils;
import io.tehuti.metrics.MetricsRepository;
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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AbstractAvroStoreClientTest {
  private static class SimpleStoreClient<K, V> extends AbstractAvroStoreClient<K, V> {
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
      super(
          transportClient,
          needSchemaReader,
          ClientConfig.defaultGenericClientConfig(storeName).setDeserializationExecutor(deserializationExecutor));
      this.overrideGetSchemaReader = overrideGetSchemaReader;
    }

    @Override
    public RecordDeserializer<V> getDataRecordDeserializer(int schemaId) throws VeniceClientException {
      return null;
    }

    @Override
    protected SchemaReader getSchemaReader() {
      if (overrideGetSchemaReader) {
        SchemaReader mockSchemaReader = mock(SchemaReader.class);
        doReturn(Schema.create(Schema.Type.STRING)).when(mockSchemaReader).getKeySchema();
        return mockSchemaReader;
      } else {
        return super.getSchemaReader();
      }
    }

    @Override
    public Schema getLatestValueSchema() {
      return VALUE_SCHEMA;
    }
  }

  private static final Schema VALUE_SCHEMA = Schema.parse(
      "{\n" + "\t\"type\": \"record\",\n" + "\t\"name\": \"record_schema\",\n" + "\t\"fields\": [\n"
          + "\t\t{\"name\": \"int_field\", \"type\": \"int\", \"default\": 0, \"doc\": \"doc for int_field\"},\n"
          + "\t\t{\"name\": \"float_field\", \"type\": \"float\", \"doc\": \"doc for float_field\"},\n" + "\t\t{\n"
          + "\t\t\t\"name\": \"record_field\",\n" + "\t\t\t\"namespace\": \"com.linkedin.test\",\n"
          + "\t\t\t\"type\": {\n" + "\t\t\t\t\"name\": \"Record1\",\n" + "\t\t\t\t\"type\": \"record\",\n"
          + "\t\t\t\t\"fields\": [\n"
          + "\t\t\t\t\t{\"name\": \"nested_field1\", \"type\": \"double\", \"doc\": \"doc for nested field\"}\n"
          + "\t\t\t\t]\n" + "\t\t\t}\n" + "\t\t},\n"
          + "\t\t{\"name\": \"float_array_field1\", \"type\": {\"type\": \"array\", \"items\": \"float\"}},\n"
          + "\t\t{\"name\": \"float_array_field2\", \"type\": {\"type\": \"array\", \"items\": \"float\"}},\n"
          + "\t\t{\"name\": \"int_array_field2\", \"type\": {\"type\": \"array\", \"items\": \"int\"}}\n" + "\t]\n"
          + "}");

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
        + "         { \"name\": \"veniceComputationError\", \"type\": { \"type\": \"map\", \"values\": \"string\" }, \"doc\": \"\", \"default\": { } }        "
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
    result1.put("veniceComputationError", Collections.emptyMap());
    ComputeResponseRecordV1 record1 = new ComputeResponseRecordV1();
    record1.keyIndex = 0;
    record1.value = ByteBuffer.wrap(resultSerializer.serialize(result1));

    List<Float> hadamardProductResult2 = Arrays.asList(3.2f, 4.2f);
    GenericRecord result2 = new GenericData.Record(resultSchema);
    result2.put("int_field", 2);
    result2.put("dot_product_for_float_array_field1", 1.2f);
    result2.put("cosine_similarity_for_float_array_field2", 2.2f);
    result2.put("hadamard_product_for_float_array_field1", hadamardProductResult2);
    result2.put("veniceComputationError", Collections.emptyMap());
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
    MetricsRepository metricsRepository = new MetricsRepository();
    ClientStats stats = ClientStats.getClientStats(metricsRepository, storeName, RequestType.COMPUTE, null);
    ClientStats streamingStats =
        ClientStats.getClientStats(metricsRepository, storeName, RequestType.COMPUTE_STREAMING, null);
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
        + "         { \"name\": \"veniceComputationError\", \"type\": { \"type\": \"map\", \"values\": \"string\" }, \"doc\": \"\", \"default\": { } }        "
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
    result1.put("veniceComputationError", computationErrorMap);
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
    MetricsRepository metricsRepository = new MetricsRepository();
    ClientStats stats = ClientStats.getClientStats(metricsRepository, storeName, RequestType.COMPUTE, null);
    ClientStats streamingStats =
        ClientStats.getClientStats(metricsRepository, storeName, RequestType.COMPUTE_STREAMING, null);
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
  public void testStoreInitAsyncRetry() {
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
            result.completeExceptionally(new VeniceException("Fake request failure for key schema request"));
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
        "test_store_init_retry",
        true,
        AbstractAvroStoreClient.getDefaultDeserializationExecutor(),
        false);
    storeClient.setAsyncStoreInitSleepIntervalMs(1);
    storeClient.start();
    String testKey = "test_key";

    VeniceException thrownException = Assert.expectThrows(VeniceException.class, () -> storeClient.get(testKey));
    Assert.assertTrue(thrownException.getMessage().contains("Failed to initializing Venice Client"));

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
}
