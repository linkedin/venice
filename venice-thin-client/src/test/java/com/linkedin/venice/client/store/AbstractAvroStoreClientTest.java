package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compute.protocol.response.ComputeResponseRecordV1;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import edu.emory.mathcs.backport.java.util.Collections;
import io.tehuti.metrics.MetricsRepository;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class AbstractAvroStoreClientTest {
  private static class SimpleStoreClient<K, V> extends AbstractAvroStoreClient<K, V> {

    public SimpleStoreClient(TransportClient transportClient, String storeName, boolean needSchemaReader,
        Executor deserializationExecutor) {
      super(transportClient, needSchemaReader, ClientConfig.defaultGenericClientConfig(storeName).setDeserializationExecutor(deserializationExecutor));
    }

    @Override
    protected AbstractAvroStoreClient<K, V> getStoreClientForSchemaReader() {
      return null;
    }

    @Override
    public RecordDeserializer<V> getDataRecordDeserializer(int schemaId) throws VeniceClientException {
      return null;
    }

    @Override
    protected SchemaReader getSchemaReader() {
      SchemaReader mockSchemaReader = mock(SchemaReader.class);
      doReturn(Schema.create(Schema.Type.STRING)).when(mockSchemaReader).getKeySchema();
      return mockSchemaReader;
    }

    @Override
    public Schema getLatestValueSchema() {
      return Schema.parse(VALUE_SCHEMA);
    }
  }

  private static final String VALUE_SCHEMA = "{\n" + "\t\"type\": \"record\",\n" + "\t\"name\": \"record_schema\",\n"
      + "\t\"fields\": [\n"
      + "\t\t{\"name\": \"int_field\", \"type\": \"int\", \"default\": 0, \"doc\": \"doc for int_field\"},\n"
      + "\t\t{\"name\": \"float_field\", \"type\": \"float\", \"doc\": \"doc for float_field\"},\n" + "\t\t{\n"
      + "\t\t\t\"name\": \"record_field\",\n" + "\t\t\t\"namespace\": \"com.linkedin.test\",\n" + "\t\t\t\"type\": {\n"
      + "\t\t\t\t\"name\": \"Record1\",\n" + "\t\t\t\t\"type\": \"record\",\n" + "\t\t\t\t\"fields\": [\n"
      + "\t\t\t\t\t{\"name\": \"nested_field1\", \"type\": \"double\", \"doc\": \"doc for nested field\"}\n"
      + "\t\t\t\t]\n" + "\t\t\t}\n" + "\t\t},\n"
      + "\t\t{\"name\": \"float_array_field1\", \"type\": {\"type\": \"array\", \"items\": \"float\"}},\n"
      + "\t\t{\"name\": \"float_array_field2\", \"type\": {\"type\": \"array\", \"items\": \"float\"}},\n"
      + "\t\t{\"name\": \"int_array_field2\", \"type\": {\"type\": \"array\", \"items\": \"int\"}}\n" + "\t]\n" + "}";

  private static final Set<String> keys = new HashSet<>();
  static {
    keys.add("key1");
    keys.add("key2");
  }

  private static final Float[] dotProductParam = new Float[]{0.1f, 0.2f};

  @Test
  public void testCompute() throws ExecutionException, InterruptedException {
    TransportClient mockTransportClient = mock(TransportClient.class);

    // Mock a transport client response
    String resultSchemaStr = "{\n" + "  \"type\" : \"record\",\n" + "  \"name\" : \"test_store_VeniceComputeResult\",\n"
        + "  \"doc\" : \"\",\n" + "  \"fields\" : [ {\n" + "    \"name\" : \"int_field\",\n"
        + "    \"type\" : \"int\",\n" + "    \"doc\" : \"\",\n" + "    \"default\" : 0\n" + "  }, {\n"
        + "    \"name\" : \"dot_product_for_float_array_field1\",\n" + "    \"type\" : \"double\",\n"
        + "    \"doc\" : \"\",\n" + "    \"default\" : 0\n" + "  }, {\n"
        + "    \"name\" : \"veniceComputationError\",\n" + "    \"type\" : {\n" + "      \"type\" : \"map\",\n"
        + "      \"values\" : \"string\"\n" + "    },\n" + "    \"doc\" : \"\",\n" + "    \"default\" : { }\n"
        + "  } ]\n" + "}";
    Schema resultSchema = Schema.parse(resultSchemaStr);
    RecordSerializer<GenericRecord> resultSerializer = SerializerDeserializerFactory.getAvroGenericSerializer(resultSchema);
    List<ComputeResponseRecordV1> responseRecordV1List = new ArrayList<>();
    GenericRecord result1 = new GenericData.Record(resultSchema);
    result1.put("int_field", 1);
    result1.put("dot_product_for_float_array_field1", 1.1d);
    result1.put("veniceComputationError", Collections.emptyMap());
    ComputeResponseRecordV1 record1 = new ComputeResponseRecordV1();
    record1.keyIndex = 0;
    record1.value = ByteBuffer.wrap(resultSerializer.serialize(result1));

    GenericRecord result2 = new GenericData.Record(resultSchema);
    result2.put("int_field", 2);
    result2.put("dot_product_for_float_array_field1", 1.2d);
    result2.put("veniceComputationError", Collections.emptyMap());
    ComputeResponseRecordV1 record2 = new ComputeResponseRecordV1();
    record2.keyIndex = 1;
    record2.value = ByteBuffer.wrap(resultSerializer.serialize(result2));
    responseRecordV1List.add(record1);
    responseRecordV1List.add(record2);

    RecordSerializer<ComputeResponseRecordV1> computeResponseSerializer = SerializerDeserializerFactory.getAvroGenericSerializer(ComputeResponseRecordV1.SCHEMA$);

    byte[] serializedResponse = computeResponseSerializer.serializeObjects(responseRecordV1List);

    TransportClientResponse clientResponse = new TransportClientResponse(
        ReadAvroProtocolDefinition.COMPUTE_RESPONSE_V1.getProtocolVersion(), serializedResponse);
    CompletableFuture<TransportClientResponse> transportFuture = new CompletableFuture<>();
    transportFuture.complete(clientResponse);

    doReturn(transportFuture).when(mockTransportClient).post(any(), any(), any());
    String storeName = "test_store";
    SimpleStoreClient<String, GenericRecord> storeClient = new SimpleStoreClient<>(mockTransportClient, storeName,
        false, AbstractAvroStoreClient.getDefaultDeserializationExecutor());
    MetricsRepository metricsRepository = new MetricsRepository();
    ClientStats clientStats = new ClientStats(metricsRepository, storeName, RequestType.COMPUTE);
    CompletableFuture<Map<String, GenericRecord>> computeFuture = storeClient.compute(Optional.of(clientStats), 0)
        .project("int_field")
        .dotProduct("float_array_field1", dotProductParam, "dot_product_for_float_array_field1")
        .execute(keys);
    Map<String, GenericRecord> computeResult = computeFuture.get();
    Assert.assertEquals(computeResult.size(), 2);
    Assert.assertNotNull(computeResult.get("key1"));
    GenericRecord resultForKey1 = computeResult.get("key1");
    Assert.assertEquals(1, resultForKey1.get("int_field"));
    Assert.assertEquals(1.1d, resultForKey1.get("dot_product_for_float_array_field1"));
    Assert.assertNotNull(computeResult.get("key2"));
    GenericRecord resultForKey2 = computeResult.get("key2");
    Assert.assertEquals(2, resultForKey2.get("int_field"));
    Assert.assertEquals(1.2d, resultForKey2.get("dot_product_for_float_array_field1"));
  }

  @Test
  public void testComputeFailure() throws ExecutionException, InterruptedException {
    TransportClient mockTransportClient = mock(TransportClient.class);

    // Mock a transport client response
    String resultSchemaStr = "{\n" + "  \"type\" : \"record\",\n" + "  \"name\" : \"test_store_VeniceComputeResult\",\n"
        + "  \"doc\" : \"\",\n" + "  \"fields\" : [ {\n" + "    \"name\" : \"int_field\",\n"
        + "    \"type\" : \"int\",\n" + "    \"doc\" : \"\",\n" + "    \"default\" : 0\n" + "  }, {\n"
        + "    \"name\" : \"dot_product_for_float_array_field1\",\n" + "    \"type\" : \"double\",\n"
        + "    \"doc\" : \"\",\n" + "    \"default\" : 0\n" + "  }, {\n"
        + "    \"name\" : \"veniceComputationError\",\n" + "    \"type\" : {\n" + "      \"type\" : \"map\",\n"
        + "      \"values\" : \"string\"\n" + "    },\n" + "    \"doc\" : \"\",\n" + "    \"default\" : { }\n"
        + "  } ]\n" + "}";
    Schema resultSchema = Schema.parse(resultSchemaStr);
    RecordSerializer<GenericRecord> resultSerializer = SerializerDeserializerFactory.getAvroGenericSerializer(resultSchema);
    List<ComputeResponseRecordV1> responseRecordV1List = new ArrayList<>();
    GenericRecord result1 = new GenericData.Record(resultSchema);
    result1.put("int_field", 1);
    result1.put("dot_product_for_float_array_field1", 0d);
    Map<String, String> computationErrorMap = new HashMap<>();
    computationErrorMap.put("dot_product_for_float_array_field1", "array length are different");
    result1.put("veniceComputationError", computationErrorMap);
    ComputeResponseRecordV1 record1 = new ComputeResponseRecordV1();
    record1.keyIndex = 0;
    record1.value = ByteBuffer.wrap(resultSerializer.serialize(result1));
    responseRecordV1List.add(record1);

    RecordSerializer<ComputeResponseRecordV1> computeResponseSerializer = SerializerDeserializerFactory.getAvroGenericSerializer(ComputeResponseRecordV1.SCHEMA$);

    byte[] serializedResponse = computeResponseSerializer.serializeObjects(responseRecordV1List);

    TransportClientResponse clientResponse = new TransportClientResponse(
        ReadAvroProtocolDefinition.COMPUTE_RESPONSE_V1.getProtocolVersion(), serializedResponse);
    CompletableFuture<TransportClientResponse> transportFuture = new CompletableFuture<>();
    transportFuture.complete(clientResponse);

    doReturn(transportFuture).when(mockTransportClient).post(any(), any(), any());
    String storeName = "test_store";
    SimpleStoreClient<String, GenericRecord> storeClient = new SimpleStoreClient<>(mockTransportClient, storeName,
        false, AbstractAvroStoreClient.getDefaultDeserializationExecutor());
    MetricsRepository metricsRepository = new MetricsRepository();
    ClientStats clientStats = new ClientStats(metricsRepository, storeName, RequestType.COMPUTE);
    CompletableFuture<Map<String, GenericRecord>> computeFuture = storeClient.compute(Optional.of(clientStats), 0)
        .project("int_field")
        .dotProduct("float_array_field1", dotProductParam, "dot_product_for_float_array_field1")
        .execute(keys);
    Map<String, GenericRecord> computeResult = computeFuture.get();
    Assert.assertEquals(computeResult.size(), 1);
    Assert.assertNotNull(computeResult.get("key1"));
    GenericRecord resultForKey1 = computeResult.get("key1");
    Assert.assertEquals(1, resultForKey1.get("int_field"));
    try {
      resultForKey1.get("dot_product_for_float_array_field1");
      Assert.fail("An exception should be thrown when retrieving a failed computation result");
    } catch (VeniceException e) {
      String errorMsgFromVenice = "computing this field: dot_product_for_float_array_field1, error message: array length are different";
      Assert.assertTrue(e.getMessage().contains(errorMsgFromVenice), "Error message doesn't contain: [" +
          errorMsgFromVenice + "], and received message is :" + e.getMessage());
    } catch (Exception e) {
      Assert.fail("Only VeniceException shuold be thrown");
    }
    Assert.assertNull(computeResult.get("key2"));
  }
}
