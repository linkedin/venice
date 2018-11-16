package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compute.protocol.response.ComputeResponseRecordV1;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.TestUtils;
import edu.emory.mathcs.backport.java.util.Collections;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.eclipse.jetty.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.*;

public class StatTrackingStoreClientTest {
  private InternalAvroStoreClient<String, Object> mockStoreClient;
  private String metricPrefix;

  @BeforeTest
  public void setUp() {
    mockStoreClient = mock(InternalAvroStoreClient.class);

    String storeName = TestUtils.getUniqueString("store");
    doReturn(storeName).when(mockStoreClient).getStoreName();
    metricPrefix = "." + storeName;
  }


  @Test
  public void testGet() throws ExecutionException, InterruptedException {
    CompletableFuture<Object> mockInnerFuture = new CompletableFuture();
    Object mockReturnObject = mock(Object.class);
    mockInnerFuture.complete(mockReturnObject);
    mockInnerFuture = mockInnerFuture.handle((value, throwable) -> {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return value;

    });

    doReturn(mockInnerFuture).when(mockStoreClient).get(any(), any(), anyLong());

    MetricsRepository repository = new MetricsRepository();

    StatTrackingStoreClient<String, Object> statTrackingStoreClient = new StatTrackingStoreClient<>(mockStoreClient, repository);
    statTrackingStoreClient.get("key").get();

    Map<String, ? extends Metric> metrics = repository.metrics();
    Metric requestMetric = metrics.get(metricPrefix + "--request.OccurrenceRate");
    Metric healthyRequestMetric = metrics.get(metricPrefix + "--healthy_request.OccurrenceRate");
    Metric unhealthyRequestMetric = metrics.get(metricPrefix + "--unhealthy_request.OccurrenceRate");

    Assert.assertTrue(requestMetric.value() > 0.0);
    Assert.assertTrue(healthyRequestMetric.value() > 0.0);
    Assert.assertEquals(unhealthyRequestMetric.value(), 0.0);
  }

  @Test
  public void testMultiGet() throws ExecutionException, InterruptedException {
    CompletableFuture<Object> mockInnerFuture = new CompletableFuture();
    Map<String, String> result = new HashMap<>();
    Set<String> keySet = new HashSet<>();
    String keyPrefix = "key_";
    for (int i = 0; i < 5; ++i) {
      result.put(keyPrefix + i, "value_" + i);
    }
    for (int i = 0; i < 10; ++i) {
      keySet.add(keyPrefix + i);
    }
    mockInnerFuture.complete(result);
    mockInnerFuture = mockInnerFuture.handle((value, throwable) -> {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return value;

    });
    doReturn(mockInnerFuture).when(mockStoreClient).batchGet(any(), any(), anyLong());

    MetricsRepository repository = new MetricsRepository();

    StatTrackingStoreClient<String, Object> statTrackingStoreClient = new StatTrackingStoreClient<>(mockStoreClient, repository);
    statTrackingStoreClient.batchGet(keySet).get();

    Map<String, ? extends Metric> metrics = repository.metrics();
    Metric requestMetric = metrics.get(metricPrefix + "--multiget_request.OccurrenceRate");
    Metric healthyRequestMetric = metrics.get(metricPrefix + "--multiget_healthy_request.OccurrenceRate");
    Metric unhealthyRequestMetric = metrics.get(metricPrefix + "--multiget_unhealthy_request.OccurrenceRate");
    Metric keyCountMetric = metrics.get(metricPrefix + "--multiget_request_key_count.Avg");
    Metric successKeyCountMetric = metrics.get(metricPrefix + "--multiget_success_request_key_count.Avg");
    Metric successKeyRatioMetric = metrics.get(metricPrefix + "--multiget_success_request_key_ratio.SimpleRatioStat");

    Assert.assertTrue(requestMetric.value() > 0.0);
    Assert.assertTrue(healthyRequestMetric.value() > 0.0);
    Assert.assertEquals(unhealthyRequestMetric.value(), 0.0);
    Assert.assertEquals(keyCountMetric.value(), 10.0);
    Assert.assertEquals(successKeyCountMetric.value(), 5.0);
    Assert.assertTrue(successKeyRatioMetric.value() > 0, "Success Key Ratio should be positive");
  }

  @Test
  public void testGetWithException() throws ExecutionException, InterruptedException {
    CompletableFuture<Object> mockInnerFuture = new CompletableFuture();
    mockInnerFuture.completeExceptionally(new VeniceClientHttpException("Inner mock exception", HttpStatus.BAD_REQUEST_400));
    mockInnerFuture = mockInnerFuture.handle((value, throwable) -> {
      try {
        Thread.sleep(50);
        InternalAvroStoreClient.handleStoreExceptionInternally(throwable);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return value;

    });
    doReturn(mockInnerFuture).when(mockStoreClient).get(any(), any(), anyLong());

    MetricsRepository repository = new MetricsRepository();

    StatTrackingStoreClient<String, Object> statTrackingStoreClient = new StatTrackingStoreClient<>(mockStoreClient, repository);
    try {
      statTrackingStoreClient.get("key").get();
      Assert.fail("ExecutionException should be thrown");
    } catch (ExecutionException e) {
      System.out.println(e);
      // expected
    }

    Map<String, ? extends Metric> metrics = repository.metrics();
    Metric requestMetric = metrics.get(metricPrefix + "--request.OccurrenceRate");
    Metric healthyRequestMetric = metrics.get(metricPrefix + "--healthy_request.OccurrenceRate");
    Metric unhealthyRequestMetric = metrics.get(metricPrefix + "--unhealthy_request.OccurrenceRate");
    Metric http400RequestMetric = metrics.get(metricPrefix + "--http_400_request.OccurrenceRate");

    Assert.assertTrue(requestMetric.value() > 0.0);
    Assert.assertEquals(healthyRequestMetric.value(), 0.0);
    Assert.assertTrue(unhealthyRequestMetric.value() > 0.0);
    Assert.assertTrue(http400RequestMetric.value() > 0.0);
  }

  // Test Compute
  private static class SimpleStoreClient<K, V> extends AbstractAvroStoreClient<K, V> {

    public SimpleStoreClient(TransportClient transportClient, String storeName, boolean needSchemaReader,
        Executor deserializationExecutor) {
      super(transportClient, storeName, needSchemaReader, deserializationExecutor);
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

    RecordSerializer<ComputeResponseRecordV1> computeResponseSerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(ComputeResponseRecordV1.SCHEMA$);

    byte[] serializedResponse = computeResponseSerializer.serializeObjects(responseRecordV1List);

    TransportClientResponse clientResponse = new TransportClientResponse(
        ReadAvroProtocolDefinition.COMPUTE_RESPONSE_V1.getProtocolVersion(), serializedResponse);
    CompletableFuture<TransportClientResponse> transportFuture = new CompletableFuture<>();
    transportFuture.complete(clientResponse);
    transportFuture.handle( (value, throwable) -> {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return value;
    });

    doReturn(transportFuture).when(mockTransportClient).post(any(), any(), any());
    String storeName = "test_store";
    SimpleStoreClient<String, GenericRecord>
        storeClient = new SimpleStoreClient<>(mockTransportClient, storeName,
        false, AbstractAvroStoreClient.getDefaultDeserializationExecutor());

    MetricsRepository repository = new MetricsRepository();
    StatTrackingStoreClient<String, GenericRecord> statTrackingStoreClient = new StatTrackingStoreClient<>(storeClient, repository);

    CompletableFuture<Map<String, GenericRecord>> computeFuture = statTrackingStoreClient.compute()
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

    // Verify metrics
    Map<String, ? extends Metric> metrics = repository.metrics();
    String storeMetricPrefix = "." + storeName;

    Metric requestMetric = metrics.get(storeMetricPrefix + "--compute_request.OccurrenceRate");
    Metric healthyRequestMetric = metrics.get(storeMetricPrefix + "--compute_healthy_request.OccurrenceRate");
    Metric unhealthyRequestMetric = metrics.get(storeMetricPrefix + "--compute_unhealthy_request.OccurrenceRate");

    Assert.assertTrue(requestMetric.value() > 0.0);
    Assert.assertTrue(healthyRequestMetric.value() > 0.0);
    Assert.assertEquals(unhealthyRequestMetric.value(), 0.0);
  }
}
