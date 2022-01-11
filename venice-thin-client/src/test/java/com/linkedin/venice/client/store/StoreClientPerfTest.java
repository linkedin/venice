package com.linkedin.venice.client.store;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.VeniceConstants;
import com.linkedin.venice.client.schema.RouterBackedSchemaReader;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.utils.StoreClientTestUtils;
import com.linkedin.venice.compute.protocol.response.ComputeResponseRecordV1;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.MockD2ServerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.Utils;
import io.netty.handler.codec.http.FullHttpResponse;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


@Test
public class StoreClientPerfTest {
  private static final Logger LOGGER = Logger.getLogger(StoreClientPerfTest.class);
  private MockD2ServerWrapper routerServer;

  private String storeName = "test_store";
  private String defaultKeySchemaStr = "\"string\"";

  private D2Client d2Client;

  @BeforeTest
  public void setUp() throws Exception {
    routerServer = ServiceFactory.getMockD2Server("Mock-router-server");
    // d2 based client
    d2Client = D2TestUtils.getAndStartD2Client(routerServer.getZkAddress());
  }

  @AfterTest
  public void cleanUp() throws Exception {
    routerServer.close();
  }

  private void setupSchemaAndRequest(int valueSchemaId, String valueSchemaStr) throws IOException {
    routerServer.clearResponseMapping();
    // Push key schema: string
    FullHttpResponse schemaResponse = StoreClientTestUtils.constructHttpSchemaResponse(storeName, 1, defaultKeySchemaStr);
    String keySchemaPath = "/" + RouterBackedSchemaReader.TYPE_KEY_SCHEMA + "/" + storeName;
    routerServer.addResponseForUri(keySchemaPath, schemaResponse);
    String clusterDiscoveryPath = "/"+ D2ServiceDiscovery.TYPE_D2_SERVICE_DISCOVERY+"/"+storeName;

    routerServer.addResponseForUri(clusterDiscoveryPath, StoreClientTestUtils.constructHttpClusterDiscoveryResponse(storeName, "test_cluster", D2TestUtils.DEFAULT_TEST_SERVICE_NAME));

    Map<Integer, String> valueSchemaEntries = new HashMap<>();
    valueSchemaEntries.put(valueSchemaId, valueSchemaStr);

    // Push value schema
    FullHttpResponse valueSchemaResponse = StoreClientTestUtils.constructHttpSchemaResponse(storeName, valueSchemaId, valueSchemaStr);
    String valueSchemaPath = "/" + RouterBackedSchemaReader.TYPE_VALUE_SCHEMA + "/" + storeName + "/" + valueSchemaId;
    routerServer.addResponseForUri(valueSchemaPath, valueSchemaResponse);
    FullHttpResponse multiValueSchemaResponse = StoreClientTestUtils.constructHttpMultiSchemaResponse(storeName, valueSchemaEntries);
    String multiValueSchemaPath = "/" + RouterBackedSchemaReader.TYPE_VALUE_SCHEMA + "/" + storeName;
    routerServer.addResponseForUri(multiValueSchemaPath, multiValueSchemaResponse);
  }

  /**
   * Disabled because perf testing is not a functional test. Not relevant in the standard test suite.
   */
  @Test(enabled = false)
  public void clientStressTest() throws InterruptedException, ExecutionException, IOException {
    ClientConfig baseClientConfig = ClientConfig
        .defaultGenericClientConfig(storeName)
        .setD2ServiceName(D2TestUtils.DEFAULT_TEST_SERVICE_NAME)
        .setD2Client(d2Client);

    // Test variables
    int[] concurrentCallsPerBatchArray = new int[]{1, 2, 10};
    boolean[] yesAndNo = new boolean[]{true, false};

    int totalTests = yesAndNo.length * 2 * concurrentCallsPerBatchArray.length;
    List<ResultsContainer> resultsContainers = new ArrayList<>();
    int testNumber = 1;
    boolean warmedUp = false;

    for (boolean compute: yesAndNo) {
      for (boolean fastAvro : yesAndNo) {
        for (int concurrentCallsPerBatch : concurrentCallsPerBatchArray) {
          MetricsRepository metricsRepository = new MetricsRepository();
          ClientConfig newClientConfig = ClientConfig.cloneConfig(baseClientConfig)
              .setUseFastAvro(fastAvro)
              .setMetricsRepository(metricsRepository);
          if (!warmedUp) {
            ClientConfig warmUpConfig = ClientConfig.cloneConfig(newClientConfig)
                // Throw-away metrics repo, just to avoid double-registering metrics
                .setMetricsRepository(new MetricsRepository());
            LOGGER.info("\n\n");
            LOGGER.info("Warm up test.\n\n");
            clientStressTest(warmUpConfig, concurrentCallsPerBatch, compute);
            warmedUp = true;
            LOGGER.info("\n\n");
            LOGGER.info("Warm up finished. Beginning real tests now.\n\n");
          }
          LOGGER.info("\n\n");
          LOGGER.info("Test " + testNumber + "/" + totalTests + "\n\n");
          resultsContainers.add(clientStressTest(newClientConfig, concurrentCallsPerBatch, compute));
          testNumber++;
          LOGGER.info("\n\n");
          LOGGER.info("Finished "
              + (compute ? "compute" : "batch get") + " requests"
              + " with" + (fastAvro ? "" : "out") + " fast-avro"
              + " at " + concurrentCallsPerBatch + " concurrentCallsPerBatch."
              + " All results so far:\n\n");
          printCSV(resultsContainers);
        }
      }
    }
  }

  private void printCSV(List<ResultsContainer> resultsContainers) {

    StringBuilder sb = new StringBuilder();
    sb.append("\n\nCSV output:\n\n\n");
    sb.append("Request type,");
    sb.append("Fast Avro,");
    sb.append("Max concurrent queries,");
    sb.append("Batch get deserializer,");
    sb.append("Envelope iterable impl,");
    sb.append("Total queries,");
    sb.append("Throughput,");
    sb.append("Request serialization time Avg,");
    sb.append("Request serialization time p50,");
    sb.append("Request serialization time p99,");
    sb.append("Request submission to response time Avg,");
    sb.append("Request submission to response time p50,");
    sb.append("Request submission to response time p99,");
    sb.append("Response deserialization time Avg,");
    sb.append("Response deserialization time p50,");
    sb.append("Response deserialization time p99,");
    sb.append("Response envelope deserialization time Avg,");
    sb.append("Response envelope deserialization time p50,");
    sb.append("Response envelope deserialization time p99,");
    sb.append("Response records deserialization time Avg,");
    sb.append("Response records deserialization time p50,");
    sb.append("Response records deserialization time p99,");
    sb.append("Response records deserialization submission time Avg,");
    sb.append("Response records deserialization submission time p50,");
    sb.append("Response records deserialization submission time p99,");
    sb.append("Latency Avg,");
    sb.append("Latency p50,");
    sb.append("Latency p77,");
    sb.append("Latency p90,");
    sb.append("Latency p95,");
    sb.append("Latency p99,");
    sb.append("Latency p99.9\n");

    for (ResultsContainer resultsContainer: resultsContainers) {
      boolean first = true;
      for (Object object: resultsContainer) {
        if (first) {
          first = false;
        } else {
          sb.append(",");
        }
        sb.append(object.toString());
      }
      sb.append("\n");
    }

    LOGGER.info(sb.toString());
  }

  private static class TestComputeRequestBuilder extends AvroComputeRequestBuilderV3<String> {
    public TestComputeRequestBuilder(Schema latestValueSchema, InternalAvroStoreClient storeClient,
        Optional<ClientStats> stats, Optional<ClientStats> streamingStats, long preRequestTimeInNS) {
      super(latestValueSchema, storeClient, stats, streamingStats);
    }

    public Pair<Schema, String> getResultSchema() {
      return super.getResultSchema();
    }
  }

  private ResultsContainer clientStressTest(ClientConfig clientConfig, int numberOfConcurrentCallsPerBatch, boolean compute) throws IOException, ExecutionException, InterruptedException {
    try (AvroGenericStoreClient<String, GenericRecord> client = ClientFactory.getAndStartGenericAvroClient(clientConfig)) {
      MetricsRepository metricsRepository = clientConfig.getMetricsRepository();

      int valueSchemaId = 1;
      int valueSizeInBytes = 800;
      String valueSchemaStr = TestPushUtils.USER_SCHEMA_WITH_A_FLOAT_ARRAY_STRING;
      Schema valueSchema = new Schema.Parser().parse(valueSchemaStr);
      Set<String> keys = new HashSet<>();
      setupSchemaAndRequest(valueSchemaId, valueSchemaStr);
      // Construct response

      RecordSerializer<Object> valueSerializer = SerializerDeserializerFactory.getAvroGenericSerializer(Schema.parse(valueSchemaStr));
      List<MultiGetResponseRecordV1> records = new ArrayList<>();

      TestComputeRequestBuilder testComputeRequestBuilder = new TestComputeRequestBuilder(
          client.getLatestValueSchema(),
          (InternalAvroStoreClient) client,
          Optional.empty(),
          Optional.empty(),
          0);
      Collection<String> fieldNames = valueSchema.getFields().stream()
          .map(field -> field.name())
          .collect(Collectors.toList());
      testComputeRequestBuilder.project(fieldNames);
      Pair<Schema, String> computeResultSchemaPair = testComputeRequestBuilder.getResultSchema();
      Schema computeResultSchema = computeResultSchemaPair.getFirst();
      RecordSerializer<Object> computeResultSerializer = SerializerDeserializerFactory.getAvroGenericSerializer(computeResultSchema);
      RecordDeserializer<Object> computeResultDeserializer = SerializerDeserializerFactory.getAvroGenericDeserializer(computeResultSchema);
      List<ComputeResponseRecordV1> computeRecords = new ArrayList<>();
      LOGGER.debug("computeResultSchema : \n" + computeResultSchema.toString(true));

      for (int k = 0; k < 1000; k++) {
        MultiGetResponseRecordV1 dataRecord = new MultiGetResponseRecordV1();
        dataRecord.keyIndex = k;
        dataRecord.schemaId = valueSchemaId;
        dataRecord.value = ByteBuffer.wrap(valueSerializer.serialize(TestPushUtils.getRecordWithFloatArray(valueSchema, k, valueSizeInBytes)));
        records.add(dataRecord);

        ComputeResponseRecordV1 computeRecord = new ComputeResponseRecordV1();
        computeRecord.keyIndex = k;
        GenericRecord computeResultRecord = TestPushUtils.getRecordWithFloatArray(computeResultSchema, k, valueSizeInBytes);
        computeResultRecord.put(VeniceConstants.VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME, new HashMap<String, String>());
        if (k == 0) {
           LOGGER.debug("computeResultRecord: " + computeResultRecord.toString());
        }
        computeRecord.value = ByteBuffer.wrap(computeResultSerializer.serialize(computeResultRecord));
        computeRecords.add(computeRecord);

        // Just to see if Avro will choke on it...
        GenericRecord deserializedComputeResultRecord = (GenericRecord) computeResultDeserializer.deserialize(computeRecord.value);
        Assert.assertEquals(deserializedComputeResultRecord, computeResultRecord);

        keys.add("key" + k);
      }

      // Serialize MultiGetResponse
      RecordSerializer<MultiGetResponseRecordV1> responseSerializer = SerializerDeserializerFactory.getAvroGenericSerializer(MultiGetResponseRecordV1.SCHEMA$);
      byte[] responseBytes = responseSerializer.serializeObjects(records);
      int responseSchemaId = 1;
      FullHttpResponse httpResponse = StoreClientTestUtils.constructStoreResponse(responseSchemaId, responseBytes);
      routerServer.addResponseForUri("/" + AbstractAvroStoreClient.TYPE_STORAGE + "/" + storeName, httpResponse);

      // Serialize ComputeResponse
      RecordSerializer<ComputeResponseRecordV1> computeSerializer = SerializerDeserializerFactory.getAvroGenericSerializer(ComputeResponseRecordV1.SCHEMA$);
      byte[] computeResponseBytes = computeSerializer.serializeObjects(computeRecords);
      FullHttpResponse computeHttpResponse = StoreClientTestUtils.constructStoreResponse(responseSchemaId, computeResponseBytes);
      routerServer.addResponseForUri("/" + AbstractAvroStoreClient.TYPE_COMPUTE+ "/" + storeName, computeHttpResponse);

      // Batch-get

      int keysPerCall = 1000;
      int numberOfBatchesOfConcurrentCalls = 1000;
      int numberOfCalls = 10000; // numberOfConcurrentCallsPerBatch * numberOfBatchesOfConcurrentCalls;
      CompletableFuture[] futures = new CompletableFuture[numberOfConcurrentCallsPerBatch];
      long firstQueryStartTime = System.currentTimeMillis();
      AtomicInteger errors = new AtomicInteger(0);
      AtomicInteger success = new AtomicInteger(0);

      ResultsContainer r = new ResultsContainer();

      LOGGER.info("");
      LOGGER.info("=============================================================================================");
      LOGGER.info("Request Type:           " + r.put(compute ? "compute" : "batch-get"));
      LOGGER.info("Fast Avro:              " + r.put(clientConfig.isUseFastAvro()));
      LOGGER.info("Max concurrent queries: " + r.put(numberOfConcurrentCallsPerBatch));
      LOGGER.info("Total queries:          " + r.put(numberOfCalls));
      LOGGER.info("keys/query:             " + keysPerCall);
      LOGGER.info("bytes/value:            " + valueSizeInBytes);
      LOGGER.info("");

      ComputeRequestBuilder<String> computeRequestBuilder = client.compute().project(fieldNames);
      for (int call = 1; call <= numberOfCalls; call++) {
        CompletableFuture<Map<String, GenericRecord>> future;
        if (compute) {
          future = computeRequestBuilder.execute(keys);
        } else {
          future = client.batchGet(keys);
        }
        futures[call % numberOfConcurrentCallsPerBatch] = future.handle((o, throwable) -> {
          if (throwable != null) {
            if (errors.getAndIncrement() < 10) {
              // Only log the first few errors
              LOGGER.error("Query error!", throwable);
            }
          } else {
            Assert.assertEquals(o.size(), keysPerCall, "Not enough records returned!");
            success.getAndIncrement();
          }
          return null;
        });
        if (call > 0 && call % numberOfConcurrentCallsPerBatch == 0) {
          CompletableFuture.allOf(futures).get();
        }
      }

      return CompletableFuture.allOf(futures).thenApply(aVoid -> {
        Assert.assertEquals(success.get(), numberOfCalls);
        Assert.assertEquals(errors.get(), 0);

        long allQueriesFinishTime = System.currentTimeMillis();
        double totalQueryTime = allQueriesFinishTime - firstQueryStartTime;
        Map<String, ? extends Metric> metrics = metricsRepository.metrics();
        String metricPrefix = "." + storeName + "--";
        if (compute) {
          metricPrefix += RequestType.COMPUTE.getMetricPrefix();
        } else {
          metricPrefix += RequestType.MULTI_GET.getMetricPrefix();
        }

        Metric requestSerializationTimeMetric = metrics.get(metricPrefix + "request_serialization_time.Avg");
        Metric requestSubmissionToResponseHandlingTimeMetric = metrics.get(metricPrefix + "request_submission_to_response_handling_time.Avg");
        Metric responseDeserializationTimeMetric = metrics.get(metricPrefix + "response_deserialization_time.Avg");
        Metric responseEnvelopeDeserializationTimeMetric = metrics.get(metricPrefix + "response_envelope_deserialization_time.Avg");
        Metric responseRecordsDeserializationTimeMetric = metrics.get(metricPrefix + "response_records_deserialization_time.Avg");
        Metric responseRecordsDeserializationSubmissionToStartTime = metrics.get(metricPrefix + "response_records_deserialization_submission_to_start_time.Avg");

        Metric requestSerializationTimeMetric50 = metrics.get(metricPrefix + "request_serialization_time.50thPercentile");
        Metric requestSubmissionToResponseHandlingTimeMetric50 = metrics.get(metricPrefix + "request_submission_to_response_handling_time.50thPercentile");
        Metric responseDeserializationTimeMetric50 = metrics.get(metricPrefix + "response_deserialization_time.50thPercentile");
        Metric responseEnvelopeDeserializationTimeMetric50 = metrics.get(metricPrefix + "response_envelope_deserialization_time.50thPercentile");
        Metric responseRecordsDeserializationTimeMetric50 = metrics.get(metricPrefix + "response_records_deserialization_time.50thPercentile");
        Metric responseRecordsDeserializationSubmissionToStartTime50 = metrics.get(metricPrefix + "response_records_deserialization_submission_to_start_time.50thPercentile");

        Metric requestSerializationTimeMetric99 = metrics.get(metricPrefix + "request_serialization_time.99thPercentile");
        Metric requestSubmissionToResponseHandlingTimeMetric99 = metrics.get(metricPrefix + "request_submission_to_response_handling_time.99thPercentile");
        Metric responseDeserializationTimeMetric99 = metrics.get(metricPrefix + "response_deserialization_time.99thPercentile");
        Metric responseEnvelopeDeserializationTimeMetric99 = metrics.get(metricPrefix + "response_envelope_deserialization_time.99thPercentile");
        Metric responseRecordsDeserializationTimeMetric99 = metrics.get(metricPrefix + "response_records_deserialization_time.99thPercentile");
        Metric responseRecordsDeserializationSubmissionToStartTime99 = metrics.get(metricPrefix + "response_records_deserialization_submission_to_start_time.99thPercentile");

        Metric latencyMetricAvg = metrics.get(metricPrefix + "healthy_request_latency.Avg");
        Metric latencyMetric50 = metrics.get(metricPrefix + "healthy_request_latency.50thPercentile");
        Metric latencyMetric77 = metrics.get(metricPrefix + "healthy_request_latency.77thPercentile");
        Metric latencyMetric90 = metrics.get(metricPrefix + "healthy_request_latency.90thPercentile");
        Metric latencyMetric95 = metrics.get(metricPrefix + "healthy_request_latency.95thPercentile");
        Metric latencyMetric99 = metrics.get(metricPrefix + "healthy_request_latency.99thPercentile");
        Metric latencyMetric999 = metrics.get(metricPrefix + "healthy_request_latency.99_9thPercentile");
        DecimalFormat decimalFormat = new DecimalFormat("0.0");

        LOGGER.info("Throughput: " + r.put((decimalFormat.format(numberOfCalls / (totalQueryTime / 1000.0)))) + " queries/sec");
        LOGGER.info("");
        LOGGER.info("Request serialization time                       (Avg, p50, p99) : " +
            r.round(requestSerializationTimeMetric) + " ms, \t" +
            r.round(requestSerializationTimeMetric50) + " ms, \t" +
            r.round(requestSerializationTimeMetric99) + " ms.");
        LOGGER.info("Request submission to response time              (Avg, p50, p99) : " +
            r.round(requestSubmissionToResponseHandlingTimeMetric) + " ms, \t" +
            r.round(requestSubmissionToResponseHandlingTimeMetric50) + " ms, \t" +
            r.round(requestSubmissionToResponseHandlingTimeMetric99) + " ms.");
        LOGGER.info("Response deserialization time                    (Avg, p50, p99) : " +
            r.round(responseDeserializationTimeMetric) + " ms, \t" +
            r.round(responseDeserializationTimeMetric50) + " ms, \t" +
            r.round(responseDeserializationTimeMetric99) + " ms.");
        LOGGER.info("Response envelope deserialization time           (Avg, p50, p99) : " +
            r.round(responseEnvelopeDeserializationTimeMetric) + " ms, \t" +
            r.round(responseEnvelopeDeserializationTimeMetric50) + " ms, \t" +
            r.round(responseEnvelopeDeserializationTimeMetric99) + " ms.");
        LOGGER.info("Response records deserialization time            (Avg, p50, p99) : " +
            r.round(responseRecordsDeserializationTimeMetric) + " ms, \t" +
            r.round(responseRecordsDeserializationTimeMetric50) + " ms, \t" +
            r.round(responseRecordsDeserializationTimeMetric99) + " ms.");
        LOGGER.info("Response records deserialization submission time (Avg, p50, p99) : " +
            r.round(responseRecordsDeserializationSubmissionToStartTime) + " ms, \t" +
            r.round(responseRecordsDeserializationSubmissionToStartTime50) + " ms, \t" +
            r.round(responseRecordsDeserializationSubmissionToStartTime99) + " ms.");
        LOGGER.info("Latency                    (Avg, p50, p77, p90, p95, p99, p99.9) : " +
            r.round(latencyMetricAvg) + " ms, \t" +
            r.round(latencyMetric50) + " ms, \t" +
            r.round(latencyMetric77) + " ms, \t" +
            r.round(latencyMetric90) + " ms, \t" +
            r.round(latencyMetric95) + " ms, \t" +
            r.round(latencyMetric99) + " ms, \t" +
            r.round(latencyMetric999) + " ms.");
        LOGGER.info("");

        return r;
      }).get();
    }
  }

  /**
   * A utility class to contain results while also easily printing them out.
   */
  private static class ResultsContainer extends ArrayList<Object> {
    /**
     * @param value to be added into the container.
     * @return the passed in {@param value}
     */
    public Object put(Object value) {
      add(value);
      return value;
    }

    /**
     * @param metric to be added into the container.
     * @return the passed in {@param value}
     */
    public double round(Metric metric) {
      double value = Utils.round(metric.value(), 1);
      add(value);
      return value;
    }
  }
}
