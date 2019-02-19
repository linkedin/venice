package com.linkedin.venice.endToEnd;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.KafkaPushJob;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongBinaryOperator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.hadoop.KafkaPushJob.*;
import static com.linkedin.venice.utils.TestPushUtils.*;

//TODO: write a H2VWrapper that can handle the whole flow

@Test(singleThreaded = true)
public abstract class TestBatch {
  private static final Logger LOGGER = Logger.getLogger(TestBatch.class);
  private static final int TEST_TIMEOUT = 60 * Time.MS_PER_SECOND;
  private static final String STRING_SCHEMA = "\"string\"";

  private VeniceClusterWrapper veniceCluster;

  public abstract VeniceClusterWrapper initializeVeniceCluster();

  @BeforeClass
  public void setup() {
    veniceCluster = initializeVeniceCluster();
  }

  @AfterClass
  public void cleanup() {
    if (veniceCluster != null) {
      veniceCluster.close();
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void storeWithNoVersionThrows400() {

    //Create store
    File inputDir = getTempDataDirectory();
    Schema keySchema = Schema.parse("\"string\"");
    Schema valueSchema = Schema.parse("\"string\"");
    Pair<Schema, Schema> schemas = new Pair<>(keySchema, valueSchema);
    String storeName = TestUtils.getUniqueString("store");
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = defaultH2VProps(veniceCluster, inputDirPath, storeName);
    createStoreForJob(veniceCluster, schemas.getFirst().toString(), schemas.getSecond().toString(), props, false, false);

    //Query store
    try(AvroGenericStoreClient avroClient = ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName)
        .setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      try {
        Object value1 = avroClient.get("key1").get();
        Assert.fail("Single get request on store with no push should fail");
      } catch (Exception e){
        Assert.assertTrue(e.getMessage().contains("Please push data to that store"));
      }

      try {
        Set<String> keys = new HashSet<>();
        keys.add("key2");
        keys.add("key3");
        Object values = avroClient.batchGet(keys).get();
        Assert.fail("Batch get request on store with no push should fail");
      } catch (Exception e){
        Assert.assertTrue(e.getMessage().contains("Please push data to that store"));
      }

    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testDuplicateKey() throws Exception {
    try {
      testStoreWithDuplicateKeys(false);
      Assert.fail();
    } catch (VeniceException e) {
      //push is expected to fail because of duplicate key
    }

    testStoreWithDuplicateKeys(true);
  }

  private void testStoreWithDuplicateKeys(boolean isDuplicateKeyAllowed) throws Exception {
    testBatchStore(inputDir -> {
      Schema recordSchema = writeSimpleAvroFileWithDuplicateKey(inputDir);
      return new Pair<>(recordSchema.getField("id").schema(),
                        recordSchema.getField("name").schema());
        }, props -> {
          if (isDuplicateKeyAllowed) {
            props.setProperty(ALLOW_DUPLICATE_KEY, "true");
          }
        },
        (avroClient, vsonClient, metricsRepository) -> {});
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testCompressingRecord() throws Exception {
    testBatchStore(inputDir -> {
      Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir, false);
      return new Pair<>(recordSchema.getField("id").schema(),
                        recordSchema.getField("name").schema());
    }, properties -> {}, (avroClient, vsonClient, metricsRepository) -> {
      //test single get
      for (int i = 1; i <= 100; i ++) {
        Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "test_name_" + i);
      }

      //test batch get
      for (int i = 0; i < 10; i ++) {
        Set<String> keys = new HashSet<>();
        for (int j = 1; j <= 10; j ++) {
          keys.add(Integer.toString(i * 10 + j));
        }

        Map<CharSequence, CharSequence> values = (Map<CharSequence, CharSequence>) avroClient.batchGet(keys).get();
        Assert.assertEquals(values.size(), 10);

        for (int j = 1; j <= 10; j ++) {
          Assert.assertEquals(values.get(Integer.toString(i * 10 + j)).toString(), "test_name_" + ((i * 10) + j));
        }
      }
    }, new UpdateStoreQueryParams().setCompressionStrategy(CompressionStrategy.GZIP));
  }

  @Test
  public void testIncrementalPush() throws Exception {
    String storeName = testBatchStore(inputDir -> {
      Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
      return new Pair<>(recordSchema.getField("id").schema(), recordSchema.getField("name").schema());
    }, properties -> {
    }, (avroClient, vsonClient, metricsRepository) -> {
      for (int i = 1; i <= 100; i++) {
        Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "test_name_" + i);
      }
    }, new UpdateStoreQueryParams().setIncrementalPushEnabled(true));

    testBatchStore(inputDir -> {
      Schema recordSchema = writeSimpleAvroFileWithUserSchema2(inputDir);
      return new Pair<>(recordSchema.getField("id").schema(), recordSchema.getField("name").schema());
    }, properties -> {
      properties.setProperty(INCREMENTAL_PUSH, "true");
    }, (avroClient, vsonClient, metricsRepository) -> {
      for (int i = 51; i <= 150; i++) {
        Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "test_name_" + (i * 2));
      }
    }, storeName, new UpdateStoreQueryParams().setIncrementalPushEnabled(true));
  }

  //This method will be enabled once offline push monitor's changes get merged.
  //@Test //(timeOut = TEST_TIMEOUT)
  public void testLeaderFollowerStateModel() throws Exception {
    testBatchStore(inputDir -> {
      Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir, false);
      return new Pair<>(recordSchema.getField("id").schema(),
          recordSchema.getField("name").schema());
    }, properties -> {}, (avroClient, vsonClient, metricsRepository) -> {
      //this section will be enabled once VersionFinder's changes get merged
      /*//test single get
      for (int i = 1; i <= 100; i ++) {
        Assert.assertEquals(avroClient.get(Integer.toString(i)).get().toString(), "test_name_" + i);
      }

      //test batch get
      for (int i = 0; i < 10; i ++) {
        Set<String> keys = new HashSet<>();
        for (int j = 1; j <= 10; j ++) {
          keys.add(Integer.toString(i * 10 + j));
        }

        Map<CharSequence, CharSequence> values = (Map<CharSequence, CharSequence>) avroClient.batchGet(keys).get();
        Assert.assertEquals(values.size(), 10);

        for (int j = 1; j <= 10; j ++) {
          Assert.assertEquals(values.get(Integer.toString(i * 10 + j)).toString(), "test_name_" + ((i * 10) + j));
        }
      }*/
    }, new UpdateStoreQueryParams().setLeaderFollowerModel(true));
  }

  protected String testBatchStore(InputFileWriter inputFileWriter, Consumer<Properties> extraProps, H2VValidator dataValidator) throws Exception {
    return testBatchStore(inputFileWriter, extraProps, dataValidator, new UpdateStoreQueryParams());
  }

  private String testBatchStore(InputFileWriter inputFileWriter, Consumer<Properties> extraProps, H2VValidator dataValidator,
      UpdateStoreQueryParams storeParms) throws Exception {
    return testBatchStore(inputFileWriter, extraProps, dataValidator, null, storeParms);
  }

  private String testBatchStore(InputFileWriter inputFileWriter, Consumer<Properties> extraProps, H2VValidator dataValidator,
      String existingStore, UpdateStoreQueryParams storeParms) throws Exception {
    File inputDir = getTempDataDirectory();
    Pair<Schema, Schema> schemas = inputFileWriter.write(inputDir);
    String storeName = Utils.isNullOrEmpty(existingStore) ? TestUtils.getUniqueString("store") : existingStore;

    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = defaultH2VProps(veniceCluster, inputDirPath, storeName);
    extraProps.accept(props);

    if (Utils.isNullOrEmpty(existingStore)) {
      createStoreForJob(veniceCluster.getClusterName(), schemas.getFirst().toString(), schemas.getSecond().toString(), props,
          storeParms);
    }

    KafkaPushJob job = new KafkaPushJob("Test Batch push job", props);
    job.run();

    MetricsRepository metricsRepository = new MetricsRepository();

    AvroGenericStoreClient avroClient = ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName)
        .setVeniceURL(veniceCluster.getRandomRouterURL())
        .setMetricsRepository(metricsRepository) // metrics only available for Avro client...
    );
    AvroGenericStoreClient vsonClient = ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultVsonGenericClientConfig(storeName)
        .setVeniceURL(veniceCluster.getRandomRouterURL())
    );

    dataValidator.validate(avroClient, vsonClient, metricsRepository);

    return storeName;
  }

  interface InputFileWriter {
    Pair<Schema, Schema> write(File inputDir) throws IOException;
  }

  interface H2VValidator {
    void validate(AvroGenericStoreClient avroClient, AvroGenericStoreClient vsonClient, MetricsRepository metricsRepository) throws Exception;
  }

  @Test //(timeOut = TEST_TIMEOUT)
  public void testLargeValues() throws Exception {
    try {
      testStoreWithLargeValues(false);
      Assert.fail("Pushing large values with chunking disabled should fail.");
    } catch (VeniceException e) {
      //push is expected to fail because of large values
    }

    testStoreWithLargeValues(true);
  }

  private void testStoreWithLargeValues(boolean isChunkingAllowed) throws Exception {
    int maxValueSize = 3 * 1024 * 1024; // 3 MB apiece
    int numberOfRecords = 10;
    testBatchStore(inputDir -> {
          Schema recordSchema = writeSimpleAvroFileWithCustomSize(inputDir, numberOfRecords, 0, maxValueSize);
          return new Pair<>(recordSchema.getField("id").schema(),
                            recordSchema.getField("name").schema());
        }, props -> {},
        (avroClient, vsonClient, metricsRepository) -> {
          Set<String> keys = new HashSet(10);

          // Single gets
          for (int i = 0; i < numberOfRecords; i++) {
            int expectedSize = maxValueSize / numberOfRecords * (i + 1);
            String key = new Integer(i).toString();
            keys.add(key);
            char[] chars = new char[expectedSize];
            Arrays.fill(chars, Integer.toString(i).charAt(0));
            String expectedString = new String(chars);
            Utf8 expectedUtf8 = new Utf8(expectedString);

            LOGGER.info("About to query key: " + i);
            Utf8 returnedUtf8Value = (Utf8) avroClient.get(key).get();
            Assert.assertNotNull(returnedUtf8Value, "Avro client returned null value for key: " + key + ".");
            LOGGER.info("Received value of size: " + returnedUtf8Value.length() + " for key: " + key);
            Assert.assertEquals(returnedUtf8Value.toString().substring(0, 1), key, "Avro value does not begin with the expected prefix.");
            Assert.assertEquals(returnedUtf8Value.length(), expectedSize, "Avro value does not have the expected size.");
            Assert.assertEquals(returnedUtf8Value, expectedUtf8, "The entire large value should be filled with the same char: " + key);

            String jsonValue = (String) vsonClient.get(key).get();
            Assert.assertNotNull(jsonValue, "VSON client returned null value for key: " + key + ".");
            Assert.assertEquals(jsonValue.substring(0, 1), key, "VSON value does not begin with the expected prefix.");
            Assert.assertEquals(jsonValue.length(), expectedSize, "VSON value does not have the expected size.");
            Assert.assertEquals(jsonValue, expectedString, "The entire large value should be filled with the same char: " + key);
          }

          // Batch-get
          Map<String, Utf8> utf8Results = (Map<String, Utf8>) avroClient.batchGet(keys).get();
          Map<String, String> jsonResults = (Map<String, String>) vsonClient.batchGet(keys).get();
          for (String key: keys) {
            int i = Integer.parseInt(key);
            int expectedSize = maxValueSize / numberOfRecords * (i + 1);
            char[] chars = new char[expectedSize];
            Arrays.fill(chars, key.charAt(0));
            String expectedString = new String(chars);
            Utf8 expectedUtf8 = new Utf8(expectedString);

            Utf8 returnedUtf8Value = utf8Results.get(key);
            Assert.assertNotNull(returnedUtf8Value, "Avro client returned null value for key: " + key + ".");
            LOGGER.info("Received value of size: " + returnedUtf8Value.length() + " for key: " + key);
            Assert.assertEquals(returnedUtf8Value.toString().substring(0, 1), key, "Avro value does not begin with the expected prefix.");
            Assert.assertEquals(returnedUtf8Value.length(), expectedSize, "Avro value does not have the expected size.");
            Assert.assertEquals(returnedUtf8Value, expectedUtf8, "The entire large value should be filled with the same char: " + key);

            String jsonValue = jsonResults.get(key);
            Assert.assertNotNull(jsonValue, "VSON client returned null value for key: " + key + ".");
            Assert.assertEquals(jsonValue.substring(0, 1), key, "VSON value does not begin with the expected prefix.");
            Assert.assertEquals(jsonValue.length(), expectedSize, "VSON value does not have the expected size.");
            Assert.assertEquals(jsonValue, expectedString, "The entire large value should be filled with the same char: " + key);
          }
        }, new UpdateStoreQueryParams().setChunkingEnabled(isChunkingAllowed));
  }

  @Test(timeOut =  TEST_TIMEOUT)
  public void testRunJobWithSchemaThatContainsUnknownField() throws Exception {
    testBatchStore(inputDir -> {
          Schema recordSchema = writeSchemaWithUnknownFieldIntoAvroFile(inputDir);
          return new Pair<>(recordSchema.getField("key").schema(),
              recordSchema.getField("value").schema());
        }, props -> {
          props.setProperty(KEY_FIELD_PROP, "key");
          props.setProperty(VALUE_FIELD_PROP, "value");
        },
        (avroClient, vsonClient, metricsRepository) -> {
          String schemaWithoutSymbolDocStr = loadFileAsString("SchemaWithoutSymbolDoc.avsc");
          Schema schemaWithoutSymbolDoc = Schema.parse(schemaWithoutSymbolDocStr);
          GenericRecord keyRecord = new GenericData.Record(schemaWithoutSymbolDoc.getField("key").schema());
          keyRecord.put("memberId", (long)1);
          keyRecord.put("source", testRecordType.OFFLINE);
          IndexedRecord value = (IndexedRecord)avroClient.get(keyRecord).get();
          Assert.assertEquals(value.get(0).toString(), "LOGO");
          Assert.assertEquals(value.get(1), 1);

          String schemaWithSymbolDocStr = loadFileAsString("SchemaWithSymbolDoc.avsc");
          Schema schemaWithSymbolDoc = Schema.parse(schemaWithSymbolDocStr);
          GenericRecord keyRecord2 = new GenericData.Record(schemaWithSymbolDoc.getField("key").schema());
          keyRecord2.put("memberId", (long)2);
          keyRecord2.put("source", testRecordType.NEARLINE);
          IndexedRecord value2 = (IndexedRecord)avroClient.get(keyRecord2).get();
          Assert.assertEquals(value2.get(0).toString(), "INDUSTRY");
          Assert.assertEquals(value2.get(1), 2);
        });
  }

  private static class StatCounter extends AtomicLong {
    final long initialValue;
    final LongBinaryOperator accumulator;

    public StatCounter(long initialValue, LongBinaryOperator accumulator) {
      super(initialValue);
      this.initialValue = initialValue;
      this.accumulator = accumulator;
    }
    public void add(long newDataPoint) {
      accumulateAndGet(newDataPoint, accumulator);
    }
    public void reset() {
      set(initialValue);
    }
  }
  private static class MaxLong extends StatCounter {
    public MaxLong() {
      super(Integer.MIN_VALUE, (left, right) -> Math.max(left, right));
    }
  }
  private static class MinLong extends StatCounter {
    public MinLong() {
      super(Integer.MAX_VALUE, (left, right) -> Math.min(left, right));
    }
  }
  private static class TotalLong extends StatCounter {
    public TotalLong() {
      super(0, (left, right) -> left + right);
    }
  }

  @Test(enabled = false) // disabled because performance testing is not very amenable to the unit test environment
  public void stressTestLargeMultiGet() throws Exception {
    int valueSize = 800;
    int numberOfRecords = 100000;
    testBatchStore(inputDir -> {
          Schema recordSchema = writeAvroFileWithManyFloatsAndCustomTotalSize(inputDir, numberOfRecords, valueSize, valueSize);
          return new Pair<>(recordSchema.getField("id").schema(),
              recordSchema.getField("name").schema());
        }, props -> {},
        (avroClient, vsonClient, metricsRepository) -> {
          // Batch-get

          String storeName = avroClient.getStoreName();

          int numberOfBatchesOfConcurrentCalls = 200;
          int numberOfConcurrentCallsPerBatch = 10;
          int numberOfCalls = numberOfConcurrentCallsPerBatch * numberOfBatchesOfConcurrentCalls;
          int keysPerCall = 1000;
          final StatCounter minQueryTimeMs = new MinLong();
          final StatCounter maxQueryTimeMs = new MaxLong();
          final StatCounter totalQueryTimeMs = new TotalLong();
          final StatCounter globalMinQueryTimeMs = new MinLong();
          final StatCounter globalMaxQueryTimeMs = new MaxLong();
          final StatCounter globalTotalQueryTimeMs = new TotalLong();
          CompletableFuture[] futures = new CompletableFuture[numberOfConcurrentCallsPerBatch];
          long firstQueryStartTime = System.currentTimeMillis();
          for (int call = 0; call < numberOfCalls; call++) {
            final int finalCall = call;
            Set<String> keys = new HashSet(keysPerCall);
            for (int key = 0; key < keysPerCall; key++) {
              int keyToQuery = (call * keysPerCall + key) % numberOfRecords;
              keys.add(new Integer(keyToQuery).toString());
            }
            final long startTime = System.nanoTime();
            futures[call % numberOfConcurrentCallsPerBatch] = avroClient.batchGet(keys).thenAccept(o -> {
              long endTime = System.nanoTime();
              Map<String, Utf8> utf8Results = (Map<String, Utf8>) o;
              Assert.assertEquals(utf8Results.size(), keysPerCall, "Not enough records returned!");
              long queryTimeNs = endTime - startTime;
              long queryTimeMs = queryTimeNs / Time.NS_PER_MS;
              LOGGER.info("Call #" + finalCall + ": " + queryTimeNs + " ns (" + queryTimeMs + " ms).");
              minQueryTimeMs.add(queryTimeMs);
              maxQueryTimeMs.add(queryTimeMs);
              totalQueryTimeMs.add(queryTimeMs);
              globalMinQueryTimeMs.add(queryTimeMs);
              globalMaxQueryTimeMs.add(queryTimeMs);
              globalTotalQueryTimeMs.add(queryTimeMs);
            });
            if (call > 0 && call % numberOfConcurrentCallsPerBatch == 0) {
              CompletableFuture.allOf(futures).thenAccept(aVoid -> {
                LOGGER.info("Min query time: " + minQueryTimeMs + " ms.");
                LOGGER.info("Max query time: " + maxQueryTimeMs + " ms.");
                LOGGER.info("Average query time: " + (totalQueryTimeMs.get() / numberOfConcurrentCallsPerBatch) + " ms.");
                minQueryTimeMs.reset();
                maxQueryTimeMs.reset();
                totalQueryTimeMs.reset();
              }).get();
            }
          }
          CompletableFuture.allOf(futures).thenAccept(aVoid -> {
            long allQueriesFinishTime = System.currentTimeMillis();
            double totalQueryTime = allQueriesFinishTime - firstQueryStartTime;
            LOGGER.info("Total query time: " + totalQueryTime + " ms for "
                + numberOfCalls + " total queries in "
                + numberOfBatchesOfConcurrentCalls + " batches of "
                + numberOfConcurrentCallsPerBatch + " calls each.");
            DecimalFormat decimalFormat = new DecimalFormat("0.0");
            LOGGER.info("Throughput (per test metrics): " + decimalFormat.format(numberOfCalls / (totalQueryTime / 1000.0)) + " queries / sec");
            LOGGER.info("Global min query time (per test metrics): " + globalMinQueryTimeMs + " ms.");
            LOGGER.info("Global max query time (per test metrics): " + globalMaxQueryTimeMs + " ms.");
            LOGGER.info("Global average query time (per test metrics): " + (globalTotalQueryTimeMs.get() / numberOfCalls) + " ms.");

            Map<String, ? extends Metric> metrics = metricsRepository.metrics();
            String metricPrefix = "." + storeName + "--" + RequestType.MULTI_GET.getMetricPrefix();

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

            Metric latencyMetric50 = metrics.get(metricPrefix + "healthy_request_latency.50thPercentile");
            Metric latencyMetric90 = metrics.get(metricPrefix + "healthy_request_latency.90thPercentile");
            Metric latencyMetric99 = metrics.get(metricPrefix + "healthy_request_latency.99thPercentile");


            LOGGER.info("Request serialization time                       (Avg, p50, p99) : "
                + Utils.round(requestSerializationTimeMetric.value(), 1) + " ms, \t"
                + Utils.round(requestSerializationTimeMetric50.value(), 1) + " ms, \t"
                + Utils.round(requestSerializationTimeMetric99.value(), 1) + " ms.");
            LOGGER.info("Request submission to response time              (Avg, p50, p99) : "
                + Utils.round(requestSubmissionToResponseHandlingTimeMetric.value(), 1) + " ms, \t"
                + Utils.round(requestSubmissionToResponseHandlingTimeMetric50.value(), 1) + " ms, \t"
                + Utils.round(requestSubmissionToResponseHandlingTimeMetric99.value(), 1) + " ms.");
            LOGGER.info("Response deserialization time                    (Avg, p50, p99) : "
                + Utils.round(responseDeserializationTimeMetric.value(), 1) + " ms, \t"
                + Utils.round(responseDeserializationTimeMetric50.value(), 1) + " ms, \t"
                + Utils.round(responseDeserializationTimeMetric99.value(), 1) + " ms.");
            LOGGER.info("Response envelope deserialization time           (Avg, p50, p99) : "
                + Utils.round(responseEnvelopeDeserializationTimeMetric.value(), 1) + " ms, \t"
                + Utils.round(responseEnvelopeDeserializationTimeMetric50.value(), 1) + " ms, \t"
                + Utils.round(responseEnvelopeDeserializationTimeMetric99.value(), 1) + " ms.");
            LOGGER.info("Response records deserialization time            (Avg, p50, p99) : "
                + Utils.round(responseRecordsDeserializationTimeMetric.value(), 9) + " ms, \t"
                + Utils.round(responseRecordsDeserializationTimeMetric50.value(), 9) + " ms, \t"
                + Utils.round(responseRecordsDeserializationTimeMetric99.value(), 9) + " ms.");
            LOGGER.info("Response records deserialization submission time (Avg, p50, p99) : "
                + Utils.round(responseRecordsDeserializationSubmissionToStartTime.value(), 9) + " ms, \t"
                + Utils.round(responseRecordsDeserializationSubmissionToStartTime50.value(), 9) + " ms, \t"
                + Utils.round(responseRecordsDeserializationSubmissionToStartTime99.value(), 9) + " ms.");
            LOGGER.info("Latency                                          (p50, p90, p99) : "
                + Utils.round(latencyMetric50.value(), 1) + " ms, \t"
                + Utils.round(latencyMetric90.value(), 1) + " ms, \t"
                + Utils.round(latencyMetric99.value(), 1) + " ms.");

          }).get();
        });
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testRunMRJobAndPBNJ() throws Exception {
    testRunPushJobAndPBNJ(false);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testRunMapOnlyJobAndPBNJ() throws Exception {
    testRunPushJobAndPBNJ(true);
  }

  private void testRunPushJobAndPBNJ(boolean mapOnly) throws Exception {
    Utils.thisIsLocalhost();

    File inputDir = getTempDataDirectory();
    Schema recordSchema = writeSimpleAvroFileWithUserSchema(inputDir);
    String inputDirPath = "file:" + inputDir.getAbsolutePath();
    String storeName = TestUtils.getUniqueString("store");
    Properties props = defaultH2VProps(veniceCluster, inputDirPath, storeName);
    if (mapOnly) {
      props.setProperty(KafkaPushJob.VENICE_MAP_ONLY, "true");
    }
    props.setProperty(KafkaPushJob.PBNJ_ENABLE, "true");
    props.setProperty(KafkaPushJob.PBNJ_ROUTER_URL_PROP, veniceCluster.getRandomRouterURL());
    createStoreForJob(veniceCluster, recordSchema, props);

    KafkaPushJob job = new KafkaPushJob("Test push job", props);
    job.run();

    // Verify job properties
    Assert.assertEquals(job.getKafkaTopic(), Version.composeKafkaTopic(storeName, 1));
    Assert.assertEquals(job.getInputDirectory(), inputDirPath);
    String schema = "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";
    Assert.assertEquals(job.getFileSchemaString(), schema);
    Assert.assertEquals(job.getKeySchemaString(), STRING_SCHEMA);
    Assert.assertEquals(job.getValueSchemaString(), STRING_SCHEMA);
    Assert.assertEquals(job.getInputFileDataSize(), 3872);

    // Verify the data in Venice Store
    String routerUrl = veniceCluster.getRandomRouterURL();
    try(AvroGenericStoreClient<String, Object> client =
        ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl))) {
      for (int i = 1; i <= 100; ++i) {
        String expected = "test_name_" + i;
        String actual = client.get(Integer.toString(i)).get().toString(); /* client.get().get() returns a Utf8 object */
        Assert.assertEquals(actual, expected);
      }

      ControllerClient controllerClient = new ControllerClient(veniceCluster.getClusterName(), routerUrl);
      JobStatusQueryResponse jobStatus = controllerClient.queryJobStatus(job.getKafkaTopic());
      Assert.assertEquals(jobStatus.getStatus(), ExecutionStatus.COMPLETED.toString(),
          "After job is complete, status should reflect that");
      // In this test we are allowing the progress to not reach the full capacity, but we still want to make sure
      // that most of the progress has completed
      Assert.assertTrue(jobStatus.getMessagesConsumed()*1.5 > jobStatus.getMessagesAvailable(),
          "Complete job should have progress");
    }
  }
}
