package com.linkedin.venice.producer.online;

import static com.linkedin.venice.ConfigKeys.CLIENT_PRODUCER_CALLBACK_THREAD_COUNT;
import static com.linkedin.venice.ConfigKeys.CLIENT_PRODUCER_SCHEMA_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_PRODUCER_WORKER_COUNT;
import static com.linkedin.venice.ConfigKeys.PUBSUB_PRODUCER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.ConfigKeys.VENICE_SYSTEM_PRODUCER_CALLBACK_THREAD_COUNT;
import static com.linkedin.venice.ConfigKeys.VENICE_SYSTEM_PRODUCER_WORKER_COUNT;
import static com.linkedin.venice.serialization.avro.AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE;
import static com.linkedin.venice.utils.TestWriteUtils.loadFileAsStringQuietlyWithErrorLogged;
import static com.linkedin.venice.writer.VeniceWriter.APP_DEFAULT_LOGICAL_TS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.RandomRecordGenerator;
import com.linkedin.avroutil1.compatibility.RecordGenerationConfig;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.ClientFactoryTestUtils;
import com.linkedin.venice.client.store.InternalAvroStoreClient;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.MultiSchemaIdResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.StoreJSONSerializer;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.meta.BufferReplayPolicy;
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
import com.linkedin.venice.producer.AbstractVeniceProducer;
import com.linkedin.venice.producer.DurableWrite;
import com.linkedin.venice.producer.PartitionedProducerExecutor;
import com.linkedin.venice.producer.VeniceProducer;
import com.linkedin.venice.pubsub.PubSubProducerAdapterContext;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterHook;
import com.linkedin.venice.writer.VeniceWriterOptions;
import com.linkedin.venice.writer.update.UpdateBuilder;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class OnlineVeniceProducerTest {
  private static final ObjectMapper MAPPER = ObjectMapperFactory.getInstance();
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

  private static final GenericRecord mockValue1 = getMockValue(VALUE_SCHEMA_1);
  private static final GenericRecord mockValue2 = getMockValue(VALUE_SCHEMA_2);

  private static final Schema UPDATE_SCHEMA_1 =
      WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(VALUE_SCHEMA_1);
  private static final Schema UPDATE_SCHEMA_2 =
      WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(VALUE_SCHEMA_2);
  private static final Schema UPDATE_SCHEMA_3 =
      WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(VALUE_SCHEMA_3);
  private static final Schema UPDATE_SCHEMA_4 =
      WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(VALUE_SCHEMA_4);

  private static final RecordSerializer<Object> keySerializer = getSerializer(KEY_SCHEMA);
  private static final RecordSerializer<Object> value1Serializer = getSerializer(VALUE_SCHEMA_1);
  private static final RecordSerializer<Object> value2Serializer = getSerializer(VALUE_SCHEMA_2);
  private static final RecordSerializer<Object> update2Serializer = getSerializer(UPDATE_SCHEMA_2);

  private static final String FIELD_NUMBER = "favorite_number";
  private static final String FIELD_COLOR = "favorite_color";
  private static final String FIELD_COMPANY = "favorite_company";

  private static final String TOTAL_OPERATION_METRIC_NAME = ".test_store--write_operation.OccurrenceRate";
  private static final String PUT_OPERATION_METRIC_NAME = ".test_store--put_operation.OccurrenceRate";
  private static final String DELETE_OPERATION_METRIC_NAME = ".test_store--delete_operation.OccurrenceRate";
  private static final String UPDATE_OPERATION_METRIC_NAME = ".test_store--update_operation.OccurrenceRate";
  private static final String SUCCESS_OPERATION_METRIC_NAME = ".test_store--success_write_operation.OccurrenceRate";
  private static final String FAILED_OPERATION_METRIC_NAME = ".test_store--failed_write_operation.OccurrenceRate";
  private static final String PENDING_OPERATION_METRIC_NAME = ".test_store--pending_write_operation.Gauge";

  @BeforeTest
  public void setUp() {
    ClientFactoryTestUtils.setUnitTestMode();
    ClientFactoryTestUtils.resetTransportClientProvider();
  }

  @AfterTest
  public void tearDown() {
    ClientFactoryTestUtils.resetUnitTestMode();
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testConstructor() throws IOException {
    ClientConfig storeClientConfig = configureMocksAndGetStoreConfig(storeName);

    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    Properties backendConfigs = new Properties();
    VeniceProducer producer =
        new TestOnlineVeniceProducer(storeClientConfig, new VeniceProperties(backendConfigs), metricsRepository);
    producer.close();
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testFailRequestTopic() throws IOException {
    VersionCreationResponse versionCreationResponse = new VersionCreationResponse();
    versionCreationResponse.setError("ERROR RESPONSE");

    ClientConfig storeClientConfig =
        configureMocksAndGetStoreConfig(storeName, false, MAPPER.writeValueAsBytes(versionCreationResponse));

    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    Properties backendConfigs = new Properties();
    Assert.assertThrows(
        VeniceException.class,
        () -> new TestOnlineVeniceProducer(storeClientConfig, new VeniceProperties(backendConfigs), metricsRepository));

    // Error response which doesn't deserialize to VersionCreationResponse
    ClientConfig storeClientConfig2 = configureMocksAndGetStoreConfig(
        storeName,
        false,
        versionCreationResponse.getError().getBytes(StandardCharsets.UTF_8));
    Assert.assertThrows(
        VeniceException.class,
        () -> new TestOnlineVeniceProducer(
            storeClientConfig2,
            new VeniceProperties(backendConfigs),
            metricsRepository));
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testPut() throws IOException, ExecutionException, InterruptedException {
    ClientConfig storeClientConfig = configureMocksAndGetStoreConfig(storeName);

    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    Properties backendConfigs = new Properties();
    try (TestOnlineVeniceProducer producer =
        new TestOnlineVeniceProducer(storeClientConfig, new VeniceProperties(backendConfigs), metricsRepository)) {
      ArgumentCaptor<byte[]> keyArg = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<byte[]> valueArg = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<Integer> valueSchemaIdArg = ArgumentCaptor.forClass(int.class);
      ArgumentCaptor<PubSubProducerCallback> producerCallbackArg =
          ArgumentCaptor.forClass(PubSubProducerCallback.class);

      producer.asyncPut("KEY1", mockValue1).get();
      verify(producer.mockVeniceWriter, times(1)).put(
          keyArg.capture(),
          valueArg.capture(),
          valueSchemaIdArg.capture(),
          eq(APP_DEFAULT_LOGICAL_TS),
          producerCallbackArg.capture());

      assertEquals(keySerializer.serialize("KEY1"), keyArg.getValue());
      assertEquals(value1Serializer.serialize(mockValue1), valueArg.getValue());
      assertEquals(1, valueSchemaIdArg.getValue().intValue());

      Assert.assertTrue(metricsRepository.getMetric(TOTAL_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertTrue(metricsRepository.getMetric(PUT_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertTrue(metricsRepository.getMetric(SUCCESS_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertEquals(metricsRepository.getMetric(DELETE_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(UPDATE_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(FAILED_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(PENDING_OPERATION_METRIC_NAME).value(), 0.0);

      producer.asyncPut("KEY2", mockValue2).get();
      verify(producer.mockVeniceWriter, times(2)).put(
          keyArg.capture(),
          valueArg.capture(),
          valueSchemaIdArg.capture(),
          eq(APP_DEFAULT_LOGICAL_TS),
          producerCallbackArg.capture());

      assertEquals(keySerializer.serialize("KEY2"), keyArg.getValue());
      assertEquals(value2Serializer.serialize(mockValue2), valueArg.getValue());
      assertEquals(2, valueSchemaIdArg.getValue().intValue());
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testPutWithLogicalTs() throws IOException, ExecutionException, InterruptedException {
    ClientConfig storeClientConfig = configureMocksAndGetStoreConfig(storeName);

    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    Properties backendConfigs = new Properties();
    try (TestOnlineVeniceProducer producer =
        new TestOnlineVeniceProducer(storeClientConfig, new VeniceProperties(backendConfigs), metricsRepository)) {

      ArgumentCaptor<byte[]> keyArg = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<byte[]> valueArg = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<Integer> valueSchemaIdArg = ArgumentCaptor.forClass(int.class);
      ArgumentCaptor<Long> logicalTsArg = ArgumentCaptor.forClass(long.class);
      ArgumentCaptor<PubSubProducerCallback> producerCallbackArg =
          ArgumentCaptor.forClass(PubSubProducerCallback.class);

      producer.asyncPut(1000, "KEY1", mockValue1).get();
      verify(producer.mockVeniceWriter, times(1)).put(
          keyArg.capture(),
          valueArg.capture(),
          valueSchemaIdArg.capture(),
          logicalTsArg.capture(),
          producerCallbackArg.capture());

      assertEquals(keySerializer.serialize("KEY1"), keyArg.getValue());
      assertEquals(value1Serializer.serialize(mockValue1), valueArg.getValue());
      assertEquals(1, valueSchemaIdArg.getValue().intValue());
      assertEquals(1000, logicalTsArg.getValue().longValue());

      Assert.assertTrue(metricsRepository.getMetric(TOTAL_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertTrue(metricsRepository.getMetric(PUT_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertTrue(metricsRepository.getMetric(SUCCESS_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertEquals(metricsRepository.getMetric(DELETE_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(UPDATE_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(FAILED_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(PENDING_OPERATION_METRIC_NAME).value(), 0.0);

      producer.asyncPut(1002, "KEY2", mockValue2).get();
      verify(producer.mockVeniceWriter, times(2)).put(
          keyArg.capture(),
          valueArg.capture(),
          valueSchemaIdArg.capture(),
          logicalTsArg.capture(),
          producerCallbackArg.capture());

      assertEquals(keySerializer.serialize("KEY2"), keyArg.getValue());
      assertEquals(value2Serializer.serialize(mockValue2), valueArg.getValue());
      assertEquals(2, valueSchemaIdArg.getValue().intValue());
      assertEquals(1002, logicalTsArg.getValue().longValue());

      assertThrowsExceptionFromFuture(VeniceException.class, () -> producer.asyncPut(-5, "KEY1", mockValue1).get());
      verify(producer.mockVeniceWriter, times(2)).put(
          keyArg.capture(),
          valueArg.capture(),
          valueSchemaIdArg.capture(),
          logicalTsArg.capture(),
          producerCallbackArg.capture());
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testPutWithInvalidSchema() throws IOException {
    ClientConfig storeClientConfig = configureMocksAndGetStoreConfig(storeName);

    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    Properties backendConfigs = new Properties();
    try (TestOnlineVeniceProducer producer =
        new TestOnlineVeniceProducer(storeClientConfig, new VeniceProperties(backendConfigs), metricsRepository)) {
      assertThrowsExceptionFromFuture(VeniceException.class, () -> producer.asyncPut("KEY1", true).get());
      assertThrowsExceptionFromFuture(VeniceException.class, () -> producer.asyncPut("KEY1", "random_string").get());
      assertThrowsExceptionFromFuture(VeniceException.class, () -> producer.asyncPut("KEY1", 10).get());
      assertThrowsExceptionFromFuture(VeniceException.class, () -> producer.asyncPut("KEY1", 10L).get());
      assertThrowsExceptionFromFuture(VeniceException.class, () -> producer.asyncPut("KEY1", 1.0).get());
      assertThrowsExceptionFromFuture(VeniceException.class, () -> producer.asyncPut("KEY1", 1.0f).get());
      assertThrowsExceptionFromFuture(
          VeniceException.class,
          () -> producer.asyncPut("KEY1", "bytes".getBytes(StandardCharsets.UTF_8)).get());
      // Test invalid object. This can be an object of any unsupported type. Using "Schema" as the unsupported type
      assertThrowsExceptionFromFuture(VeniceException.class, () -> producer.asyncPut("KEY1", VALUE_SCHEMA_1).get());

      Assert.assertTrue(metricsRepository.getMetric(TOTAL_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertTrue(metricsRepository.getMetric(PUT_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertEquals(metricsRepository.getMetric(SUCCESS_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(DELETE_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(UPDATE_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertTrue(metricsRepository.getMetric(FAILED_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertEquals(metricsRepository.getMetric(PENDING_OPERATION_METRIC_NAME).value(), 0.0);
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testPutWithFailedWrite() throws IOException {
    ClientConfig storeClientConfig = configureMocksAndGetStoreConfig(storeName);

    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    Properties backendConfigs = new Properties();
    try (TestOnlineVeniceProducer producer = new TestOnlineVeniceProducer(
        storeClientConfig,
        new VeniceProperties(backendConfigs),
        metricsRepository,
        true)) {
      assertThrowsExceptionFromFuture(VeniceException.class, () -> producer.asyncPut("KEY1", mockValue1).get());

      Assert.assertTrue(metricsRepository.getMetric(TOTAL_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertTrue(metricsRepository.getMetric(PUT_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertEquals(metricsRepository.getMetric(SUCCESS_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(DELETE_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(UPDATE_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertTrue(metricsRepository.getMetric(FAILED_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertEquals(metricsRepository.getMetric(PENDING_OPERATION_METRIC_NAME).value(), 0.0);
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testDelete() throws IOException, ExecutionException, InterruptedException {
    ClientConfig storeClientConfig = configureMocksAndGetStoreConfig(storeName);

    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    Properties backendConfigs = new Properties();
    try (TestOnlineVeniceProducer producer =
        new TestOnlineVeniceProducer(storeClientConfig, new VeniceProperties(backendConfigs), metricsRepository)) {
      ArgumentCaptor<byte[]> keyArg = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<PubSubProducerCallback> producerCallbackArg =
          ArgumentCaptor.forClass(PubSubProducerCallback.class);

      producer.asyncDelete("KEY1").get();
      verify(producer.mockVeniceWriter, times(1))
          .delete(keyArg.capture(), eq(APP_DEFAULT_LOGICAL_TS), producerCallbackArg.capture());

      assertEquals(keySerializer.serialize("KEY1"), keyArg.getValue());

      Assert.assertTrue(metricsRepository.getMetric(TOTAL_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertTrue(metricsRepository.getMetric(DELETE_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertTrue(metricsRepository.getMetric(SUCCESS_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertEquals(metricsRepository.getMetric(PUT_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(UPDATE_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(FAILED_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(PENDING_OPERATION_METRIC_NAME).value(), 0.0);

      producer.asyncDelete("KEY2").get();
      verify(producer.mockVeniceWriter, times(2))
          .delete(keyArg.capture(), eq(APP_DEFAULT_LOGICAL_TS), producerCallbackArg.capture());

      assertEquals(keySerializer.serialize("KEY2"), keyArg.getValue());
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testDeleteWithLogicalTs() throws IOException, ExecutionException, InterruptedException {
    ClientConfig storeClientConfig = configureMocksAndGetStoreConfig(storeName);

    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    Properties backendConfigs = new Properties();
    try (TestOnlineVeniceProducer producer =
        new TestOnlineVeniceProducer(storeClientConfig, new VeniceProperties(backendConfigs), metricsRepository)) {
      ArgumentCaptor<byte[]> keyArg = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<Long> logicalTsArg = ArgumentCaptor.forClass(long.class);
      ArgumentCaptor<PubSubProducerCallback> producerCallbackArg =
          ArgumentCaptor.forClass(PubSubProducerCallback.class);

      producer.asyncDelete(1000, "KEY1").get();
      verify(producer.mockVeniceWriter, times(1))
          .delete(keyArg.capture(), logicalTsArg.capture(), producerCallbackArg.capture());

      assertEquals(keySerializer.serialize("KEY1"), keyArg.getValue());
      assertEquals(1000, logicalTsArg.getValue().longValue());

      Assert.assertTrue(metricsRepository.getMetric(TOTAL_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertTrue(metricsRepository.getMetric(DELETE_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertTrue(metricsRepository.getMetric(SUCCESS_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertEquals(metricsRepository.getMetric(PUT_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(UPDATE_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(FAILED_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(PENDING_OPERATION_METRIC_NAME).value(), 0.0);

      producer.asyncDelete(1002, "KEY2").get();
      verify(producer.mockVeniceWriter, times(2))
          .delete(keyArg.capture(), logicalTsArg.capture(), producerCallbackArg.capture());

      assertEquals(keySerializer.serialize("KEY2"), keyArg.getValue());
      assertEquals(1002, logicalTsArg.getValue().longValue());

      assertThrowsExceptionFromFuture(VeniceException.class, () -> producer.asyncDelete(-1, "KEY1").get());
      verify(producer.mockVeniceWriter, times(2))
          .delete(keyArg.capture(), logicalTsArg.capture(), producerCallbackArg.capture());
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testDeleteWithFailedWrite() throws IOException {
    ClientConfig storeClientConfig = configureMocksAndGetStoreConfig(storeName);

    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    Properties backendConfigs = new Properties();
    try (TestOnlineVeniceProducer producer = new TestOnlineVeniceProducer(
        storeClientConfig,
        new VeniceProperties(backendConfigs),
        metricsRepository,
        true)) {
      assertThrowsExceptionFromFuture(VeniceException.class, () -> producer.asyncDelete("KEY1").get());

      Assert.assertTrue(metricsRepository.getMetric(TOTAL_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertTrue(metricsRepository.getMetric(DELETE_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertEquals(metricsRepository.getMetric(SUCCESS_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(PUT_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(UPDATE_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertTrue(metricsRepository.getMetric(FAILED_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertEquals(metricsRepository.getMetric(PENDING_OPERATION_METRIC_NAME).value(), 0.0);
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testUpdate() throws IOException, ExecutionException, InterruptedException {
    ClientConfig storeClientConfig = configureMocksAndGetStoreConfig(storeName, true);

    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    Properties backendConfigs = new Properties();
    try (TestOnlineVeniceProducer producer =
        new TestOnlineVeniceProducer(storeClientConfig, new VeniceProperties(backendConfigs), metricsRepository)) {
      ArgumentCaptor<byte[]> keyArg = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<byte[]> updateArg = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<Integer> valueSchemaIdArg = ArgumentCaptor.forClass(int.class);
      ArgumentCaptor<Integer> derivedSchemaIdArg = ArgumentCaptor.forClass(int.class);
      ArgumentCaptor<PubSubProducerCallback> producerCallbackArg =
          ArgumentCaptor.forClass(PubSubProducerCallback.class);

      // Update only fields in UPDATE_SCHEMA_1
      producer.asyncUpdate("KEY1", updateBuilderObj -> {
        UpdateBuilder updateBuilder = ((UpdateBuilder) updateBuilderObj);
        updateBuilder.setNewFieldValue(FIELD_NUMBER, 10L);
        updateBuilder.setNewFieldValue(FIELD_COMPANY, "LinkedIn");
      }).get();
      verify(producer.mockVeniceWriter, times(1)).update(
          keyArg.capture(),
          updateArg.capture(),
          valueSchemaIdArg.capture(),
          derivedSchemaIdArg.capture(),
          producerCallbackArg.capture(),
          eq(APP_DEFAULT_LOGICAL_TS));

      // The update value should still use update schema 2 since we use superset schema
      GenericRecord expectedUpdateValue1 = new GenericData.Record(UPDATE_SCHEMA_2);
      expectedUpdateValue1.put(FIELD_NUMBER, 10L);
      expectedUpdateValue1.put(FIELD_COMPANY, "LinkedIn");
      expectedUpdateValue1.put(FIELD_COLOR, createFieldNoOpRecord(UPDATE_SCHEMA_2, FIELD_COLOR));

      assertEquals(keySerializer.serialize("KEY1"), keyArg.getValue());
      assertEquals(update2Serializer.serialize(expectedUpdateValue1), updateArg.getValue());
      assertEquals(2, valueSchemaIdArg.getValue().intValue());
      assertEquals(1, derivedSchemaIdArg.getValue().intValue());

      // Update field only in UPDATE_SCHEMA_2
      producer.asyncUpdate("KEY2", updateBuilderObj -> {
        UpdateBuilder updateBuilder = ((UpdateBuilder) updateBuilderObj);
        updateBuilder.setNewFieldValue(FIELD_COLOR, "green");
      }).get();
      verify(producer.mockVeniceWriter, times(2)).update(
          keyArg.capture(),
          updateArg.capture(),
          valueSchemaIdArg.capture(),
          derivedSchemaIdArg.capture(),
          producerCallbackArg.capture(),
          eq(APP_DEFAULT_LOGICAL_TS));

      // The update value should still use update schema 2 since we use superset schema
      GenericRecord expectedUpdateValue2 = new GenericData.Record(UPDATE_SCHEMA_2);
      expectedUpdateValue2.put(FIELD_NUMBER, createFieldNoOpRecord(UPDATE_SCHEMA_2, FIELD_NUMBER));
      expectedUpdateValue2.put(FIELD_COMPANY, createFieldNoOpRecord(UPDATE_SCHEMA_2, FIELD_COMPANY));
      expectedUpdateValue2.put(FIELD_COLOR, "green");

      assertEquals(keySerializer.serialize("KEY2"), keyArg.getValue());
      assertEquals(update2Serializer.serialize(expectedUpdateValue2), updateArg.getValue());
      assertEquals(2, valueSchemaIdArg.getValue().intValue());
      assertEquals(1, derivedSchemaIdArg.getValue().intValue());

      Assert.assertTrue(metricsRepository.getMetric(TOTAL_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertTrue(metricsRepository.getMetric(UPDATE_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertTrue(metricsRepository.getMetric(SUCCESS_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertEquals(metricsRepository.getMetric(PUT_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(DELETE_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(FAILED_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(PENDING_OPERATION_METRIC_NAME).value(), 0.0);

      assertThrowsExceptionFromFuture(
          VeniceException.class,
          () -> producer.asyncUpdate(-2, "KEY1", updateBuilderObj -> {}).get());
      verify(producer.mockVeniceWriter, times(2)).update(
          keyArg.capture(),
          updateArg.capture(),
          valueSchemaIdArg.capture(),
          derivedSchemaIdArg.capture(),
          producerCallbackArg.capture(),
          eq(APP_DEFAULT_LOGICAL_TS));
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testUpdateWithLogicalTs() throws IOException, ExecutionException, InterruptedException {
    ClientConfig storeClientConfig = configureMocksAndGetStoreConfig(storeName, true);

    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    Properties backendConfigs = new Properties();
    try (TestOnlineVeniceProducer producer =
        new TestOnlineVeniceProducer(storeClientConfig, new VeniceProperties(backendConfigs), metricsRepository)) {
      ArgumentCaptor<byte[]> keyArg = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<byte[]> updateArg = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<Integer> valueSchemaIdArg = ArgumentCaptor.forClass(int.class);
      ArgumentCaptor<Integer> derivedSchemaIdArg = ArgumentCaptor.forClass(int.class);
      ArgumentCaptor<PubSubProducerCallback> producerCallbackArg =
          ArgumentCaptor.forClass(PubSubProducerCallback.class);
      ArgumentCaptor<Long> logicalTsArg = ArgumentCaptor.forClass(long.class);

      // Update only fields in UPDATE_SCHEMA_1
      producer.asyncUpdate(1000, "KEY1", updateBuilderObj -> {
        UpdateBuilder updateBuilder = ((UpdateBuilder) updateBuilderObj);
        updateBuilder.setNewFieldValue(FIELD_NUMBER, 10L);
        updateBuilder.setNewFieldValue(FIELD_COMPANY, "LinkedIn");
      }).get();
      verify(producer.mockVeniceWriter, times(1)).update(
          keyArg.capture(),
          updateArg.capture(),
          valueSchemaIdArg.capture(),
          derivedSchemaIdArg.capture(),
          producerCallbackArg.capture(),
          logicalTsArg.capture());

      // The update value should still use update schema 2 since we use superset schema
      GenericRecord expectedUpdateValue1 = new GenericData.Record(UPDATE_SCHEMA_2);
      expectedUpdateValue1.put(FIELD_NUMBER, 10L);
      expectedUpdateValue1.put(FIELD_COMPANY, "LinkedIn");
      expectedUpdateValue1.put(FIELD_COLOR, createFieldNoOpRecord(UPDATE_SCHEMA_2, FIELD_COLOR));

      assertEquals(keySerializer.serialize("KEY1"), keyArg.getValue());
      assertEquals(update2Serializer.serialize(expectedUpdateValue1), updateArg.getValue());
      assertEquals(2, valueSchemaIdArg.getValue().intValue());
      assertEquals(1, derivedSchemaIdArg.getValue().intValue());
      assertEquals(1000, logicalTsArg.getValue().longValue());

      Assert.assertTrue(metricsRepository.getMetric(TOTAL_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertTrue(metricsRepository.getMetric(UPDATE_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertTrue(metricsRepository.getMetric(SUCCESS_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertEquals(metricsRepository.getMetric(PUT_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(DELETE_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(FAILED_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(PENDING_OPERATION_METRIC_NAME).value(), 0.0);

      // Update field only in UPDATE_SCHEMA_2
      producer.asyncUpdate(1002, "KEY2", updateBuilderObj -> {
        UpdateBuilder updateBuilder = ((UpdateBuilder) updateBuilderObj);
        updateBuilder.setNewFieldValue(FIELD_COLOR, "green");
      }).get();
      verify(producer.mockVeniceWriter, times(2)).update(
          keyArg.capture(),
          updateArg.capture(),
          valueSchemaIdArg.capture(),
          derivedSchemaIdArg.capture(),
          producerCallbackArg.capture(),
          logicalTsArg.capture());

      // The update value should still use update schema 2 since we use superset schema
      GenericRecord expectedUpdateValue2 = new GenericData.Record(UPDATE_SCHEMA_2);
      expectedUpdateValue2.put(FIELD_NUMBER, createFieldNoOpRecord(UPDATE_SCHEMA_2, FIELD_NUMBER));
      expectedUpdateValue2.put(FIELD_COMPANY, createFieldNoOpRecord(UPDATE_SCHEMA_2, FIELD_COMPANY));
      expectedUpdateValue2.put(FIELD_COLOR, "green");

      assertEquals(keySerializer.serialize("KEY2"), keyArg.getValue());
      assertEquals(update2Serializer.serialize(expectedUpdateValue2), updateArg.getValue());
      assertEquals(2, valueSchemaIdArg.getValue().intValue());
      assertEquals(1, derivedSchemaIdArg.getValue().intValue());
      assertEquals(1002, logicalTsArg.getValue().longValue());
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testUpdateOnUnsupportedStore() throws IOException {
    ClientConfig storeClientConfig = configureMocksAndGetStoreConfig(storeName);

    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    Properties backendConfigs = new Properties();
    try (TestOnlineVeniceProducer producer =
        new TestOnlineVeniceProducer(storeClientConfig, new VeniceProperties(backendConfigs), metricsRepository)) {
      ArgumentCaptor<byte[]> keyArg = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<byte[]> updateArg = ArgumentCaptor.forClass(byte[].class);
      ArgumentCaptor<Integer> valueSchemaIdArg = ArgumentCaptor.forClass(int.class);
      ArgumentCaptor<Integer> derivedSchemaIdArg = ArgumentCaptor.forClass(int.class);
      ArgumentCaptor<PubSubProducerCallback> producerCallbackArg =
          ArgumentCaptor.forClass(PubSubProducerCallback.class);
      ArgumentCaptor<Long> logicalTsArg = ArgumentCaptor.forClass(long.class);

      // Update only fields in UPDATE_SCHEMA_1
      assertThrowsExceptionFromFuture(
          VeniceException.class,
          () -> producer.asyncUpdate(1000, "KEY1", updateBuilderObj -> {
            UpdateBuilder updateBuilder = ((UpdateBuilder) updateBuilderObj);
            updateBuilder.setNewFieldValue(FIELD_NUMBER, 10L);
            updateBuilder.setNewFieldValue(FIELD_COMPANY, "LinkedIn");
          }).get());
      verify(producer.mockVeniceWriter, never()).update(
          keyArg.capture(),
          updateArg.capture(),
          valueSchemaIdArg.capture(),
          derivedSchemaIdArg.capture(),
          producerCallbackArg.capture(),
          logicalTsArg.capture());

      Assert.assertTrue(metricsRepository.getMetric(TOTAL_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertTrue(metricsRepository.getMetric(UPDATE_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertEquals(metricsRepository.getMetric(SUCCESS_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(PUT_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(DELETE_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertTrue(metricsRepository.getMetric(FAILED_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertEquals(metricsRepository.getMetric(PENDING_OPERATION_METRIC_NAME).value(), 0.0);
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testUpdateWithFailedWrite() throws IOException {
    ClientConfig storeClientConfig = configureMocksAndGetStoreConfig(storeName);

    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    Properties backendConfigs = new Properties();
    try (TestOnlineVeniceProducer producer = new TestOnlineVeniceProducer(
        storeClientConfig,
        new VeniceProperties(backendConfigs),
        metricsRepository,
        true)) {
      assertThrowsExceptionFromFuture(
          VeniceException.class,
          () -> producer.asyncUpdate(1000, "KEY1", updateBuilderObj -> {
            UpdateBuilder updateBuilder = ((UpdateBuilder) updateBuilderObj);
            updateBuilder.setNewFieldValue(FIELD_NUMBER, 10L);
            updateBuilder.setNewFieldValue(FIELD_COMPANY, "LinkedIn");
          }).get());

      Assert.assertTrue(metricsRepository.getMetric(TOTAL_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertTrue(metricsRepository.getMetric(UPDATE_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertEquals(metricsRepository.getMetric(SUCCESS_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(PUT_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(DELETE_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertTrue(metricsRepository.getMetric(FAILED_OPERATION_METRIC_NAME).value() > 0.0);
      Assert.assertEquals(metricsRepository.getMetric(PENDING_OPERATION_METRIC_NAME).value(), 0.0);
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testOperationsOnClosedProducer() throws IOException {
    ClientConfig storeClientConfig = configureMocksAndGetStoreConfig(storeName);

    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    Properties backendConfigs = new Properties();
    TestOnlineVeniceProducer producer =
        new TestOnlineVeniceProducer(storeClientConfig, new VeniceProperties(backendConfigs), metricsRepository);
    producer.close();

    assertThrowsExceptionFromFuture(VeniceException.class, () -> producer.asyncPut("KEY1", mockValue1).get());
    assertThrowsExceptionFromFuture(VeniceException.class, () -> producer.asyncPut(1000, "KEY1", mockValue1).get());

    assertThrowsExceptionFromFuture(VeniceException.class, () -> producer.asyncDelete("KEY1").get());
    assertThrowsExceptionFromFuture(VeniceException.class, () -> producer.asyncDelete(1000, "KEY1").get());

    assertThrowsExceptionFromFuture(
        VeniceException.class,
        () -> producer.asyncUpdate("KEY1", updateBuilderObj -> {}).get());
    assertThrowsExceptionFromFuture(
        VeniceException.class,
        () -> producer.asyncUpdate(1000, "KEY1", updateBuilderObj -> {}).get());
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testConcurrentCloseReturnsWithoutRacingOneShotClientCleanup() throws Exception {
    ClientConfig storeClientConfig = configureMocksAndGetStoreConfig(storeName);
    TransportClient mockTransportClient = ClientFactory.getTransportClient(storeClientConfig);
    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    TestOnlineVeniceProducer producer =
        new TestOnlineVeniceProducer(storeClientConfig, VeniceProperties.empty(), metricsRepository);
    CountDownLatch writerCloseEntered = new CountDownLatch(1);
    CountDownLatch releaseWriterClose = new CountDownLatch(1);
    AtomicBoolean firstWriterClose = new AtomicBoolean(true);
    doAnswer(invocation -> {
      if (firstWriterClose.compareAndSet(true, false)) {
        writerCloseEntered.countDown();
        releaseWriterClose.await();
      }
      return null;
    }).when(producer.mockVeniceWriter).close();

    CompletableFuture<Void> firstClose = null;
    CompletableFuture<Void> concurrentClose = null;
    CountDownLatch concurrentCloseInvoked = new CountDownLatch(1);
    try {
      firstClose =
          CompletableFuture.runAsync(() -> closeUnchecked(producer), dedicatedThreadExecutor("test-first-close"));
      assertTrue(writerCloseEntered.await(5, TimeUnit.SECONDS));
      concurrentClose = CompletableFuture.runAsync(() -> {
        concurrentCloseInvoked.countDown();
        closeUnchecked(producer);
      }, dedicatedThreadExecutor("test-concurrent-close"));
      assertTrue(concurrentCloseInvoked.await(5, TimeUnit.SECONDS));
      concurrentClose.get(5, TimeUnit.SECONDS);
      verify(mockTransportClient, never()).close();
    } finally {
      releaseWriterClose.countDown();
      awaitQuietly(firstClose);
      awaitQuietly(concurrentClose);
    }
    firstClose.get(5, TimeUnit.SECONDS);

    verify(producer.mockVeniceWriter, times(1)).close();
    verify(mockTransportClient, atLeastOnce()).close();
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testCallbackContinuationCloseReturnsBeforeExecutorTerminates() throws Exception {
    ClientConfig storeClientConfig = configureMocksAndGetStoreConfig(storeName);
    TransportClient mockTransportClient = ClientFactory.getTransportClient(storeClientConfig);
    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    Properties backendConfigs = new Properties();
    backendConfigs.setProperty(CLIENT_PRODUCER_WORKER_COUNT, "1");
    backendConfigs.setProperty(CLIENT_PRODUCER_CALLBACK_THREAD_COUNT, "1");
    TestOnlineVeniceProducer producer =
        new TestOnlineVeniceProducer(storeClientConfig, new VeniceProperties(backendConfigs), metricsRepository);
    CompletableFuture<PubSubProducerCallback> pubSubCallback = new CompletableFuture<>();
    doAnswer(invocation -> {
      pubSubCallback.complete(invocation.getArgument(4));
      return null;
    }).when(producer.mockVeniceWriter).put(any(), any(), anyInt(), anyLong(), any());

    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
    CompletableFuture<Void> closeReturned = new CompletableFuture<>();
    CountDownLatch releaseCallbackTask = new CountDownLatch(1);
    AtomicBoolean closeRanOnCallbackExecutor = new AtomicBoolean();
    CompletableFuture<Void> callbackTask = null;
    try {
      CompletableFuture<DurableWrite> durableWrite = producer.asyncPut("KEY1", mockValue1);
      PubSubProducerCallback callback = pubSubCallback.get(remainingNanos(deadlineNanos), TimeUnit.NANOSECONDS);
      callbackTask = durableWrite.thenRun(() -> {
        closeRanOnCallbackExecutor.set(producer.getCapturedDispatcher().isCurrentThreadExecutingCallback());
        try {
          closeUnchecked(producer);
          closeReturned.complete(null);
        } catch (Throwable throwable) {
          closeReturned.completeExceptionally(throwable);
        }
        try {
          releaseCallbackTask.await();
        } catch (InterruptedException exception) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(exception);
        }
      });
      callback.onCompletion(null, null);

      closeReturned.get(remainingNanos(deadlineNanos), TimeUnit.NANOSECONDS);
      assertTrue(closeRanOnCallbackExecutor.get(), "The continuation must execute on the callback executor");
      Assert.assertFalse(
          producer.getCapturedDispatcher().awaitCallbackTermination(0, TimeUnit.NANOSECONDS),
          "The callback executor cannot terminate while its current task is explicitly blocked");
    } finally {
      releaseCallbackTask.countDown();
      closeUnchecked(producer);
    }
    callbackTask.get(remainingNanos(deadlineNanos), TimeUnit.NANOSECONDS);
    assertTrue(
        producer.getCapturedDispatcher().awaitCallbackTermination(remainingNanos(deadlineNanos), TimeUnit.NANOSECONDS));
    verify(producer.mockVeniceWriter, times(1)).close();
    verify(mockTransportClient, atLeastOnce()).close();
  }

  @Test(timeOut = 10000)
  public void testDeferredCompletionFailureClosesClientsOnceAndRemainsSticky() throws Exception {
    ClientConfig storeClientConfig = configureMocksAndGetStoreConfig(storeName + "-deferred-failure");
    TestOnlineVeniceProducer producer = new TestOnlineVeniceProducer(
        storeClientConfig,
        VeniceProperties.empty(),
        MetricsRepositoryUtils.createSingleThreadedMetricsRepository());
    ClientResourceMocks clientResources = replaceClientResourcesWithMocks(producer);
    RuntimeException deferredFailure = new RuntimeException("deferred completion failed");
    IOException schemaCloseFailure = new IOException("schema reader close failed");
    RuntimeException storeClientCloseFailure = new RuntimeException("store client close failed");
    Mockito.doThrow(schemaCloseFailure).when(clientResources.schemaReader).close();
    Mockito.doThrow(storeClientCloseFailure).when(clientResources.storeClient).close();
    CountDownLatch deferredCompletionRan = new CountDownLatch(1);
    try {
      producer.scheduleDeferredFailure(deferredFailure, deferredCompletionRan);
      assertTrue(deferredCompletionRan.await(5, TimeUnit.SECONDS));

      IOException firstCloseFailure = Assert.expectThrows(IOException.class, producer::close);
      IOException secondCloseFailure = Assert.expectThrows(IOException.class, producer::close);

      Assert.assertSame(firstCloseFailure.getCause(), deferredFailure);
      Assert.assertSame(secondCloseFailure, firstCloseFailure);
      Assert.assertEquals(
          firstCloseFailure.getSuppressed(),
          new Throwable[] { schemaCloseFailure, storeClientCloseFailure });
      verify(clientResources.schemaReader, times(1)).close();
      verify(clientResources.storeClient, times(1)).close();
      verify(producer.mockVeniceWriter, times(1)).close();
    } finally {
      clientResources.closeOriginalResources();
    }
  }

  @Test(timeOut = 10000)
  public void testLateDeferredFailureAfterReentrantCloseRemainsVisibleAndSticky() throws Exception {
    ClientConfig storeClientConfig = configureMocksAndGetStoreConfig(storeName + "-late-deferred-failure");
    TestOnlineVeniceProducer producer = new TestOnlineVeniceProducer(
        storeClientConfig,
        VeniceProperties.empty(),
        MetricsRepositoryUtils.createSingleThreadedMetricsRepository());
    ClientResourceMocks clientResources = replaceClientResourcesWithMocks(producer);
    RuntimeException deferredFailure = new RuntimeException("failure after reentrant close");
    CountDownLatch reentrantCloseReturned = new CountDownLatch(1);
    CountDownLatch deferredCompletionFinished = new CountDownLatch(1);
    try {
      producer.scheduleCloseThenFailure(deferredFailure, reentrantCloseReturned, deferredCompletionFinished);
      assertTrue(reentrantCloseReturned.await(5, TimeUnit.SECONDS));
      assertTrue(deferredCompletionFinished.await(5, TimeUnit.SECONDS));

      IOException firstObservedFailure = Assert.expectThrows(IOException.class, producer::close);
      IOException secondObservedFailure = Assert.expectThrows(IOException.class, producer::close);

      Assert.assertSame(firstObservedFailure.getCause(), deferredFailure);
      Assert.assertSame(secondObservedFailure, firstObservedFailure);
      verify(clientResources.schemaReader, times(1)).close();
      verify(clientResources.storeClient, times(1)).close();
      verify(producer.mockVeniceWriter, times(1)).close();
    } finally {
      clientResources.closeOriginalResources();
    }
  }

  @Test(timeOut = 10000)
  public void testWorkerTerminationFailureKeepsClientsOpen() throws Exception {
    ClientConfig storeClientConfig = configureMocksAndGetStoreConfig(storeName + "-worker-failure");
    WorkerTerminationFailingOnlineVeniceProducer producer = new WorkerTerminationFailingOnlineVeniceProducer(
        storeClientConfig,
        VeniceProperties.empty(),
        MetricsRepositoryUtils.createSingleThreadedMetricsRepository());
    ClientResourceMocks clientResources = replaceClientResourcesWithMocks(producer);
    try {
      Assert.expectThrows(IOException.class, producer::close);
      verify(clientResources.schemaReader, never()).close();
      verify(clientResources.storeClient, never()).close();

      producer.close();
      verify(clientResources.schemaReader, times(1)).close();
      verify(clientResources.storeClient, times(1)).close();
    } finally {
      Utils.closeQuietlyWithErrorLogged(producer);
      clientResources.closeOriginalResources();
    }
  }

  @Test(timeOut = 10000)
  public void testCallbackTerminationFailureKeepsClientsOpen() throws Exception {
    ClientConfig storeClientConfig = configureMocksAndGetStoreConfig(storeName + "-callback-failure");
    CallbackTerminationFailingOnlineVeniceProducer producer = new CallbackTerminationFailingOnlineVeniceProducer(
        storeClientConfig,
        VeniceProperties.empty(),
        MetricsRepositoryUtils.createSingleThreadedMetricsRepository());
    ClientResourceMocks clientResources = replaceClientResourcesWithMocks(producer);
    try {
      Assert.expectThrows(IOException.class, producer::close);
      verify(clientResources.schemaReader, never()).close();
      verify(clientResources.storeClient, never()).close();

      producer.close();
      verify(clientResources.schemaReader, times(1)).close();
      verify(clientResources.storeClient, times(1)).close();
    } finally {
      Utils.closeQuietlyWithErrorLogged(producer);
      clientResources.closeOriginalResources();
    }
  }

  private static long remainingNanos(long deadlineNanos) {
    return Math.max(1, deadlineNanos - System.nanoTime());
  }

  private static Executor dedicatedThreadExecutor(String threadName) {
    return task -> {
      Thread thread = new Thread(task, threadName);
      thread.setDaemon(true);
      thread.start();
    };
  }

  private static void awaitQuietly(CompletableFuture<?> future) {
    if (future == null) {
      return;
    }
    try {
      future.handle((ignored, failure) -> null).get(5, TimeUnit.SECONDS);
    } catch (Throwable ignored) {
      // Preserve the primary test failure while making a best effort to terminate test work.
    }
  }

  private static void closeUnchecked(VeniceProducer<?, ?> producer) {
    try {
      producer.close();
    } catch (IOException exception) {
      throw new RuntimeException(exception);
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testConcurrentEnsureSchemaRefreshed() throws IOException, ExecutionException, InterruptedException {
    boolean updateEnabled = true;
    ClientConfig storeClientConfig = configureMocksAndGetStoreConfig(storeName, updateEnabled);
    TransportClient mockTransportClient = ClientFactory.getTransportClient(storeClientConfig);
    configureMockTransportClient(mockTransportClient, updateEnabled, null, 500);

    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    Properties backendConfigs = new Properties();

    // Should be high enough to not get triggered during the test as it might end up fetching the schemas instead
    backendConfigs.put(CLIENT_PRODUCER_SCHEMA_REFRESH_INTERVAL_SECONDS, 2 * Time.MS_PER_MINUTE);
    try (VeniceProducer producer =
        new TestOnlineVeniceProducer(storeClientConfig, new VeniceProperties(backendConfigs), metricsRepository)) {
      CompletableFuture<DurableWrite> future1 = producer.asyncUpdate(1000, "KEY1", updateBuilderObj -> {
        UpdateBuilder updateBuilder = ((UpdateBuilder) updateBuilderObj);
        updateBuilder.setNewFieldValue(FIELD_COLOR, "green");
        Assert.assertEquals(updateBuilder.build().getSchema().toString(), UPDATE_SCHEMA_2.toString());
      });

      CompletableFuture<DurableWrite> future2 = producer.asyncUpdate(1000, "KEY2", updateBuilderObj -> {
        UpdateBuilder updateBuilder = ((UpdateBuilder) updateBuilderObj);
        updateBuilder.setNewFieldValue(FIELD_COLOR, "red");
      });

      /**
       * Before this fix, one of these {@code asyncUpdate} call would think that update schemas had already been fetched
       * and because it wouldn't find the update schemas themselves, the future would return exceptionally with:
       * {@literal Update schema not found. Check if partial update is enabled for the store...}
       */
      future1.get();
      future2.get();
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testFetchLatestValueAndUpdateSchemas() throws IOException, ExecutionException, InterruptedException {
    ClientConfig storeClientConfig = configureMocksAndGetStoreConfig(storeName, true);

    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    Properties backendConfigs = new Properties();
    backendConfigs.put(CLIENT_PRODUCER_SCHEMA_REFRESH_INTERVAL_SECONDS, 1);
    try (VeniceProducer producer =
        new TestOnlineVeniceProducer(storeClientConfig, new VeniceProperties(backendConfigs), metricsRepository)) {
      producer.asyncUpdate(1000, "KEY1", updateBuilderObj -> {
        UpdateBuilder updateBuilder = ((UpdateBuilder) updateBuilderObj);
        updateBuilder.setNewFieldValue(FIELD_COLOR, "green");
        Assert.assertEquals(updateBuilder.build().getSchema().toString(), UPDATE_SCHEMA_2.toString());
      }).get();

      // Register 2 new value schemas with one of them as a new superset schema
      configureSchemaResponseMocks(
          ClientFactory.getTransportClient(storeClientConfig),
          Arrays.asList(VALUE_SCHEMA_1, VALUE_SCHEMA_2, VALUE_SCHEMA_3, VALUE_SCHEMA_4),
          3,
          Arrays.asList(UPDATE_SCHEMA_1, UPDATE_SCHEMA_2, UPDATE_SCHEMA_3, UPDATE_SCHEMA_4),
          true,
          0,
          storeName);
      // Wait for at least one schema refresh cycle to pick up the new schemas
      Utils.sleep(2000);
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
        try {
          producer.asyncUpdate(1000, "KEY1", updateBuilderObj -> {
            UpdateBuilder updateBuilder = ((UpdateBuilder) updateBuilderObj);
            updateBuilder.setNewFieldValue(FIELD_COLOR, "green");
            Assert.assertEquals(updateBuilder.build().getSchema().toString(), UPDATE_SCHEMA_3.toString());
          }).get();
        } catch (ExecutionException e) {
          Assert.fail("asyncUpdate threw ExecutionException: " + e.getCause());
        }
      });
    }
  }

  /**
   * Verifies that operations on the SAME key are executed in submission order,
   * which is the core guarantee of partition-based workers. Operations on
   * DIFFERENT keys may execute in any order (parallel workers).
   *
   * <p>This test uses multiple keys to validate that:
   * <ul>
   *   <li>Per-key ordering is maintained (operations on same key execute in submission order)</li>
   *   <li>Cross-key operations can execute concurrently (different keys go to different workers)</li>
   * </ul>
   */
  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testWriteOperationsExecuteInOrderPerKey() throws IOException, ExecutionException, InterruptedException {
    ClientConfig storeClientConfig = configureMocksAndGetStoreConfig(storeName);

    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    Properties backendConfigs = new Properties();
    // Use multiple workers to enable parallel processing of different keys
    backendConfigs.put(CLIENT_PRODUCER_WORKER_COUNT, 4);

    try (TestOnlineVeniceProducer producer =
        new TestOnlineVeniceProducer(storeClientConfig, new VeniceProperties(backendConfigs), metricsRepository)) {
      // Track the order of write operations PER KEY
      // Partition-based workers guarantee per-key ordering, not global ordering
      Map<String, List<String>> writeOrderByKey = new java.util.concurrent.ConcurrentHashMap<>();

      // Pre-compute expected serialized keys
      byte[] key1Bytes = keySerializer.serialize("KEY1");
      byte[] key2Bytes = keySerializer.serialize("KEY2");
      byte[] key3Bytes = keySerializer.serialize("KEY3");

      // Configure mock to record the order of writes per key
      doAnswer(invocation -> {
        Object[] args = invocation.getArguments();
        byte[] argKeyBytes = (byte[]) args[0];
        String key = getKeyName(argKeyBytes, key1Bytes, key2Bytes, key3Bytes);
        if (key != null) {
          writeOrderByKey.computeIfAbsent(key, k -> Collections.synchronizedList(new java.util.ArrayList<>()))
              .add("PUT");
        }
        // Simulate some write latency to increase chance of concurrent execution
        Utils.sleep(10);
        ((PubSubProducerCallback) args[4]).onCompletion(null, null);
        return null;
      }).when(producer.mockVeniceWriter).put(any(), any(), anyInt(), anyLong(), any());

      doAnswer(invocation -> {
        Object[] args = invocation.getArguments();
        byte[] argKeyBytes = (byte[]) args[0];
        String key = getKeyName(argKeyBytes, key1Bytes, key2Bytes, key3Bytes);
        if (key != null) {
          writeOrderByKey.computeIfAbsent(key, k -> Collections.synchronizedList(new java.util.ArrayList<>()))
              .add("DELETE");
        }
        Utils.sleep(10);
        ((PubSubProducerCallback) args[2]).onCompletion(null, null);
        return null;
      }).when(producer.mockVeniceWriter).delete(any(), anyLong(), any());

      // Submit operations on MULTIPLE keys - each key should maintain its own order
      // KEY1: PUT, PUT, DELETE
      // KEY2: DELETE, PUT, PUT
      // KEY3: PUT, DELETE, PUT
      List<CompletableFuture<DurableWrite>> futures = new java.util.ArrayList<>();

      // Interleave operations across keys to maximize concurrent execution
      futures.add(producer.asyncPut("KEY1", mockValue1)); // KEY1: op 0
      futures.add(producer.asyncDelete(100, "KEY2")); // KEY2: op 0
      futures.add(producer.asyncPut("KEY3", mockValue1)); // KEY3: op 0
      futures.add(producer.asyncPut("KEY1", mockValue2)); // KEY1: op 1
      futures.add(producer.asyncPut("KEY2", mockValue1)); // KEY2: op 1
      futures.add(producer.asyncDelete(200, "KEY3")); // KEY3: op 1
      futures.add(producer.asyncDelete(300, "KEY1")); // KEY1: op 2
      futures.add(producer.asyncPut("KEY2", mockValue2)); // KEY2: op 2
      futures.add(producer.asyncPut("KEY3", mockValue2)); // KEY3: op 2

      // Wait for all operations to complete
      for (CompletableFuture<DurableWrite> future: futures) {
        future.get();
      }

      // Verify per-key ordering is maintained
      // KEY1: PUT, PUT, DELETE (in that order)
      List<String> key1Order = writeOrderByKey.get("KEY1");
      assertEquals(3, key1Order.size(), "KEY1 should have 3 operations");
      assertEquals("PUT", key1Order.get(0), "KEY1 op 0 should be PUT");
      assertEquals("PUT", key1Order.get(1), "KEY1 op 1 should be PUT");
      assertEquals("DELETE", key1Order.get(2), "KEY1 op 2 should be DELETE");

      // KEY2: DELETE, PUT, PUT (in that order)
      List<String> key2Order = writeOrderByKey.get("KEY2");
      assertEquals(3, key2Order.size(), "KEY2 should have 3 operations");
      assertEquals("DELETE", key2Order.get(0), "KEY2 op 0 should be DELETE");
      assertEquals("PUT", key2Order.get(1), "KEY2 op 1 should be PUT");
      assertEquals("PUT", key2Order.get(2), "KEY2 op 2 should be PUT");

      // KEY3: PUT, DELETE, PUT (in that order)
      List<String> key3Order = writeOrderByKey.get("KEY3");
      assertEquals(3, key3Order.size(), "KEY3 should have 3 operations");
      assertEquals("PUT", key3Order.get(0), "KEY3 op 0 should be PUT");
      assertEquals("DELETE", key3Order.get(1), "KEY3 op 1 should be DELETE");
      assertEquals("PUT", key3Order.get(2), "KEY3 op 2 should be PUT");

      // Note: We intentionally do NOT assert anything about the global order across keys.
      // Different keys can execute in any order depending on worker scheduling.
    }
  }

  /**
   * Helper to identify which key the bytes correspond to.
   */
  private String getKeyName(byte[] argKeyBytes, byte[] key1Bytes, byte[] key2Bytes, byte[] key3Bytes) {
    if (Arrays.equals(argKeyBytes, key1Bytes)) {
      return "KEY1";
    } else if (Arrays.equals(argKeyBytes, key2Bytes)) {
      return "KEY2";
    } else if (Arrays.equals(argKeyBytes, key3Bytes)) {
      return "KEY3";
    }
    return null;
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testProducerConfigsAreExtractedToWriterOptions() throws IOException {
    ClientConfig storeClientConfig = configureMocksAndGetStoreConfig(storeName);

    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    Properties backendConfigs = new Properties();
    // Set producer configs that should be extracted to VeniceWriterOptions
    backendConfigs.put(VeniceWriter.PRODUCER_COUNT, "3");
    backendConfigs.put(VeniceWriter.PRODUCER_THREAD_COUNT, "5");
    backendConfigs.put(VeniceWriter.PRODUCER_QUEUE_SIZE, "10485760"); // 10MB

    try (TestOnlineVeniceProducer producer =
        new TestOnlineVeniceProducer(storeClientConfig, new VeniceProperties(backendConfigs), metricsRepository)) {
      VeniceWriterOptions writerOptions = producer.getCapturedWriterOptions();

      Assert.assertNotNull(writerOptions, "VeniceWriterOptions should be captured");
      Assert.assertEquals(writerOptions.getProducerCount(), 3, "Producer count should be extracted from config");
      Assert.assertEquals(
          writerOptions.getProducerThreadCount(),
          5,
          "Producer thread count should be extracted from config");
      Assert.assertEquals(
          writerOptions.getProducerQueueSize(),
          10485760,
          "Producer queue size should be extracted from config");
    }
  }

  @Test(timeOut = 10000)
  public void testOnlineProducerFactoryWriterHookOverloads() throws Exception {
    ClientConfig storeClientConfig = configureMocksAndGetStoreConfig(storeName + "-factory-hook");
    VeniceWriterHook writerHook = Mockito.mock(VeniceWriterHook.class);
    Properties producerProperties = new Properties();
    producerProperties.put(PUBSUB_PRODUCER_ADAPTER_FACTORY_CLASS, TestPubSubProducerAdapterFactory.class.getName());
    producerProperties.put(CLIENT_PRODUCER_WORKER_COUNT, 0);
    VeniceProperties producerConfigs = new VeniceProperties(producerProperties);

    try (OnlineVeniceProducer producer =
        OnlineProducerFactory.createProducer(storeClientConfig, producerConfigs, null, writerHook)) {
      Assert.assertSame(producer.getWriterHook(), writerHook);
      producer.asyncPut("KEY1", mockValue1).get(5, TimeUnit.SECONDS);
      verify(writerHook).onBeforeProduce(eq(VeniceWriterHook.OperationType.PUT), anyInt(), anyInt());
    }

    ClientConfig legacyStoreClientConfig = configureMocksAndGetStoreConfig(storeName + "-factory-legacy");
    try (OnlineVeniceProducer producer =
        OnlineProducerFactory.createProducer(legacyStoreClientConfig, producerConfigs, null)) {
      Assert.assertNull(producer.getWriterHook());
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testProducerConfigsDefaultsWhenNotSet() throws IOException {
    ClientConfig storeClientConfig = configureMocksAndGetStoreConfig(storeName);

    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    Properties backendConfigs = new Properties();
    // Don't set any producer configs - should use defaults

    try (TestOnlineVeniceProducer producer =
        new TestOnlineVeniceProducer(storeClientConfig, new VeniceProperties(backendConfigs), metricsRepository)) {
      VeniceWriterOptions writerOptions = producer.getCapturedWriterOptions();

      Assert.assertNotNull(writerOptions, "VeniceWriterOptions should be captured");
      // Default values from VeniceWriterOptions.Builder
      Assert.assertEquals(writerOptions.getProducerCount(), 1, "Producer count should default to 1");
      Assert.assertEquals(writerOptions.getProducerThreadCount(), 1, "Producer thread count should default to 1");
      Assert.assertEquals(
          writerOptions.getProducerQueueSize(),
          5 * 1024 * 1024,
          "Producer queue size should default to 5MB");
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testSystemProducerNamespaceDoesNotConfigureOnlineProducer() throws IOException {
    ClientConfig storeClientConfig = configureMocksAndGetStoreConfig(storeName);
    MetricsRepository metricsRepository = MetricsRepositoryUtils.createSingleThreadedMetricsRepository();
    Properties backendConfigs = new Properties();
    backendConfigs.put(VENICE_SYSTEM_PRODUCER_WORKER_COUNT, 0);
    backendConfigs.put(VENICE_SYSTEM_PRODUCER_CALLBACK_THREAD_COUNT, 2);

    try (TestOnlineVeniceProducer producer =
        new TestOnlineVeniceProducer(storeClientConfig, new VeniceProperties(backendConfigs), metricsRepository)) {
      Assert.assertEquals(producer.getCapturedDispatcher().getWorkerCount(), 4);
      Assert.assertFalse(producer.getCapturedDispatcher().isCallbackExecutorEnabled());
    }
  }

  private void configureMockKmeTransportClient(TransportClient transportClient) throws JsonProcessingException {
    doCallRealMethod().when(transportClient).getCopyIfNotUsableInCallback();
    doCallRealMethod().when(transportClient).get(anyString());
    doCallRealMethod().when(transportClient).post(anyString(), any());

    Map<Integer, Schema> schemasInLocalResources = Utils.getAllSchemasFromResources(KAFKA_MESSAGE_ENVELOPE);

    MultiSchemaResponse.Schema[] valueSchemaArr = new MultiSchemaResponse.Schema[schemasInLocalResources.size()];
    for (int i = 0; i < schemasInLocalResources.size(); i++) {
      MultiSchemaResponse.Schema valueSchema = new MultiSchemaResponse.Schema();
      valueSchema.setId(i + 1);
      valueSchema.setSchemaStr(schemasInLocalResources.get(i + 1).toString());

      valueSchemaArr[i] = valueSchema;
    }

    MultiSchemaResponse multiSchemaResponse = new MultiSchemaResponse();
    multiSchemaResponse.setSchemas(valueSchemaArr);
    multiSchemaResponse.setCluster(clusterName);

    doAnswer(invocation -> getTransportClientFuture(MAPPER.writeValueAsBytes(multiSchemaResponse), 0))
        .when(transportClient)
        .get(eq("value_schema/" + KAFKA_MESSAGE_ENVELOPE.getSystemStoreName()), anyMap());

    for (int i = 0; i < schemasInLocalResources.size(); i++) {
      SchemaResponse valueSchemaResponse = new SchemaResponse();
      valueSchemaResponse.setId(i + 1);
      valueSchemaResponse.setSchemaStr(schemasInLocalResources.get(i + 1).toString());
      doAnswer(invocation -> getTransportClientFuture(MAPPER.writeValueAsBytes(valueSchemaResponse), 0))
          .when(transportClient)
          .get(eq("value_schema/" + KAFKA_MESSAGE_ENVELOPE.getSystemStoreName() + "/" + (i + 1)), anyMap());
    }

    MultiSchemaIdResponse multiSchemaIdResponse = new MultiSchemaIdResponse();
    Set<Integer> schemaIdSet = new HashSet<>();
    for (int i = 1; i <= schemasInLocalResources.size(); i++) {
      schemaIdSet.add(i);
    }
    multiSchemaIdResponse.setSchemaIdSet(schemaIdSet);
    doAnswer(invocation -> getTransportClientFuture(MAPPER.writeValueAsBytes(multiSchemaIdResponse), 0))
        .when(transportClient)
        .get(eq("value_schema_ids/" + KAFKA_MESSAGE_ENVELOPE.getSystemStoreName()), anyMap());
  }

  private ClientConfig configureMocksAndGetStoreConfig(String storeName) throws IOException {
    return configureMocksAndGetStoreConfig(storeName, false, null);
  }

  private ClientConfig configureMocksAndGetStoreConfig(String storeName, boolean updateEnabled) throws IOException {
    return configureMocksAndGetStoreConfig(storeName, updateEnabled, null);
  }

  private ClientConfig configureMocksAndGetStoreConfig(
      String storeName,
      boolean updateEnabled,
      byte[] requestTopicResponse) throws IOException {
    TransportClient mockTransportClient = mock(TransportClient.class);
    TransportClient mockKmeTransportClient = mock(TransportClient.class);

    ClientConfig storeClientConfig = ClientConfig.defaultGenericClientConfig(storeName);
    ClientConfig<KafkaMessageEnvelope> kmeClientConfig = ClientConfig.cloneConfig(storeClientConfig)
        .setStoreName(KAFKA_MESSAGE_ENVELOPE.getSystemStoreName())
        .setSpecificValueClass(KafkaMessageEnvelope.class);

    configureMockTransportClient(mockTransportClient, updateEnabled, requestTopicResponse, 0, storeName);
    configureMockKmeTransportClient(mockKmeTransportClient);

    ClientFactoryTestUtils.registerTransportClient(storeClientConfig, mockTransportClient);
    ClientFactoryTestUtils.registerTransportClient(kmeClientConfig, mockKmeTransportClient);

    return storeClientConfig;
  }

  private ClientResourceMocks replaceClientResourcesWithMocks(OnlineVeniceProducer producer) throws Exception {
    Field schemaReaderField = OnlineVeniceProducer.class.getDeclaredField("schemaReader");
    schemaReaderField.setAccessible(true);
    SchemaReader originalSchemaReader = (SchemaReader) schemaReaderField.get(producer);
    SchemaReader schemaReader = mock(SchemaReader.class);
    schemaReaderField.set(producer, schemaReader);

    Field storeClientField = OnlineVeniceProducer.class.getDeclaredField("storeClient");
    storeClientField.setAccessible(true);
    InternalAvroStoreClient originalStoreClient = (InternalAvroStoreClient) storeClientField.get(producer);
    InternalAvroStoreClient storeClient = mock(InternalAvroStoreClient.class);
    storeClientField.set(producer, storeClient);
    return new ClientResourceMocks(schemaReader, storeClient, originalSchemaReader, originalStoreClient);
  }

  private void configureMockTransportClient(
      TransportClient transportClient,
      boolean updateEnabled,
      byte[] requestTopicResponse,
      int delayInResponseMs) {
    configureMockTransportClient(transportClient, updateEnabled, requestTopicResponse, delayInResponseMs, storeName);
  }

  private void configureMockTransportClient(
      TransportClient transportClient,
      boolean updateEnabled,
      byte[] requestTopicResponse,
      int delayInResponseMs,
      String configuredStoreName) {
    doCallRealMethod().when(transportClient).getCopyIfNotUsableInCallback();
    doCallRealMethod().when(transportClient).get(anyString());
    doCallRealMethod().when(transportClient).post(anyString(), any());

    int partitionCount = 10;
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    Version version = new VersionImpl(configuredStoreName, 1, "test-job-id");
    version.setPartitionCount(partitionCount);

    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(1000, 1000, -1, BufferReplayPolicy.REWIND_FROM_EOP);

    ZKStore store = new ZKStore(
        configuredStoreName,
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

    doAnswer(invocation -> {
      if (requestTopicResponse == null) {
        VersionCreationResponse versionCreationResponse = new VersionCreationResponse();
        versionCreationResponse.setPartitions(partitionCount);
        versionCreationResponse.setPartitionerClass(partitionerConfig.getPartitionerClass());
        versionCreationResponse.setPartitionerParams(partitionerConfig.getPartitionerParams());
        versionCreationResponse.setKafkaBootstrapServers("localhost:9092");
        versionCreationResponse.setKafkaTopic(Utils.getRealTimeTopicName(store));
        versionCreationResponse.setEnableSSL(false);

        return getTransportClientFuture(MAPPER.writeValueAsBytes(versionCreationResponse), delayInResponseMs);
      } else {
        return getTransportClientFuture(requestTopicResponse, delayInResponseMs);
      }
    }).when(transportClient).get(eq("request_topic/" + configuredStoreName), anyMap());

    doAnswer(invocation -> getTransportClientFuture(STORE_SERIALIZER.serialize(store, null), delayInResponseMs))
        .when(transportClient)
        .get(eq("store_state/" + configuredStoreName), anyMap());

    configureSchemaResponseMocks(
        transportClient,
        Arrays.asList(VALUE_SCHEMA_1, VALUE_SCHEMA_2),
        2,
        Arrays.asList(UPDATE_SCHEMA_1, UPDATE_SCHEMA_2),
        updateEnabled,
        delayInResponseMs,
        configuredStoreName);
  }

  private void configureSchemaResponseMocks(
      TransportClient transportClient,
      List<Schema> valueSchemas,
      int supersetSchemaId,
      List<Schema> updateSchemas,
      boolean updateEnabled,
      int delayInResponseMs,
      String configuredStoreName) {
    String keySchemaStr = KEY_SCHEMA.toString();
    SchemaResponse keySchemaResponse = new SchemaResponse();
    keySchemaResponse.setId(1);
    keySchemaResponse.setSchemaStr(keySchemaStr);

    doAnswer(invocation -> getTransportClientFuture(MAPPER.writeValueAsBytes(keySchemaResponse), delayInResponseMs))
        .when(transportClient)
        .get(eq("key_schema/" + configuredStoreName), anyMap());

    MultiSchemaIdResponse multiSchemaIdResponse = new MultiSchemaIdResponse();
    if (supersetSchemaId > 0) {
      multiSchemaIdResponse.setSuperSetSchemaId(supersetSchemaId);
    }
    Set<Integer> schemaIdSet = new HashSet<>();
    for (int i = 1; i <= valueSchemas.size(); i++) {
      schemaIdSet.add(i);
    }
    multiSchemaIdResponse.setSchemaIdSet(schemaIdSet);
    doAnswer(invocation -> getTransportClientFuture(MAPPER.writeValueAsBytes(multiSchemaIdResponse), delayInResponseMs))
        .when(transportClient)
        .get(eq("value_schema_ids/" + configuredStoreName), anyMap());

    // Also mock the all_value_schema_ids endpoint used by RouterBackedSchemaReader
    doAnswer(invocation -> getTransportClientFuture(MAPPER.writeValueAsBytes(multiSchemaIdResponse), delayInResponseMs))
        .when(transportClient)
        .get(eq("all_value_schema_ids/" + configuredStoreName), anyMap());

    for (int i = 0; i < valueSchemas.size(); i++) {
      SchemaResponse valueSchemaResponse = new SchemaResponse();
      valueSchemaResponse.setId(i + 1);
      valueSchemaResponse.setSchemaStr(valueSchemas.get(i).toString());
      doAnswer(invocation -> getTransportClientFuture(MAPPER.writeValueAsBytes(valueSchemaResponse), delayInResponseMs))
          .when(transportClient)
          .get(eq("value_schema/" + configuredStoreName + "/" + (i + 1)), anyMap());
    }

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

    doAnswer(invocation -> getTransportClientFuture(MAPPER.writeValueAsBytes(multiSchemaResponse), delayInResponseMs))
        .when(transportClient)
        .get(eq("value_schema/" + configuredStoreName), anyMap());

    if (updateEnabled) {
      MultiSchemaResponse allUpdateSchemaResponse = new MultiSchemaResponse();
      allUpdateSchemaResponse.setCluster(clusterName);
      allUpdateSchemaResponse.setName(configuredStoreName);

      MultiSchemaResponse.Schema[] multiSchemas = new MultiSchemaResponse.Schema[updateSchemas.size()];
      for (int i = 0; i < updateSchemas.size(); i++) {
        SchemaResponse updateSchemaResponse = new SchemaResponse();
        updateSchemaResponse.setCluster(clusterName);
        updateSchemaResponse.setName(configuredStoreName);
        updateSchemaResponse.setId(i + 1);
        updateSchemaResponse.setDerivedSchemaId(1);
        updateSchemaResponse.setSchemaStr(updateSchemas.get(i).toString());

        doAnswer(
            invocation -> getTransportClientFuture(MAPPER.writeValueAsBytes(updateSchemaResponse), delayInResponseMs))
                .when(transportClient)
                .get(eq("update_schema/" + configuredStoreName + "/" + (i + 1)), anyMap());

        MultiSchemaResponse.Schema schema = new MultiSchemaResponse.Schema();
        schema.setId(i + 1);
        schema.setDerivedSchemaId(1);
        schema.setSchemaStr(updateSchemas.get(i).toString());
        multiSchemas[i] = schema;
      }
      allUpdateSchemaResponse.setSchemas(multiSchemas);

      doAnswer(
          invocation -> getTransportClientFuture(MAPPER.writeValueAsBytes(allUpdateSchemaResponse), delayInResponseMs))
              .when(transportClient)
              .get(eq("update_schema/" + configuredStoreName), anyMap());
    } else {
      for (int i = 0; i < updateSchemas.size(); i++) {
        SchemaResponse noUpdateSchemaResponse = new SchemaResponse();
        noUpdateSchemaResponse.setError(
            "Update schema doesn't exist for value schema id: " + (i + 1) + " of store: " + configuredStoreName);

        doAnswer(
            invocation -> getTransportClientFuture(MAPPER.writeValueAsBytes(noUpdateSchemaResponse), delayInResponseMs))
                .when(transportClient)
                .get(eq("update_schema/" + configuredStoreName + "/" + (i + 1)), anyMap());
      }

      MultiSchemaResponse allUpdateSchemaResponse = new MultiSchemaResponse();
      allUpdateSchemaResponse.setCluster(clusterName);
      allUpdateSchemaResponse.setName(configuredStoreName);

      MultiSchemaResponse.Schema[] multiSchemas = new MultiSchemaResponse.Schema[0];
      allUpdateSchemaResponse.setSchemas(multiSchemas);

      doAnswer(
          invocation -> getTransportClientFuture(MAPPER.writeValueAsBytes(allUpdateSchemaResponse), delayInResponseMs))
              .when(transportClient)
              .get(eq("update_schema/" + configuredStoreName), anyMap());
    }
  }

  private static class TestOnlineVeniceProducer<K, V> extends OnlineVeniceProducer<K, V> {
    // Creating globally to access the same object in tests
    private VeniceWriter<byte[], byte[], byte[]> mockVeniceWriter;
    private boolean failPubSubWrites;
    private VeniceWriterOptions capturedWriterOptions;
    private PartitionedProducerExecutor capturedDispatcher;

    public TestOnlineVeniceProducer(
        ClientConfig storeClientConfig,
        VeniceProperties backendConfigs,
        MetricsRepository metricsRepository) {
      this(storeClientConfig, backendConfigs, metricsRepository, false);
    }

    public TestOnlineVeniceProducer(
        ClientConfig storeClientConfig,
        VeniceProperties backendConfigs,
        MetricsRepository metricsRepository,
        boolean failPubSubWrites) {
      super(storeClientConfig, backendConfigs, metricsRepository, null);
      this.failPubSubWrites = failPubSubWrites;

      configureVeniceWriteMock();
    }

    @Override
    protected VeniceWriter<byte[], byte[], byte[]> constructVeniceWriter(
        Properties properties,
        VeniceWriterOptions writerOptions) {
      this.capturedWriterOptions = writerOptions;
      if (mockVeniceWriter == null) {
        mockVeniceWriter = Mockito.mock(VeniceWriter.class);
      }
      return mockVeniceWriter;
    }

    @Override
    protected PartitionedProducerExecutor createDispatcher(
        String storeName,
        VeniceProperties configs,
        MetricsRepository metricsRepository) {
      capturedDispatcher = super.createDispatcher(storeName, configs, metricsRepository);
      return capturedDispatcher;
    }

    public VeniceWriterOptions getCapturedWriterOptions() {
      return capturedWriterOptions;
    }

    public PartitionedProducerExecutor getCapturedDispatcher() {
      return capturedDispatcher;
    }

    public void scheduleDeferredFailure(RuntimeException failure, CountDownLatch completionRan) {
      Runnable completion = () -> {
        try {
          throw failure;
        } finally {
          completionRan.countDown();
        }
      };
      scheduleDeferredCompletionForTest(completion);
    }

    public void scheduleCloseThenFailure(
        RuntimeException failure,
        CountDownLatch closeReturned,
        CountDownLatch completionFinished) {
      scheduleDeferredCompletionForTest(() -> {
        try {
          try {
            close();
          } catch (IOException exception) {
            throw new AssertionError("Reentrant close unexpectedly failed", exception);
          }
          closeReturned.countDown();
          throw failure;
        } finally {
          completionFinished.countDown();
        }
      });
    }

    private void scheduleDeferredCompletionForTest(Runnable completion) {
      try {
        Method scheduleDeferredCompletion =
            AbstractVeniceProducer.class.getDeclaredMethod("scheduleDeferredCompletion", Runnable.class);
        scheduleDeferredCompletion.setAccessible(true);
        scheduleDeferredCompletion.invoke(this, completion);
      } catch (ReflectiveOperationException exception) {
        throw new RuntimeException(exception);
      }
    }

    private void configureVeniceWriteMock() {
      doAnswer(getPubSubProducerCallbackAnswer(failPubSubWrites, 3)).when(mockVeniceWriter)
          .put(any(), any(), anyInt(), any());
      doAnswer(getPubSubProducerCallbackAnswer(failPubSubWrites, 4)).when(mockVeniceWriter)
          .put(any(), any(), anyInt(), anyLong(), any());

      doAnswer(getPubSubProducerCallbackAnswer(failPubSubWrites, 1)).when(mockVeniceWriter).delete(any(), any());
      doAnswer(getPubSubProducerCallbackAnswer(failPubSubWrites, 2)).when(mockVeniceWriter)
          .delete(any(), anyLong(), any());

      doAnswer(getPubSubProducerCallbackAnswer(failPubSubWrites, 4)).when(mockVeniceWriter)
          .update(any(), any(), anyInt(), anyInt(), any());
      doAnswer(getPubSubProducerCallbackAnswer(failPubSubWrites, 4)).when(mockVeniceWriter)
          .update(any(), any(), anyInt(), anyInt(), any(), anyLong());
    }

    private static Answer getPubSubProducerCallbackAnswer(boolean error, int callbackArgIndex) {
      if (error) {
        return invocation -> {
          Object[] args = invocation.getArguments();
          ((PubSubProducerCallback) args[callbackArgIndex]).onCompletion(null, new VeniceException());
          return null;
        };
      } else {
        return invocation -> {
          Object[] args = invocation.getArguments();
          ((PubSubProducerCallback) args[callbackArgIndex]).onCompletion(null, null);
          return null;
        };
      }
    }
  }

  private static class WorkerTerminationFailingOnlineVeniceProducer<K, V> extends TestOnlineVeniceProducer<K, V> {
    WorkerTerminationFailingOnlineVeniceProducer(
        ClientConfig storeClientConfig,
        VeniceProperties backendConfigs,
        MetricsRepository metricsRepository) {
      super(storeClientConfig, backendConfigs, metricsRepository);
    }

    @Override
    protected PartitionedProducerExecutor createDispatcher(
        String storeName,
        VeniceProperties configs,
        MetricsRepository metricsRepository) {
      return new PartitionedProducerExecutor(1, 1, 0, 1, storeName, metricsRepository) {
        private final AtomicInteger remainingFailures = new AtomicInteger(2);

        @Override
        public boolean awaitWorkerTermination(long timeout, TimeUnit unit) throws InterruptedException {
          return remainingFailures.getAndDecrement() > 0 ? false : super.awaitWorkerTermination(timeout, unit);
        }
      };
    }
  }

  private static class CallbackTerminationFailingOnlineVeniceProducer<K, V> extends TestOnlineVeniceProducer<K, V> {
    CallbackTerminationFailingOnlineVeniceProducer(
        ClientConfig storeClientConfig,
        VeniceProperties backendConfigs,
        MetricsRepository metricsRepository) {
      super(storeClientConfig, backendConfigs, metricsRepository);
    }

    @Override
    protected PartitionedProducerExecutor createDispatcher(
        String storeName,
        VeniceProperties configs,
        MetricsRepository metricsRepository) {
      return new PartitionedProducerExecutor(0, 1, 1, 1, storeName, metricsRepository) {
        private final AtomicInteger remainingFailures = new AtomicInteger(2);

        @Override
        public boolean awaitCallbackTermination(long timeout, TimeUnit unit) throws InterruptedException {
          return remainingFailures.getAndDecrement() > 0 ? false : super.awaitCallbackTermination(timeout, unit);
        }
      };
    }
  }

  private static class ClientResourceMocks {
    private final SchemaReader schemaReader;
    private final InternalAvroStoreClient storeClient;
    private final SchemaReader originalSchemaReader;
    private final InternalAvroStoreClient originalStoreClient;

    private ClientResourceMocks(
        SchemaReader schemaReader,
        InternalAvroStoreClient storeClient,
        SchemaReader originalSchemaReader,
        InternalAvroStoreClient originalStoreClient) {
      this.schemaReader = schemaReader;
      this.storeClient = storeClient;
      this.originalSchemaReader = originalSchemaReader;
      this.originalStoreClient = originalStoreClient;
    }

    private void closeOriginalResources() {
      Utils.closeQuietlyWithErrorLogged(originalSchemaReader, originalStoreClient);
    }
  }

  public static class TestPubSubProducerAdapterFactory extends PubSubProducerAdapterFactory<PubSubProducerAdapter> {
    @Override
    public PubSubProducerAdapter create(PubSubProducerAdapterContext context) {
      PubSubProducerAdapter adapter = mock(PubSubProducerAdapter.class);
      doAnswer(invocation -> {
        PubSubProducerCallback callback = invocation.getArgument(5);
        callback.onCompletion(null, null);
        return CompletableFuture.completedFuture(null);
      }).when(adapter).sendMessage(anyString(), anyInt(), any(), any(), any(), any());
      Mockito.when(adapter.getBrokerAddress()).thenReturn(context.getBrokerAddress());
      return adapter;
    }

    @Override
    public String getName() {
      return "online-producer-factory-test";
    }

    @Override
    public void close() {
    }
  }

  private static GenericRecord getMockValue(Schema schema) {
    RandomRecordGenerator recordGenerator = new RandomRecordGenerator();
    RecordGenerationConfig genConfig = RecordGenerationConfig.newConfig().withAvoidNulls(true);

    return (GenericRecord) recordGenerator.randomGeneric(schema, genConfig);
  }

  private GenericRecord createFieldNoOpRecord(Schema schema, String fieldName) {
    Schema noOpSchema = schema.getField(fieldName).schema().getTypes().get(0);
    return new GenericData.Record(noOpSchema);
  }

  private static RecordSerializer<Object> getSerializer(Schema schema) {
    return FastSerializerDeserializerFactory.getFastAvroGenericSerializer(schema);
  }

  private void assertThrowsExceptionFromFuture(Class throwableClass, Assert.ThrowingRunnable runnable) {
    Throwable thrown = null;
    try {
      runnable.run();
    } catch (ExecutionException e) {
      if (e.getCause() != null && throwableClass.isInstance(e.getCause())) {
        return;
      }

      thrown = e;
    } catch (Throwable t) {
      thrown = t;
    }

    if (thrown == null) {
      Assert.fail("Expected exception to be thrown");
    }

    throw new AssertionError(thrown.getMessage(), thrown);
  }

  private CompletableFuture<TransportClientResponse> getTransportClientFuture(byte[] body, long delayInResponseMs) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        if (delayInResponseMs > 0) {
          Utils.sleep(delayInResponseMs);
        }
        return new TransportClientResponse(1, CompressionStrategy.NO_OP, body);
      } catch (Throwable t) {
        return null;
      }
    });
  }
}
