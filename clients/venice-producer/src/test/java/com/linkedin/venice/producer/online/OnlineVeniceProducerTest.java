package com.linkedin.venice.producer.online;

import static com.linkedin.venice.ConfigKeys.CLIENT_PRODUCER_SCHEMA_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.utils.TestWriteUtils.loadFileAsStringQuietlyWithErrorLogged;
import static com.linkedin.venice.writer.VeniceWriter.APP_DEFAULT_LOGICAL_TS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.RandomRecordGenerator;
import com.linkedin.avroutil1.compatibility.RecordGenerationConfig;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.StoreJSONSerializer;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
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
import com.linkedin.venice.producer.VeniceProducer;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import com.linkedin.venice.writer.update.UpdateBuilder;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
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
  private static final String MIN_PENDING_OPERATION_METRIC_NAME = ".test_store--pending_write_operation.Min";
  private static final String MAX_PENDING_OPERATION_METRIC_NAME = ".test_store--pending_write_operation.Max";

  @Test
  public void testConstructor() throws IOException, ExecutionException, InterruptedException {
    AbstractAvroStoreClient storeClient = getMockStoreClient();
    SchemaReader kmeSchemaReader = getKmeSchemaReader();

    MetricsRepository metricsRepository = new MetricsRepository();
    Properties backendConfigs = new Properties();
    VeniceProducer producer = new TestOnlineVeniceProducer(
        storeClient,
        kmeSchemaReader,
        new VeniceProperties(backendConfigs),
        metricsRepository);
    producer.close();
  }

  @Test
  public void testFailRequestTopic() throws IOException, ExecutionException, InterruptedException {
    SchemaReader kmeSchemaReader = getKmeSchemaReader();
    MetricsRepository metricsRepository = new MetricsRepository();
    Properties backendConfigs = new Properties();

    VersionCreationResponse versionCreationResponse = new VersionCreationResponse();
    versionCreationResponse.setError("ERROR RESPONSE");

    AbstractAvroStoreClient storeClient =
        getMockStoreClientWithRequestTopicResponse(MAPPER.writeValueAsBytes(versionCreationResponse));
    Assert.assertThrows(
        VeniceException.class,
        () -> new TestOnlineVeniceProducer(
            storeClient,
            kmeSchemaReader,
            new VeniceProperties(backendConfigs),
            metricsRepository));

    AbstractAvroStoreClient storeClient2 =
        getMockStoreClientWithRequestTopicResponse(versionCreationResponse.getError().getBytes(StandardCharsets.UTF_8));
    Assert.assertThrows(
        VeniceException.class,
        () -> new TestOnlineVeniceProducer(
            storeClient2,
            kmeSchemaReader,
            new VeniceProperties(backendConfigs),
            metricsRepository));
  }

  @Test
  public void testPut() throws IOException, ExecutionException, InterruptedException {
    SchemaReader kmeSchemaReader = getKmeSchemaReader();
    AbstractAvroStoreClient storeClient = getMockStoreClient();

    MetricsRepository metricsRepository = new MetricsRepository();
    Properties backendConfigs = new Properties();
    try (TestOnlineVeniceProducer producer = new TestOnlineVeniceProducer(
        storeClient,
        kmeSchemaReader,
        new VeniceProperties(backendConfigs),
        metricsRepository)) {
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
      Assert.assertEquals(metricsRepository.getMetric(MIN_PENDING_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(MAX_PENDING_OPERATION_METRIC_NAME).value(), 1.0);

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

  @Test
  public void testPutWithLogicalTs() throws IOException, ExecutionException, InterruptedException {
    SchemaReader kmeSchemaReader = getKmeSchemaReader();
    AbstractAvroStoreClient storeClient = getMockStoreClient();

    MetricsRepository metricsRepository = new MetricsRepository();
    Properties backendConfigs = new Properties();
    try (TestOnlineVeniceProducer producer = new TestOnlineVeniceProducer(
        storeClient,
        kmeSchemaReader,
        new VeniceProperties(backendConfigs),
        metricsRepository)) {

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
      Assert.assertEquals(metricsRepository.getMetric(MIN_PENDING_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(MAX_PENDING_OPERATION_METRIC_NAME).value(), 1.0);

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

  @Test
  public void testPutWithInvalidSchema() throws IOException, ExecutionException, InterruptedException {
    SchemaReader kmeSchemaReader = getKmeSchemaReader();
    AbstractAvroStoreClient storeClient = getMockStoreClient();

    MetricsRepository metricsRepository = new MetricsRepository();
    Properties backendConfigs = new Properties();
    try (TestOnlineVeniceProducer producer = new TestOnlineVeniceProducer(
        storeClient,
        kmeSchemaReader,
        new VeniceProperties(backendConfigs),
        metricsRepository)) {
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
      Assert.assertEquals(metricsRepository.getMetric(MIN_PENDING_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(MAX_PENDING_OPERATION_METRIC_NAME).value(), 1.0);
    }
  }

  @Test
  public void testPutWithFailedWrite() throws IOException, ExecutionException, InterruptedException {
    SchemaReader kmeSchemaReader = getKmeSchemaReader();
    AbstractAvroStoreClient storeClient = getMockStoreClient();

    MetricsRepository metricsRepository = new MetricsRepository();
    Properties backendConfigs = new Properties();
    try (TestOnlineVeniceProducer producer = new TestOnlineVeniceProducer(
        storeClient,
        kmeSchemaReader,
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
      Assert.assertEquals(metricsRepository.getMetric(MIN_PENDING_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(MAX_PENDING_OPERATION_METRIC_NAME).value(), 1.0);
    }
  }

  @Test
  public void testDelete() throws IOException, ExecutionException, InterruptedException {
    SchemaReader kmeSchemaReader = getKmeSchemaReader();
    AbstractAvroStoreClient storeClient = getMockStoreClient();

    MetricsRepository metricsRepository = new MetricsRepository();
    Properties backendConfigs = new Properties();
    try (TestOnlineVeniceProducer producer = new TestOnlineVeniceProducer(
        storeClient,
        kmeSchemaReader,
        new VeniceProperties(backendConfigs),
        metricsRepository)) {
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
      Assert.assertEquals(metricsRepository.getMetric(MIN_PENDING_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(MAX_PENDING_OPERATION_METRIC_NAME).value(), 1.0);

      producer.asyncDelete("KEY2").get();
      verify(producer.mockVeniceWriter, times(2))
          .delete(keyArg.capture(), eq(APP_DEFAULT_LOGICAL_TS), producerCallbackArg.capture());

      assertEquals(keySerializer.serialize("KEY2"), keyArg.getValue());
    }
  }

  @Test
  public void testDeleteWithLogicalTs() throws IOException, ExecutionException, InterruptedException {
    SchemaReader kmeSchemaReader = getKmeSchemaReader();
    AbstractAvroStoreClient storeClient = getMockStoreClient();

    MetricsRepository metricsRepository = new MetricsRepository();
    Properties backendConfigs = new Properties();
    try (TestOnlineVeniceProducer producer = new TestOnlineVeniceProducer(
        storeClient,
        kmeSchemaReader,
        new VeniceProperties(backendConfigs),
        metricsRepository)) {
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
      Assert.assertEquals(metricsRepository.getMetric(MIN_PENDING_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(MAX_PENDING_OPERATION_METRIC_NAME).value(), 1.0);

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

  @Test
  public void testDeleteWithFailedWrite() throws IOException, ExecutionException, InterruptedException {
    SchemaReader kmeSchemaReader = getKmeSchemaReader();
    AbstractAvroStoreClient storeClient = getMockStoreClient();

    MetricsRepository metricsRepository = new MetricsRepository();
    Properties backendConfigs = new Properties();
    try (TestOnlineVeniceProducer producer = new TestOnlineVeniceProducer(
        storeClient,
        kmeSchemaReader,
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
      Assert.assertEquals(metricsRepository.getMetric(MIN_PENDING_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(MAX_PENDING_OPERATION_METRIC_NAME).value(), 1.0);
    }
  }

  @Test
  public void testUpdate() throws IOException, ExecutionException, InterruptedException {
    SchemaReader kmeSchemaReader = getKmeSchemaReader();
    AbstractAvroStoreClient storeClient = getMockStoreClient(true);

    MetricsRepository metricsRepository = new MetricsRepository();
    Properties backendConfigs = new Properties();
    try (TestOnlineVeniceProducer producer = new TestOnlineVeniceProducer(
        storeClient,
        kmeSchemaReader,
        new VeniceProperties(backendConfigs),
        metricsRepository)) {
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
      Assert.assertEquals(metricsRepository.getMetric(MIN_PENDING_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(MAX_PENDING_OPERATION_METRIC_NAME).value(), 1.0);

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

  @Test
  public void testUpdateWithLogicalTs() throws IOException, ExecutionException, InterruptedException {
    SchemaReader kmeSchemaReader = getKmeSchemaReader();
    AbstractAvroStoreClient storeClient = getMockStoreClient(true);

    MetricsRepository metricsRepository = new MetricsRepository();
    Properties backendConfigs = new Properties();
    try (TestOnlineVeniceProducer producer = new TestOnlineVeniceProducer(
        storeClient,
        kmeSchemaReader,
        new VeniceProperties(backendConfigs),
        metricsRepository)) {
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
      Assert.assertEquals(metricsRepository.getMetric(MIN_PENDING_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(MAX_PENDING_OPERATION_METRIC_NAME).value(), 1.0);

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

  @Test
  public void testUpdateOnUnsupportedStore() throws IOException, ExecutionException, InterruptedException {
    SchemaReader kmeSchemaReader = getKmeSchemaReader();
    AbstractAvroStoreClient storeClient = getMockStoreClient();

    MetricsRepository metricsRepository = new MetricsRepository();
    Properties backendConfigs = new Properties();
    try (TestOnlineVeniceProducer producer = new TestOnlineVeniceProducer(
        storeClient,
        kmeSchemaReader,
        new VeniceProperties(backendConfigs),
        metricsRepository)) {
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
      Assert.assertEquals(metricsRepository.getMetric(MIN_PENDING_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(MAX_PENDING_OPERATION_METRIC_NAME).value(), 1.0);
    }
  }

  @Test
  public void testUpdateWithFailedWrite() throws IOException, ExecutionException, InterruptedException {
    SchemaReader kmeSchemaReader = getKmeSchemaReader();
    AbstractAvroStoreClient storeClient = getMockStoreClient(true);

    MetricsRepository metricsRepository = new MetricsRepository();
    Properties backendConfigs = new Properties();
    try (TestOnlineVeniceProducer producer = new TestOnlineVeniceProducer(
        storeClient,
        kmeSchemaReader,
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
      Assert.assertEquals(metricsRepository.getMetric(MIN_PENDING_OPERATION_METRIC_NAME).value(), 0.0);
      Assert.assertEquals(metricsRepository.getMetric(MAX_PENDING_OPERATION_METRIC_NAME).value(), 1.0);
    }
  }

  @Test
  public void testOperationsOnClosedProducer() throws IOException, ExecutionException, InterruptedException {
    SchemaReader kmeSchemaReader = getKmeSchemaReader();
    AbstractAvroStoreClient storeClient = getMockStoreClient(true);

    MetricsRepository metricsRepository = new MetricsRepository();
    Properties backendConfigs = new Properties();
    TestOnlineVeniceProducer producer = new TestOnlineVeniceProducer(
        storeClient,
        kmeSchemaReader,
        new VeniceProperties(backendConfigs),
        metricsRepository);
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

  @Test
  public void testFetchLatestValueAndUpdateSchemas() throws IOException, ExecutionException, InterruptedException {
    AbstractAvroStoreClient storeClient = getMockStoreClient(true);
    SchemaReader kmeSchemaReader = getKmeSchemaReader();

    MetricsRepository metricsRepository = new MetricsRepository();
    Properties backendConfigs = new Properties();
    backendConfigs.put(CLIENT_PRODUCER_SCHEMA_REFRESH_INTERVAL_SECONDS, 1);
    try (VeniceProducer producer = new TestOnlineVeniceProducer(
        storeClient,
        kmeSchemaReader,
        new VeniceProperties(backendConfigs),
        metricsRepository)) {
      producer.asyncUpdate(1000, "KEY1", updateBuilderObj -> {
        UpdateBuilder updateBuilder = ((UpdateBuilder) updateBuilderObj);
        updateBuilder.setNewFieldValue(FIELD_COLOR, "green");
        Assert.assertEquals(updateBuilder.build().getSchema().toString(), UPDATE_SCHEMA_2.toString());
      }).get();

      // Register 2 new value schemas with one of them as a new superset schema
      configureSchemaResponseMocks(
          storeClient,
          Arrays.asList(VALUE_SCHEMA_1, VALUE_SCHEMA_2, VALUE_SCHEMA_3, VALUE_SCHEMA_4),
          3,
          Arrays.asList(UPDATE_SCHEMA_1, UPDATE_SCHEMA_2, UPDATE_SCHEMA_3, UPDATE_SCHEMA_4),
          true);
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.MINUTES, () -> {
        try {
          producer.asyncUpdate(1000, "KEY1", updateBuilderObj -> {
            UpdateBuilder updateBuilder = ((UpdateBuilder) updateBuilderObj);
            updateBuilder.setNewFieldValue(FIELD_COLOR, "green");
            Assert.assertEquals(updateBuilder.build().getSchema().toString(), UPDATE_SCHEMA_3.toString());
          }).get();
        } catch (ExecutionException e) {
          Assert.fail();
        }
      });
    }
  }

  private SchemaReader getKmeSchemaReader() {
    SchemaReader kmeSchemaReader = mock(SchemaReader.class);
    when(kmeSchemaReader.getValueSchema(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersion()))
        .thenReturn(KafkaMessageEnvelope.SCHEMA$);
    return kmeSchemaReader;
  }

  private AbstractAvroStoreClient getMockStoreClient() throws IOException, ExecutionException, InterruptedException {
    return getMockStoreClient(false);
  }

  private AbstractAvroStoreClient getMockStoreClient(boolean updateEnabled)
      throws IOException, ExecutionException, InterruptedException {
    int partitionCount = 10;
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    Version version = new VersionImpl(storeName, 1, "test-job-id");
    version.setPartitionCount(partitionCount);

    HybridStoreConfig hybridStoreConfig = new HybridStoreConfigImpl(
        1000,
        1000,
        -1,
        DataReplicationPolicy.ACTIVE_ACTIVE,
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
    versionCreationResponse.setKafkaTopic(Version.composeRealTimeTopic(storeName));
    versionCreationResponse.setAmplificationFactor(1);
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
        updateEnabled);

    return storeClient;
  }

  private void configureSchemaResponseMocks(
      AbstractAvroStoreClient storeClient,
      List<Schema> valueSchemas,
      int supersetSchemaId,
      List<Schema> updateSchemas,
      boolean updateEnabled) throws JsonProcessingException, ExecutionException, InterruptedException {
    String keySchemaStr = KEY_SCHEMA.toString();
    SchemaResponse keySchemaResponse = new SchemaResponse();
    keySchemaResponse.setId(1);
    keySchemaResponse.setSchemaStr(keySchemaStr);

    CompletableFuture<byte[]> keySchemaFuture = mock(CompletableFuture.class);
    Mockito.doReturn(MAPPER.writeValueAsBytes(keySchemaResponse)).when(keySchemaFuture).get();
    Mockito.doReturn(keySchemaFuture).when(storeClient).getRaw("key_schema/" + storeName);

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

    CompletableFuture<byte[]> valueSchemasFuture = mock(CompletableFuture.class);
    Mockito.doReturn(MAPPER.writeValueAsBytes(multiSchemaResponse)).when(valueSchemasFuture).get();
    Mockito.doReturn(valueSchemasFuture).when(storeClient).getRaw("value_schema/" + storeName);

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

        CompletableFuture<byte[]> updateSchemaFuture = mock(CompletableFuture.class);
        Mockito.doReturn(MAPPER.writeValueAsBytes(updateSchemaResponse)).when(updateSchemaFuture).get();
        Mockito.doReturn(updateSchemaFuture).when(storeClient).getRaw("update_schema/" + storeName + "/" + (i + 1));

        MultiSchemaResponse.Schema schema = new MultiSchemaResponse.Schema();
        schema.setId(i + 1);
        schema.setDerivedSchemaId(1);
        schema.setSchemaStr(updateSchemas.get(i).toString());
        multiSchemas[i] = schema;
      }
      allUpdateSchemaResponse.setSchemas(multiSchemas);
      CompletableFuture<byte[]> allUpdateSchemaFuture = mock(CompletableFuture.class);
      Mockito.doReturn(MAPPER.writeValueAsBytes(allUpdateSchemaResponse)).when(allUpdateSchemaFuture).get();
      Mockito.doReturn(allUpdateSchemaFuture).when(storeClient).getRaw("update_schema/" + storeName);
    } else {
      for (int i = 0; i < updateSchemas.size(); i++) {
        SchemaResponse noUpdateSchemaResponse = new SchemaResponse();
        noUpdateSchemaResponse
            .setError("Update schema doesn't exist for value schema id: " + (i + 1) + " of store: " + storeName);

        CompletableFuture<byte[]> updateSchemaFuture = mock(CompletableFuture.class);
        Mockito.doReturn(MAPPER.writeValueAsBytes(noUpdateSchemaResponse)).when(updateSchemaFuture).get();
        Mockito.doReturn(updateSchemaFuture).when(storeClient).getRaw("update_schema/" + storeName + "/" + (i + 1));
      }

      MultiSchemaResponse allUpdateSchemaResponse = new MultiSchemaResponse();
      allUpdateSchemaResponse.setCluster(clusterName);
      allUpdateSchemaResponse.setName(storeName);

      MultiSchemaResponse.Schema[] multiSchemas = new MultiSchemaResponse.Schema[0];
      allUpdateSchemaResponse.setSchemas(multiSchemas);
      CompletableFuture<byte[]> allUpdateSchemaFuture = mock(CompletableFuture.class);
      Mockito.doReturn(MAPPER.writeValueAsBytes(allUpdateSchemaResponse)).when(allUpdateSchemaFuture).get();
      Mockito.doReturn(allUpdateSchemaFuture).when(storeClient).getRaw("update_schema/" + storeName);
    }
  }

  private AbstractAvroStoreClient getMockStoreClientWithRequestTopicResponse(byte[] requestTopicResponse)
      throws IOException, ExecutionException, InterruptedException {
    String storeName = "test_store";

    AbstractAvroStoreClient storeClient = mock(AbstractAvroStoreClient.class);
    Mockito.doReturn(storeName).when(storeClient).getStoreName();

    CompletableFuture<byte[]> requestTopicFuture = mock(CompletableFuture.class);
    Mockito.doReturn(requestTopicResponse).when(requestTopicFuture).get();
    Mockito.doReturn(requestTopicFuture).when(storeClient).getRaw("request_topic/" + storeName);

    return storeClient;
  }

  private static class TestOnlineVeniceProducer<K, V> extends OnlineVeniceProducer<K, V> {
    // Creating globally to access the same object in tests
    private VeniceWriter<byte[], byte[], byte[]> mockVeniceWriter;
    private boolean failPubSubWrites;

    public TestOnlineVeniceProducer(
        AbstractAvroStoreClient storeClient,
        SchemaReader kmeSchemaReader,
        VeniceProperties backendConfigs,
        MetricsRepository metricsRepository) {
      this(storeClient, kmeSchemaReader, backendConfigs, metricsRepository, false);
    }

    public TestOnlineVeniceProducer(
        AbstractAvroStoreClient storeClient,
        SchemaReader kmeSchemaReader,
        VeniceProperties backendConfigs,
        MetricsRepository metricsRepository,
        boolean failPubSubWrites) {
      super(storeClient, kmeSchemaReader, backendConfigs, metricsRepository, null);
      this.failPubSubWrites = failPubSubWrites;

      configureVeniceWriteMock();
    }

    @Override
    protected VeniceWriter<byte[], byte[], byte[]> constructVeniceWriter(
        Properties properties,
        VeniceWriterOptions writerOptions) {
      if (mockVeniceWriter == null) {
        mockVeniceWriter = Mockito.mock(VeniceWriter.class);
      }
      return mockVeniceWriter;
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
    return FastSerializerDeserializerFactory.getAvroGenericSerializer(schema);
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
}
