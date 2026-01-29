package com.linkedin.davinci.transformer;

import static com.linkedin.venice.utils.TestUtils.DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;
import static org.apache.avro.Schema.Type.STRING;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.davinci.client.DaVinciRecordTransformerUtility;
import com.linkedin.davinci.client.InternalDaVinciRecordTransformer;
import com.linkedin.davinci.client.InternalDaVinciRecordTransformerConfig;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerDaVinciRecordTransformerImpl;
import com.linkedin.davinci.stats.AggVersionedDaVinciRecordTransformerStats;
import com.linkedin.davinci.store.AbstractStorageIterator;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.StoragePartitionAdjustmentTrigger;
import com.linkedin.venice.compression.NoopCompressor;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.serializer.AvroGenericDeserializer;
import com.linkedin.venice.serializer.AvroSpecificDeserializer;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class RecordTransformerTest {
  static final String storeName = "test-store";
  static final int storeVersion = 1;
  static final int partitionId = 0;
  static final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
      AvroProtocolDefinition.PARTITION_STATE.getSerializer();
  static final Lazy<Integer> lazyKey = Lazy.of(() -> 42);
  static final String value = "SampleValue";
  static final Lazy<String> lazyValue = Lazy.of(() -> value);
  static final Schema keySchema = Schema.create(Schema.Type.INT);
  static final Schema valueSchema = Schema.create(STRING);

  @Test
  public void testRecordTransformer() {
    DaVinciRecordTransformerConfig dummyRecordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestStringRecordTransformer::new)
            .setStoreRecordsInDaVinci(false)
            .build();
    assertTrue(
        dummyRecordTransformerConfig.isRecordTransformationEnabled(),
        "Default for RecordTransformationEnabled should be true");
    assertFalse(dummyRecordTransformerConfig.useSpecificRecordKeyDeserializer());
    assertFalse(dummyRecordTransformerConfig.useSpecificRecordValueDeserializer());

    DaVinciRecordTransformer<Integer, String, String> recordTransformer = new TestStringRecordTransformer(
        storeName,
        storeVersion,
        keySchema,
        valueSchema,
        valueSchema,
        dummyRecordTransformerConfig);
    assertEquals(recordTransformer.getStoreName(), storeName);
    assertEquals(recordTransformer.getStoreVersion(), storeVersion);

    assertEquals(recordTransformer.getKeySchema().getType(), Schema.Type.INT);
    assertEquals(recordTransformer.getOutputValueSchema().getType(), STRING);

    DaVinciRecordTransformerUtility<Integer, String> recordTransformerUtility =
        recordTransformer.getRecordTransformerUtility();

    assertTrue(recordTransformerUtility.getKeyDeserializer() instanceof AvroGenericDeserializer);
    assertFalse(recordTransformerUtility.getKeyDeserializer() instanceof AvroSpecificDeserializer);

    DaVinciRecordTransformerResult<String> transformerResult =
        recordTransformer.transform(lazyKey, lazyValue, partitionId, null);
    recordTransformer.processPut(lazyKey, lazyValue, partitionId, null);
    assertEquals(transformerResult.getResult(), DaVinciRecordTransformerResult.Result.TRANSFORMED);
    assertEquals(transformerResult.getValue(), value + "Transformed");
    assertNull(recordTransformer.transformAndProcessPut(lazyKey, lazyValue, partitionId, null));

    recordTransformer.processDelete(lazyKey, partitionId, null);

    assertFalse(recordTransformer.getStoreRecordsInDaVinci());

    int classHash = recordTransformer.getClassHash();
    OffsetRecord offsetRecord = new OffsetRecord(partitionStateSerializer, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    assertTrue(recordTransformerUtility.hasTransformerLogicChanged(classHash, offsetRecord));
    offsetRecord.setRecordTransformerClassHash(classHash);
    assertFalse(recordTransformerUtility.hasTransformerLogicChanged(classHash, offsetRecord));
  }

  @Test
  public void testRecordTransformationDisabled() {
    DaVinciRecordTransformerConfig dummyRecordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestStringRecordTransformer::new)
            .setRecordTransformationEnabled(false)
            .build();
    assertFalse(dummyRecordTransformerConfig.isRecordMetadataEnabled());

    DaVinciRecordTransformer<Integer, String, String> recordTransformer = new TestStringRecordTransformer(
        storeName,
        storeVersion,
        keySchema,
        valueSchema,
        valueSchema,
        dummyRecordTransformerConfig);
    DaVinciRecordTransformerUtility<Integer, String> recordTransformerUtility =
        recordTransformer.getRecordTransformerUtility();
    int classHash = recordTransformer.getClassHash();
    OffsetRecord offsetRecord = new OffsetRecord(partitionStateSerializer, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);

    assertFalse(
        recordTransformerUtility.hasTransformerLogicChanged(classHash, offsetRecord),
        "When recordTransformationEnabled is set to false, hasTransformerLogicChanged should return false");
  }

  @Test
  public void testOnRecovery() {
    DaVinciRecordTransformerConfig recordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestStringRecordTransformer::new)
            .setOutputValueSchema(Schema.create(STRING))
            .setOutputValueClass(String.class)
            .build();

    DaVinciRecordTransformer<Integer, String, String> recordTransformer = new TestStringRecordTransformer(
        storeName,
        storeVersion,
        keySchema,
        valueSchema,
        valueSchema,
        recordTransformerConfig);
    assertEquals(recordTransformer.getStoreVersion(), storeVersion);

    AbstractStorageIterator iterator = mock(AbstractStorageIterator.class);
    when(iterator.isValid()).thenReturn(true).thenReturn(false);
    when(iterator.key()).thenReturn("mockKey".getBytes());

    int schemaId = 1;
    String value = "mockValue";
    VeniceCompressor compressor = new NoopCompressor();
    when(iterator.value()).thenReturn(recordTransformer.prependSchemaIdToHeader(value, schemaId, compressor).array());

    StorageEngine storageEngine = mock(StorageEngine.class);
    Lazy<VeniceCompressor> lazyCompressor = Lazy.of(() -> compressor);

    OffsetRecord offsetRecord = new OffsetRecord(partitionStateSerializer, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    when(storageEngine.getPartitionOffset(partitionId, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING))
        .thenReturn(Optional.of(offsetRecord));

    Map<Integer, Schema> schemaIdToSchemaMap = spy(new VeniceConcurrentHashMap<>());
    ReadOnlySchemaRepository schemaRepository = mock(ReadOnlySchemaRepository.class);
    SchemaEntry schemaEntry = new SchemaEntry(schemaId, recordTransformer.getOutputValueSchema());
    when(schemaRepository.getValueSchema(anyString(), anyInt())).thenReturn(schemaEntry);

    recordTransformer.onRecovery(
        storageEngine,
        partitionId,
        partitionStateSerializer,
        lazyCompressor,
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING,
        schemaIdToSchemaMap,
        schemaRepository);
    verify(schemaIdToSchemaMap, never()).computeIfAbsent(any(), any());
    verify(schemaRepository, never()).getValueSchema(any(), anyInt());
    verify(storageEngine).clearPartitionOffset(partitionId);

    // Reset the mock to clear previous interactions
    reset(storageEngine);

    offsetRecord.setRecordTransformerClassHash(recordTransformer.getClassHash());
    assertEquals((int) offsetRecord.getRecordTransformerClassHash(), recordTransformer.getClassHash());

    // class hash should be the same when the OffsetRecord is serialized then deserialized
    byte[] offsetRecordBytes = offsetRecord.toBytes();
    OffsetRecord deserializedOffsetRecord =
        new OffsetRecord(offsetRecordBytes, partitionStateSerializer, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    assertEquals((int) deserializedOffsetRecord.getRecordTransformerClassHash(), recordTransformer.getClassHash());

    when(storageEngine.getPartitionOffset(partitionId, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING))
        .thenReturn(Optional.of(offsetRecord));

    // Execute the onRecovery method again to test the case where the classHash exists
    when(storageEngine.getIterator(partitionId)).thenReturn(iterator);
    recordTransformer.onRecovery(
        storageEngine,
        partitionId,
        partitionStateSerializer,
        lazyCompressor,
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING,
        schemaIdToSchemaMap,
        schemaRepository);
    verify(storageEngine, never()).clearPartitionOffset(partitionId);
    verify(storageEngine).getIterator(partitionId);
    verify(schemaIdToSchemaMap).computeIfAbsent(any(), any());
    verify(schemaRepository).getValueSchema(any(), anyInt());
    verify(iterator).close();

    // Ensure partition is put into read-only mode before iterating, and adjusted to default settings after
    verify(storageEngine)
        .adjustStoragePartition(eq(partitionId), eq(StoragePartitionAdjustmentTrigger.PREPARE_FOR_READ), any());
    verify(storageEngine)
        .adjustStoragePartition(eq(partitionId), eq(StoragePartitionAdjustmentTrigger.REOPEN_WITH_DEFAULTS), any());
  }

  @Test
  public void testOnRecoveryAlwaysBootstrapFromVersionTopic() {
    DaVinciRecordTransformerConfig dummyRecordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestStringRecordTransformer::new)
            .setAlwaysBootstrapFromVersionTopic(true)
            .build();

    DaVinciRecordTransformer<Integer, String, String> recordTransformer = new TestStringRecordTransformer(
        storeName,
        storeVersion,
        keySchema,
        valueSchema,
        valueSchema,
        dummyRecordTransformerConfig);
    assertEquals(recordTransformer.getStoreVersion(), storeVersion);

    StorageEngine storageEngine = mock(StorageEngine.class);
    Lazy<VeniceCompressor> compressor = Lazy.of(() -> mock(VeniceCompressor.class));

    OffsetRecord offsetRecord = new OffsetRecord(partitionStateSerializer, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    when(storageEngine.getPartitionOffset(partitionId, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING))
        .thenReturn(Optional.of(offsetRecord));

    recordTransformer.onRecovery(
        storageEngine,
        partitionId,
        partitionStateSerializer,
        compressor,
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING,
        null,
        null);
    verify(storageEngine).clearPartitionOffset(partitionId);

    // Reset the mock to clear previous interactions
    reset(storageEngine);

    offsetRecord.setRecordTransformerClassHash(recordTransformer.getClassHash());
    assertEquals((int) offsetRecord.getRecordTransformerClassHash(), recordTransformer.getClassHash());

    when(storageEngine.getPartitionOffset(partitionId, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING))
        .thenReturn(Optional.of(offsetRecord));

    // Execute the onRecovery method again to test the case where the classHash exists
    recordTransformer.onRecovery(
        storageEngine,
        partitionId,
        partitionStateSerializer,
        compressor,
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING,
        null,
        null);
    verify(storageEngine).clearPartitionOffset(partitionId);
    verify(storageEngine, never()).getIterator(partitionId);
  }

  @Test
  public void testOnRecoveryStoreRecordsInDaVinciDisabled() {
    DaVinciRecordTransformerConfig dummyRecordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestStringRecordTransformer::new)
            .setStoreRecordsInDaVinci(false)
            .build();

    DaVinciRecordTransformer<Integer, String, String> recordTransformer = new TestStringRecordTransformer(
        storeName,
        storeVersion,
        keySchema,
        valueSchema,
        valueSchema,
        dummyRecordTransformerConfig);
    assertEquals(recordTransformer.getStoreVersion(), storeVersion);

    StorageEngine storageEngine = mock(StorageEngine.class);
    Lazy<VeniceCompressor> compressor = Lazy.of(() -> mock(VeniceCompressor.class));

    OffsetRecord offsetRecord = new OffsetRecord(partitionStateSerializer, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    when(storageEngine.getPartitionOffset(partitionId, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING))
        .thenReturn(Optional.of(offsetRecord));

    recordTransformer.onRecovery(
        storageEngine,
        partitionId,
        partitionStateSerializer,
        compressor,
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING,
        null,
        null);
    verify(storageEngine, times(1)).clearPartitionOffset(partitionId);

    // Reset the mock to clear previous interactions
    reset(storageEngine);

    offsetRecord.setRecordTransformerClassHash(recordTransformer.getClassHash());
    assertEquals((int) offsetRecord.getRecordTransformerClassHash(), recordTransformer.getClassHash());

    when(storageEngine.getPartitionOffset(partitionId, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING))
        .thenReturn(Optional.of(offsetRecord));

    // Execute the onRecovery method again to test the case where the classHash exists
    recordTransformer.onRecovery(
        storageEngine,
        partitionId,
        partitionStateSerializer,
        compressor,
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING,
        null,
        null);
    // It should No-Op
    verify(storageEngine, never()).clearPartitionOffset(partitionId);
    verify(storageEngine, never()).getIterator(partitionId);
  }

  @Test
  public void testInternalRecordTransformer() {
    DaVinciRecordTransformerConfig dummyRecordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestStringRecordTransformer::new)
            .build();

    InternalDaVinciRecordTransformerConfig internalRecordTransformerConfig = new InternalDaVinciRecordTransformerConfig(
        dummyRecordTransformerConfig,
        mock(AggVersionedDaVinciRecordTransformerStats.class));

    internalRecordTransformerConfig.setStartConsumptionLatchCount(1);
    assertThrows(() -> internalRecordTransformerConfig.setStartConsumptionLatchCount(2));

    DaVinciRecordTransformer<Integer, String, String> clientRecordTransformer = spy(
        new TestStringRecordTransformer(
            storeName,
            storeVersion,
            keySchema,
            valueSchema,
            valueSchema,
            dummyRecordTransformerConfig));
    assertEquals(clientRecordTransformer.getStoreVersion(), storeVersion);

    InternalDaVinciRecordTransformer<Integer, String, String> internalRecordTransformer =
        new InternalDaVinciRecordTransformer<>(
            clientRecordTransformer,
            keySchema,
            valueSchema,
            valueSchema,
            internalRecordTransformerConfig);
    assertEquals(internalRecordTransformer.getStoreName(), storeName);
    internalRecordTransformer.onStartVersionIngestion(1, true);
    verify(clientRecordTransformer).onStartVersionIngestion(1, true);

    assertEquals(internalRecordTransformer.getCountDownStartConsumptionLatchCount(), 1L);
    assertTrue(internalRecordTransformer.getStoreRecordsInDaVinci());
    assertEquals(internalRecordTransformer.getKeySchema().getType(), Schema.Type.INT);
    assertEquals(internalRecordTransformer.getOutputValueSchema().getType(), STRING);

    internalRecordTransformer.countDownStartConsumptionLatch();
    assertEquals(internalRecordTransformer.getCountDownStartConsumptionLatchCount(), 0L);

    DaVinciRecordTransformerResult<String> recordTransformerResult =
        internalRecordTransformer.transformAndProcessPut(lazyKey, lazyValue, partitionId, null);
    verify(clientRecordTransformer).transform(eq(lazyKey), eq(lazyValue), eq(partitionId), any());
    verify(clientRecordTransformer).processPut(eq(lazyKey), any(), eq(partitionId), any());
    assertEquals(recordTransformerResult.getValue(), value + "Transformed");

    internalRecordTransformer.processDelete(lazyKey, partitionId, null);
    verify(clientRecordTransformer).processDelete(eq(lazyKey), eq(partitionId), any());

    internalRecordTransformer.onEndVersionIngestion(storeVersion);
    verify(clientRecordTransformer).onEndVersionIngestion(storeVersion);

    StorageEngine storageEngine = mock(StorageEngine.class);
    Lazy<VeniceCompressor> compressor = Lazy.of(() -> mock(VeniceCompressor.class));

    OffsetRecord offsetRecord = new OffsetRecord(partitionStateSerializer, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    when(storageEngine.getPartitionOffset(partitionId, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING))
        .thenReturn(Optional.of(offsetRecord));
    internalRecordTransformer.internalOnRecovery(
        storageEngine,
        partitionId,
        partitionStateSerializer,
        compressor,
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING,
        null,
        null);
    verify(clientRecordTransformer).onRecovery(
        storageEngine,
        partitionId,
        partitionStateSerializer,
        compressor,
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING,
        null,
        null);
  }

  @Test
  public void testInternalRecordTransformerVersionSwap() {
    int currentVersion = 1;
    int futureVersion = 2;

    DaVinciRecordTransformerConfig dummyRecordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestStringRecordTransformer::new)
            .build();

    InternalDaVinciRecordTransformerConfig internalRecordTransformerConfig = new InternalDaVinciRecordTransformerConfig(
        dummyRecordTransformerConfig,
        mock(AggVersionedDaVinciRecordTransformerStats.class));

    VeniceChangelogConsumerDaVinciRecordTransformerImpl.DaVinciRecordTransformerChangelogConsumer clientRecordTransformer =
        mock(VeniceChangelogConsumerDaVinciRecordTransformerImpl.DaVinciRecordTransformerChangelogConsumer.class);
    InternalDaVinciRecordTransformer<Integer, String, String> internalRecordTransformer =
        new InternalDaVinciRecordTransformer<>(
            clientRecordTransformer,
            keySchema,
            valueSchema,
            valueSchema,
            internalRecordTransformerConfig);

    internalRecordTransformer.onVersionSwap(currentVersion, futureVersion, partitionId);
    verify(clientRecordTransformer).onVersionSwap(currentVersion, futureVersion, partitionId);
  }

  @Test
  public void testSpecificRecordTransformer() {
    Schema keySchema = TestSpecificKey.SCHEMA$;
    Schema valueSchema = TestSpecificValue.SCHEMA$;

    DaVinciRecordTransformerConfig dummyRecordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestSpecificRecordTransformer::new)
            .setKeyClass(TestSpecificKey.class)
            .setOutputValueSchema(valueSchema)
            .setOutputValueClass(TestSpecificValue.class)
            .build();

    assertTrue(dummyRecordTransformerConfig.useSpecificRecordKeyDeserializer());
    assertTrue(dummyRecordTransformerConfig.useSpecificRecordValueDeserializer());

    DaVinciRecordTransformer<TestSpecificKey, TestSpecificValue, TestSpecificValue> recordTransformer =
        new TestSpecificRecordTransformer(
            storeName,
            storeVersion,
            keySchema,
            valueSchema,
            valueSchema,
            dummyRecordTransformerConfig);

    DaVinciRecordTransformerUtility<TestSpecificKey, TestSpecificValue> recordTransformerUtility =
        recordTransformer.getRecordTransformerUtility();

    assertTrue(recordTransformerUtility.getKeyDeserializer() instanceof AvroSpecificDeserializer);

    TestSpecificKey specificKey = new TestSpecificKey();
    int id = 123;
    specificKey.id = id;
    Lazy<TestSpecificKey> lazyKey = Lazy.of(() -> specificKey);

    TestSpecificValue specificValue = new TestSpecificValue();
    String firstName = "first";
    String lastName = "last";
    specificValue.firstName = firstName;
    specificValue.lastName = lastName;
    Lazy<TestSpecificValue> lazyValue = Lazy.of(() -> specificValue);

    DaVinciRecordTransformerResult<TestSpecificValue> transformerResult =
        recordTransformer.transform(lazyKey, lazyValue, partitionId, null);
    assertEquals(transformerResult.getResult(), DaVinciRecordTransformerResult.Result.TRANSFORMED);
    TestSpecificValue transformedSpecificValue = transformerResult.getValue();
    assertEquals(transformedSpecificValue.firstName, firstName + id);
    assertEquals(transformedSpecificValue.lastName, lastName + id);
  }

  @Test
  public void testBlockingRecordTransformerUsingUniformValueSchema() {
    DaVinciRecordTransformerConfig dummyRecordTransformerConfig = new DaVinciRecordTransformerConfig.Builder()
        .setRecordTransformerFunction(TestRecordTransformerUsingUniformInputValueSchema::new)
        .setStoreRecordsInDaVinci(false)
        .build();

    InternalDaVinciRecordTransformerConfig internalRecordTransformerConfig = new InternalDaVinciRecordTransformerConfig(
        dummyRecordTransformerConfig,
        mock(AggVersionedDaVinciRecordTransformerStats.class));

    DaVinciRecordTransformer<GenericRecord, GenericRecord, GenericRecord> recordTransformer =
        new TestRecordTransformerUsingUniformInputValueSchema(
            storeName,
            storeVersion,
            keySchema,
            valueSchema,
            valueSchema,
            dummyRecordTransformerConfig);

    assertTrue(recordTransformer.useUniformInputValueSchema());

    InternalDaVinciRecordTransformer internalRecordTransformer = new InternalDaVinciRecordTransformer(
        recordTransformer,
        keySchema,
        valueSchema,
        valueSchema,
        internalRecordTransformerConfig);

    assertTrue(internalRecordTransformer.useUniformInputValueSchema());
  }
}
