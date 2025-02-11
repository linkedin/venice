package com.linkedin.davinci.transformer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.client.BlockingDaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.davinci.client.DaVinciRecordTransformerUtility;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.AbstractStorageIterator;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.Optional;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class RecordTransformerTest {
  static final int storeVersion = 1;
  static final int partitionId = 0;
  static final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
      AvroProtocolDefinition.PARTITION_STATE.getSerializer();

  @Test
  public void testRecordTransformer() {
    Schema keySchema = Schema.create(Schema.Type.INT);
    Schema valueSchema = Schema.create(Schema.Type.STRING);

    DaVinciRecordTransformerConfig dummyRecordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestStringRecordTransformer::new)
            .setStoreRecordsInDaVinci(false)
            .build();

    DaVinciRecordTransformer<Integer, String, String> recordTransformer = new TestStringRecordTransformer(
        storeVersion,
        keySchema,
        valueSchema,
        valueSchema,
        dummyRecordTransformerConfig);
    assertEquals(recordTransformer.getStoreVersion(), storeVersion);

    assertEquals(recordTransformer.getKeySchema().getType(), Schema.Type.INT);
    assertEquals(recordTransformer.getOutputValueSchema().getType(), Schema.Type.STRING);

    Lazy<Integer> lazyKey = Lazy.of(() -> 42);
    Lazy<String> lazyValue = Lazy.of(() -> "SampleValue");
    DaVinciRecordTransformerResult<String> transformerResult = recordTransformer.transform(lazyKey, lazyValue);
    recordTransformer.processPut(lazyKey, lazyValue);
    assertEquals(transformerResult.getResult(), DaVinciRecordTransformerResult.Result.TRANSFORMED);
    assertEquals(transformerResult.getValue(), "SampleValueTransformed");
    assertNull(recordTransformer.transformAndProcessPut(lazyKey, lazyValue));

    recordTransformer.processDelete(lazyKey);

    assertFalse(recordTransformer.getStoreRecordsInDaVinci());

    int classHash = recordTransformer.getClassHash();

    DaVinciRecordTransformerUtility<Integer, String> recordTransformerUtility =
        recordTransformer.getRecordTransformerUtility();
    OffsetRecord offsetRecord = new OffsetRecord(partitionStateSerializer);

    assertTrue(recordTransformerUtility.hasTransformerLogicChanged(classHash, offsetRecord));

    offsetRecord.setRecordTransformerClassHash(classHash);

    assertFalse(recordTransformerUtility.hasTransformerLogicChanged(classHash, offsetRecord));
  }

  @Test
  public void testOnRecovery() {
    Schema keySchema = Schema.create(Schema.Type.INT);
    Schema valueSchema = Schema.create(Schema.Type.STRING);

    DaVinciRecordTransformerConfig dummyRecordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestStringRecordTransformer::new)
            .build();

    DaVinciRecordTransformer<Integer, String, String> recordTransformer = new TestStringRecordTransformer(
        storeVersion,
        keySchema,
        valueSchema,
        valueSchema,
        dummyRecordTransformerConfig);
    assertEquals(recordTransformer.getStoreVersion(), storeVersion);

    AbstractStorageIterator iterator = mock(AbstractStorageIterator.class);
    when(iterator.isValid()).thenReturn(true).thenReturn(false);
    when(iterator.key()).thenReturn("mockKey".getBytes());
    when(iterator.value()).thenReturn("mockValue".getBytes());

    AbstractStorageEngine storageEngine = mock(AbstractStorageEngine.class);
    Lazy<VeniceCompressor> compressor = Lazy.of(() -> mock(VeniceCompressor.class));

    OffsetRecord offsetRecord = new OffsetRecord(partitionStateSerializer);
    when(storageEngine.getPartitionOffset(partitionId)).thenReturn(Optional.of(offsetRecord));

    recordTransformer.onRecovery(storageEngine, partitionId, partitionStateSerializer, compressor);
    verify(storageEngine, times(1)).clearPartitionOffset(partitionId);

    // Reset the mock to clear previous interactions
    reset(storageEngine);

    offsetRecord.setRecordTransformerClassHash(recordTransformer.getClassHash());
    assertEquals((int) offsetRecord.getRecordTransformerClassHash(), recordTransformer.getClassHash());

    // class hash should be the same when the OffsetRecord is serialized then deserialized
    byte[] offsetRecordBytes = offsetRecord.toBytes();
    OffsetRecord deserializedOffsetRecord = new OffsetRecord(offsetRecordBytes, partitionStateSerializer);
    assertEquals((int) deserializedOffsetRecord.getRecordTransformerClassHash(), recordTransformer.getClassHash());

    when(storageEngine.getPartitionOffset(partitionId)).thenReturn(Optional.of(offsetRecord));

    // Execute the onRecovery method again to test the case where the classHash exists
    when(storageEngine.getIterator(partitionId)).thenReturn(iterator);
    recordTransformer.onRecovery(storageEngine, partitionId, partitionStateSerializer, compressor);
    verify(storageEngine, never()).clearPartitionOffset(partitionId);
    verify(storageEngine, times(1)).getIterator(partitionId);
  }

  @Test
  public void testOnRecoveryAlwaysBootstrapFromVersionTopic() {
    Schema keySchema = Schema.create(Schema.Type.INT);
    Schema valueSchema = Schema.create(Schema.Type.STRING);

    DaVinciRecordTransformerConfig dummyRecordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestStringRecordTransformer::new)
            .setAlwaysBootstrapFromVersionTopic(true)
            .build();

    DaVinciRecordTransformer<Integer, String, String> recordTransformer = new TestStringRecordTransformer(
        storeVersion,
        keySchema,
        valueSchema,
        valueSchema,
        dummyRecordTransformerConfig);
    assertEquals(recordTransformer.getStoreVersion(), storeVersion);

    AbstractStorageEngine storageEngine = mock(AbstractStorageEngine.class);
    Lazy<VeniceCompressor> compressor = Lazy.of(() -> mock(VeniceCompressor.class));

    OffsetRecord offsetRecord = new OffsetRecord(partitionStateSerializer);
    when(storageEngine.getPartitionOffset(partitionId)).thenReturn(Optional.of(offsetRecord));

    recordTransformer.onRecovery(storageEngine, partitionId, partitionStateSerializer, compressor);
    verify(storageEngine, times(1)).clearPartitionOffset(partitionId);

    // Reset the mock to clear previous interactions
    reset(storageEngine);

    offsetRecord.setRecordTransformerClassHash(recordTransformer.getClassHash());
    assertEquals((int) offsetRecord.getRecordTransformerClassHash(), recordTransformer.getClassHash());

    when(storageEngine.getPartitionOffset(partitionId)).thenReturn(Optional.of(offsetRecord));

    // Execute the onRecovery method again to test the case where the classHash exists
    recordTransformer.onRecovery(storageEngine, partitionId, partitionStateSerializer, compressor);
    verify(storageEngine, times(1)).clearPartitionOffset(partitionId);
    verify(storageEngine, never()).getIterator(partitionId);
  }

  @Test
  public void testBlockingRecordTransformer() {
    Schema keySchema = Schema.create(Schema.Type.INT);
    Schema valueSchema = Schema.create(Schema.Type.STRING);

    DaVinciRecordTransformerConfig dummyRecordTransformerConfig =
        new DaVinciRecordTransformerConfig.Builder().setRecordTransformerFunction(TestStringRecordTransformer::new)
            .build();

    DaVinciRecordTransformer<Integer, String, String> recordTransformer = new TestStringRecordTransformer(
        storeVersion,
        keySchema,
        valueSchema,
        valueSchema,
        dummyRecordTransformerConfig);
    assertEquals(recordTransformer.getStoreVersion(), storeVersion);

    recordTransformer = new BlockingDaVinciRecordTransformer<>(
        recordTransformer,
        keySchema,
        valueSchema,
        valueSchema,
        dummyRecordTransformerConfig);
    recordTransformer.onStartVersionIngestion(true);

    assertTrue(recordTransformer.getStoreRecordsInDaVinci());

    assertEquals(recordTransformer.getKeySchema().getType(), Schema.Type.INT);

    assertEquals(recordTransformer.getOutputValueSchema().getType(), Schema.Type.STRING);

    Lazy<Integer> lazyKey = Lazy.of(() -> 42);
    Lazy<String> lazyValue = Lazy.of(() -> "SampleValue");
    DaVinciRecordTransformerResult<String> recordTransformerResult =
        recordTransformer.transformAndProcessPut(lazyKey, lazyValue);
    assertEquals(recordTransformerResult.getValue(), "SampleValueTransformed");

    recordTransformer.processDelete(lazyKey);

    recordTransformer.onEndVersionIngestion(2);
  }
}
