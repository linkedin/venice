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
import java.io.File;
import java.util.Optional;
import org.apache.avro.Schema;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class RecordTransformerTest {
  static final int storeVersion = 1;
  static final int partitionId = 0;
  static final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
      AvroProtocolDefinition.PARTITION_STATE.getSerializer();

  @BeforeMethod
  @AfterClass
  public void deleteClassHash() {
    File file = new File(String.format("./classHash-%d.txt", storeVersion));
    if (file.exists()) {
      assertTrue(file.delete());
    }
  }

  @Test
  public void testRecordTransformer() {
    Schema keySchema = Schema.create(Schema.Type.INT);
    Schema valueSchema = Schema.create(Schema.Type.STRING);

    DaVinciRecordTransformer<Integer, String, String> recordTransformer =
        new TestStringRecordTransformer(storeVersion, keySchema, valueSchema, valueSchema, false);
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
    AbstractStorageEngine storageEngine = mock(AbstractStorageEngine.class);

    OffsetRecord offsetRecord = new OffsetRecord(partitionStateSerializer);
    when(storageEngine.getPartitionOffset(partitionId)).thenReturn(Optional.of(offsetRecord));

    assertTrue(
        recordTransformerUtility
            .hasTransformerLogicChanged(storageEngine, partitionId, partitionStateSerializer, classHash));
    assertFalse(
        recordTransformerUtility
            .hasTransformerLogicChanged(storageEngine, partitionId, partitionStateSerializer, classHash));
  }

  @Test
  public void testOnRecovery() {
    Schema keySchema = Schema.create(Schema.Type.INT);
    Schema valueSchema = Schema.create(Schema.Type.STRING);

    DaVinciRecordTransformer<Integer, String, String> recordTransformer =
        new TestStringRecordTransformer(storeVersion, keySchema, valueSchema, valueSchema, true);
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

    when(storageEngine.getPartitionOffset(partitionId)).thenReturn(Optional.of(offsetRecord));

    // Execute the onRecovery method again to test the case where the classHash exists
    when(storageEngine.getIterator(partitionId)).thenReturn(iterator);
    recordTransformer.onRecovery(storageEngine, partitionId, partitionStateSerializer, compressor);
    verify(storageEngine, never()).clearPartitionOffset(partitionId);
    verify(storageEngine, times(1)).getIterator(partitionId);
  }

  @Test
  public void testBlockingRecordTransformer() {
    Schema keySchema = Schema.create(Schema.Type.INT);
    Schema valueSchema = Schema.create(Schema.Type.STRING);

    DaVinciRecordTransformer<Integer, String, String> recordTransformer =
        new TestStringRecordTransformer(storeVersion, keySchema, valueSchema, valueSchema, true);
    assertEquals(recordTransformer.getStoreVersion(), storeVersion);

    recordTransformer = new BlockingDaVinciRecordTransformer<>(
        recordTransformer,
        keySchema,
        valueSchema,
        valueSchema,
        recordTransformer.getStoreRecordsInDaVinci());
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
