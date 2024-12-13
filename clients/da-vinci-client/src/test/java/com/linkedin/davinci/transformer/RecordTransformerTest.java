package com.linkedin.davinci.transformer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
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
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.File;
import java.util.Optional;
import org.apache.avro.Schema;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class RecordTransformerTest {
  static final int storeVersion = 1;

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
    DaVinciRecordTransformer<Integer, String, String> recordTransformer =
        new TestStringRecordTransformer(storeVersion, false);
    assertEquals(recordTransformer.getStoreVersion(), storeVersion);

    Schema keySchema = recordTransformer.getKeySchema();
    assertEquals(keySchema.getType(), Schema.Type.INT);

    Schema outputValueSchema = recordTransformer.getOutputValueSchema();
    assertEquals(outputValueSchema.getType(), Schema.Type.STRING);

    Lazy<Integer> lazyKey = Lazy.of(() -> 42);
    Lazy<String> lazyValue = Lazy.of(() -> "SampleValue");
    DaVinciRecordTransformerResult<String> transformerResult = recordTransformer.transform(lazyKey, lazyValue);
    recordTransformer.processPut(lazyKey, lazyValue);
    assertEquals(transformerResult.getResult(), DaVinciRecordTransformerResult.Result.TRANSFORMED);
    assertEquals(transformerResult.getValue(), "SampleValueTransformed");
    assertNull(recordTransformer.transformAndProcessPut(lazyKey, lazyValue));

    recordTransformer.processDelete(lazyKey);

    assertFalse(recordTransformer.getStoreRecordsInDaVinci());
    AbstractStorageEngine storageEngine = mock(AbstractStorageEngine.class);
    int classHash = recordTransformer.getClassHash();
    int partitionNumber = 1;
    OffsetRecord nullOffsetRecord = mock(OffsetRecord.class);
    when(nullOffsetRecord.getTransformerClassHash()).thenReturn(null);

    OffsetRecord matchingOffsetRecord = mock(OffsetRecord.class);
    when(matchingOffsetRecord.getTransformerClassHash()).thenReturn(classHash);

    DaVinciRecordTransformerUtility<Integer, String> recordTransformerUtility =
        recordTransformer.getRecordTransformerUtility();
    when(storageEngine.getPartitionOffset(partitionNumber)).thenReturn(Optional.of(nullOffsetRecord));
    assertTrue(recordTransformerUtility.hasTransformerLogicChanged(storageEngine, partitionNumber, classHash));
    verify(storageEngine, times(1)).putPartitionOffset(eq(partitionNumber), any(OffsetRecord.class));
    when(storageEngine.getPartitionOffset(partitionNumber)).thenReturn(Optional.of(matchingOffsetRecord));
    assertFalse(recordTransformerUtility.hasTransformerLogicChanged(storageEngine, partitionNumber, classHash));
  }

  @Test
  public void testOnRecovery() {
    DaVinciRecordTransformer<Integer, String, String> recordTransformer =
        new TestStringRecordTransformer(storeVersion, true);

    AbstractStorageIterator iterator = mock(AbstractStorageIterator.class);
    when(iterator.isValid()).thenReturn(true).thenReturn(false);
    when(iterator.key()).thenReturn("mockKey".getBytes());
    when(iterator.value()).thenReturn("mockValue".getBytes());

    AbstractStorageEngine storageEngine = mock(AbstractStorageEngine.class);
    Lazy<VeniceCompressor> compressor = Lazy.of(() -> mock(VeniceCompressor.class));

    int partitionNumber = 1;
    recordTransformer.onRecovery(storageEngine, partitionNumber, compressor);
    verify(storageEngine, times(1)).clearPartitionOffset(partitionNumber);
  }

  @Test
  public void testBlockingRecordTransformer() {
    DaVinciRecordTransformer<Integer, String, String> recordTransformer = new TestStringRecordTransformer(0, true);
    recordTransformer =
        new BlockingDaVinciRecordTransformer<>(recordTransformer, recordTransformer.getStoreRecordsInDaVinci());
    recordTransformer.onStartVersionIngestion();

    assertTrue(recordTransformer.getStoreRecordsInDaVinci());

    Schema keySchema = recordTransformer.getKeySchema();
    assertEquals(keySchema.getType(), Schema.Type.INT);

    Schema outputValueSchema = recordTransformer.getOutputValueSchema();
    assertEquals(outputValueSchema.getType(), Schema.Type.STRING);

    Lazy<Integer> lazyKey = Lazy.of(() -> 42);
    Lazy<String> lazyValue = Lazy.of(() -> "SampleValue");
    DaVinciRecordTransformerResult<String> recordTransformerResult =
        recordTransformer.transformAndProcessPut(lazyKey, lazyValue);
    assertEquals(recordTransformerResult.getValue(), "SampleValueTransformed");

    recordTransformer.processDelete(lazyKey);

    recordTransformer.onEndVersionIngestion();
  }

}
