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
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.File;
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
    assertTrue(recordTransformerUtility.hasTransformerLogicChanged(classHash));
    assertFalse(recordTransformerUtility.hasTransformerLogicChanged(classHash));
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

    int partitionNumber = 1;
    recordTransformer.onRecovery(storageEngine, partitionNumber, compressor);
    verify(storageEngine, times(1)).clearPartitionOffset(partitionNumber);

    // Reset the mock to clear previous interactions
    reset(storageEngine);

    // Execute the onRecovery method again to test the case where the classHash file exists
    when(storageEngine.getIterator(partitionNumber)).thenReturn(iterator);
    recordTransformer.onRecovery(storageEngine, partitionNumber, compressor);
    verify(storageEngine, never()).clearPartitionOffset(partitionNumber);
    verify(storageEngine, times(1)).getIterator(partitionNumber);
  }

  @Test
  public void testBlockingRecordTransformer() {
    Schema keySchema = Schema.create(Schema.Type.INT);
    Schema valueSchema = Schema.create(Schema.Type.STRING);

    DaVinciRecordTransformer<Integer, String, String> recordTransformer =
        new TestStringRecordTransformer(storeVersion, keySchema, valueSchema, valueSchema, false);
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
