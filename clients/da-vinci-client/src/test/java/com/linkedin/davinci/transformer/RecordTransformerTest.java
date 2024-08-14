package com.linkedin.davinci.transformer;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import com.linkedin.davinci.client.BlockingDaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.avro.Schema;
import org.rocksdb.RocksIterator;
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
  public void testRecordTransformer() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
    DaVinciRecordTransformer<Integer, String, String> recordTransformer =
        new TestStringRecordTransformer(storeVersion, false);
    assertEquals(recordTransformer.getStoreVersion(), storeVersion);

    Schema keyOutputSchema = recordTransformer.getKeyOutputSchema();
    assertEquals(keyOutputSchema.getType(), Schema.Type.INT);

    Schema valueOutputSchema = recordTransformer.getValueOutputSchema();
    assertEquals(valueOutputSchema.getType(), Schema.Type.STRING);

    Lazy<Integer> lazyKey = Lazy.of(() -> 42);
    Lazy<String> lazyValue = Lazy.of(() -> "SampleValue");
    String transformedRecord = recordTransformer.transform(lazyKey, lazyValue);
    recordTransformer.processPut(lazyKey, lazyValue);
    assertEquals(transformedRecord, "SampleValueTransformed");
    assertNull(recordTransformer.transformAndProcessPut(lazyKey, lazyValue));

    recordTransformer.processDelete(lazyKey);
    String deletedRecord = recordTransformer.processDelete(lazyKey);
    assertNull(deletedRecord);

    assertFalse(recordTransformer.getStoreRecordsInDaVinci());
    Class<String> outputValueClass = recordTransformer.getOutputValueClass();
    assertEquals(outputValueClass, String.class);

    int classHash = recordTransformer.getClassHash();

    // Use reflection to access the hasTransformationLogicChanged method
    Method hasTransformationLogicChanged =
        DaVinciRecordTransformer.class.getDeclaredMethod("hasTransformationLogicChanged", int.class);
    hasTransformationLogicChanged.setAccessible(true);

    assertTrue((boolean) hasTransformationLogicChanged.invoke(recordTransformer, classHash));
    assertFalse((boolean) hasTransformationLogicChanged.invoke(recordTransformer, classHash));
  }

  @Test
  public void testOnRecovery() {
    DaVinciRecordTransformer<Integer, String, String> recordTransformer =
        new TestStringRecordTransformer(storeVersion, true);

    RocksIterator iterator = mock(RocksIterator.class);
    when(iterator.isValid()).thenReturn(true).thenReturn(false);
    when(iterator.key()).thenReturn("mockKey".getBytes());
    when(iterator.value()).thenReturn("mockValue".getBytes());

    AbstractStorageEngine storageEngine = mock(AbstractStorageEngine.class);

    int partitionNumber = 1;
    recordTransformer.onRecovery(storageEngine, partitionNumber);
    verify(storageEngine, times(1)).clearPartitionOffset(partitionNumber);

    // Reset the mock to clear previous interactions
    reset(storageEngine);

    // Execute the onRecovery method again to test the case where the classHash file exists
    when(storageEngine.getRocksDBIterator(partitionNumber)).thenReturn(iterator);
    recordTransformer.onRecovery(storageEngine, partitionNumber);
    verify(storageEngine, never()).clearPartitionOffset(partitionNumber);
    verify(storageEngine, times(1)).getRocksDBIterator(partitionNumber);
  }

  @Test
  public void testBlockingRecordTransformer() {
    DaVinciRecordTransformer<Integer, String, String> recordTransformer = new TestStringRecordTransformer(0, true);
    recordTransformer =
        new BlockingDaVinciRecordTransformer<>(recordTransformer, recordTransformer.getStoreRecordsInDaVinci());
    recordTransformer.onStart();

    assertTrue(recordTransformer.getStoreRecordsInDaVinci());

    Schema keyOutputSchema = recordTransformer.getKeyOutputSchema();
    assertEquals(keyOutputSchema.getType(), Schema.Type.INT);

    Schema valueOutputSchema = recordTransformer.getValueOutputSchema();
    assertEquals(valueOutputSchema.getType(), Schema.Type.STRING);

    Lazy<Integer> lazyKey = Lazy.of(() -> 42);
    Lazy<String> lazyValue = Lazy.of(() -> "SampleValue");
    assertEquals(recordTransformer.transformAndProcessPut(lazyKey, lazyValue), "SampleValueTransformed");

    recordTransformer.processDelete(lazyKey);
    String deletedRecord = recordTransformer.processDelete(lazyKey);
    assertNull(deletedRecord);

    recordTransformer.onEnd();
  }

}
