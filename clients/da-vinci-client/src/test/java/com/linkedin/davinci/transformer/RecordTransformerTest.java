package com.linkedin.davinci.transformer;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import com.linkedin.davinci.StoreBackend;
import com.linkedin.davinci.client.BlockingDaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.File;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;
import org.mockito.Mockito;
import org.rocksdb.RocksIterator;
import org.testng.annotations.Test;


public class RecordTransformerTest {
  public void deleteClassHash() {
    File file = new File("./classHash-0.txt");
    if (file.exists()) {
      assertTrue(file.delete());
    }
  }

  @Test
  public void testRecordTransformer() {
    DaVinciRecordTransformer<Integer, String, String> recordTransformer = new TestStringRecordTransformer(0, false);
    assertEquals(recordTransformer.getStoreVersion(), 0);

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

    deleteClassHash();
    int classHash = recordTransformer.getClassHash();
    assertTrue(recordTransformer.hasTransformationLogicChanged(classHash));
    assertFalse(recordTransformer.hasTransformationLogicChanged(classHash));
  }

  @Test
  public void testOnRecovery() {
    DaVinciRecordTransformer<Integer, String, String> recordTransformer = new TestStringRecordTransformer(0, true);
    RocksIterator iterator = Mockito.mock(RocksIterator.class);
    StoreBackend storeBackend = Mockito.mock(StoreBackend.class);

    // Mock the subscribe method to return a non-null CompletableFuture
    CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
    Mockito.when(storeBackend.subscribe(Mockito.any())).thenReturn(future);

    // Mock the iterator behavior
    when(iterator.isValid()).thenReturn(true).thenReturn(false);
    when(iterator.key()).thenReturn("mockKey".getBytes());
    when(iterator.value()).thenReturn("mockValue".getBytes());

    deleteClassHash();
    recordTransformer.onRecovery(iterator, storeBackend, 1);

    // Execute the onRecovery method again to test the case where the classHash file exists
    recordTransformer.onRecovery(iterator, storeBackend, 1);
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
