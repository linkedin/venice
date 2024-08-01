package com.linkedin.davinci.transformer;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import com.linkedin.davinci.StoreBackend;
import com.linkedin.davinci.client.BlockingDaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;
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

    StoreBackend storeBackend = mock(StoreBackend.class);
    CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
    when(storeBackend.subscribe(any())).thenReturn(future);

    RocksIterator iterator = mock(RocksIterator.class);
    when(iterator.isValid()).thenReturn(true).thenReturn(false);
    when(iterator.key()).thenReturn("mockKey".getBytes());
    when(iterator.value()).thenReturn("mockValue".getBytes());

    AbstractStorageEngine storageEngine = mock(AbstractStorageEngine.class);

    deleteClassHash();
    Optional<Version> version = Optional.of(mock(Version.class));
    List<Integer> partitions = new ArrayList<>();
    int partitionId = 1;
    partitions.add(partitionId);
    recordTransformer.onRecovery(storageEngine, storeBackend, partitions, version);
    verify(storageEngine, times(1)).clearPartitionOffset(partitionId);

    // Reset the mock to clear previous interactions
    reset(storageEngine);

    // Execute the onRecovery method again to test the case where the classHash file exists
    when(storageEngine.getRocksDBIterator(partitionId)).thenReturn(iterator);
    recordTransformer.onRecovery(storageEngine, storeBackend, partitions, version);
    verify(storageEngine, never()).clearPartitionOffset(partitionId);
    verify(storageEngine, times(1)).getRocksDBIterator(partitionId);
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
