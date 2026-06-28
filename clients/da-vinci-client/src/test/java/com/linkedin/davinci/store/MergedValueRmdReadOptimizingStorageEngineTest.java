package com.linkedin.davinci.store;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.nio.ByteBuffer;
import org.testng.annotations.Test;


public class MergedValueRmdReadOptimizingStorageEngineTest {
  private static final int PARTITION = 3;

  @Test
  public void testValueAndRmdServedFromSingleMergedRead() {
    StorageEngine delegate = mock(StorageEngine.class);
    byte[] value = "value-bytes".getBytes();
    byte[] rmd = "rmd-bytes".getBytes();
    when(delegate.getValueAndReplicationMetadata(eq(PARTITION), any(ByteBuffer.class)))
        .thenReturn(new byte[][] { value, rmd });

    MergedValueRmdReadOptimizingStorageEngine engine =
        new MergedValueRmdReadOptimizingStorageEngine(delegate, PARTITION);

    byte[] key = "the-key".getBytes();
    // RMD top-level read happens first (eager during conflict resolution).
    assertEquals(engine.getReplicationMetadata(PARTITION, ByteBuffer.wrap(key)), rmd);
    // Value top-level read happens next, for the same key, and must be served from the cached merged read.
    assertEquals(engine.get(PARTITION, ByteBuffer.wrap(key)), value);

    // Exactly one merged lookup for both, and neither single-column read hit the delegate for the primary key.
    verify(delegate, times(1)).getValueAndReplicationMetadata(eq(PARTITION), any(ByteBuffer.class));
    verify(delegate, never()).get(eq(PARTITION), any(ByteBuffer.class));
    verify(delegate, never()).getReplicationMetadata(eq(PARTITION), any(ByteBuffer.class));
  }

  @Test
  public void testChunkReadsAndOtherPartitionsAreDelegated() {
    StorageEngine delegate = mock(StorageEngine.class);
    byte[] primaryKey = "manifest-key".getBytes();
    byte[] chunkKey = "chunk-key".getBytes();
    byte[] rmdChunkKey = "rmd-chunk-key".getBytes();
    byte[] chunkBytes = "chunk".getBytes();
    byte[] rmdChunkBytes = "rmd-chunk".getBytes();
    when(delegate.getValueAndReplicationMetadata(eq(PARTITION), any(ByteBuffer.class)))
        .thenReturn(new byte[][] { "manifest".getBytes(), "rmd-manifest".getBytes() });
    when(delegate.get(eq(PARTITION), eq(ByteBuffer.wrap(chunkKey)))).thenReturn(chunkBytes);
    when(delegate.getReplicationMetadata(eq(PARTITION), eq(ByteBuffer.wrap(rmdChunkKey)))).thenReturn(rmdChunkBytes);
    when(delegate.get(eq(7), eq(ByteBuffer.wrap(primaryKey)))).thenReturn("other-partition".getBytes());

    MergedValueRmdReadOptimizingStorageEngine engine =
        new MergedValueRmdReadOptimizingStorageEngine(delegate, PARTITION);

    // Prime the merged read with the primary key.
    engine.getReplicationMetadata(PARTITION, ByteBuffer.wrap(primaryKey));
    // Chunk reads (different keys) fall through to the delegate unchanged.
    assertEquals(engine.get(PARTITION, ByteBuffer.wrap(chunkKey)), chunkBytes);
    assertEquals(engine.getReplicationMetadata(PARTITION, ByteBuffer.wrap(rmdChunkKey)), rmdChunkBytes);
    // A read for a different partition is delegated and never served from the cached merged read.
    assertEquals(engine.get(7, ByteBuffer.wrap(primaryKey)), "other-partition".getBytes());

    verify(delegate, times(1)).get(eq(PARTITION), eq(ByteBuffer.wrap(chunkKey)));
    verify(delegate, times(1)).getReplicationMetadata(eq(PARTITION), eq(ByteBuffer.wrap(rmdChunkKey)));
    verify(delegate, times(1)).get(eq(7), eq(ByteBuffer.wrap(primaryKey)));
  }

  @Test
  public void testMissingRecordYieldsNullValueAndRmd() {
    StorageEngine delegate = mock(StorageEngine.class);
    when(delegate.getValueAndReplicationMetadata(eq(PARTITION), any(ByteBuffer.class))).thenReturn(null);

    MergedValueRmdReadOptimizingStorageEngine engine =
        new MergedValueRmdReadOptimizingStorageEngine(delegate, PARTITION);

    byte[] key = "missing".getBytes();
    assertNull(engine.getReplicationMetadata(PARTITION, ByteBuffer.wrap(key)));
    assertNull(engine.get(PARTITION, ByteBuffer.wrap(key)));
    verify(delegate, times(1)).getValueAndReplicationMetadata(eq(PARTITION), any(ByteBuffer.class));
  }
}
