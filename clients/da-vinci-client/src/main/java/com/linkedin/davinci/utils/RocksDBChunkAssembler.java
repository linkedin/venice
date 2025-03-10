package com.linkedin.davinci.utils;

import com.linkedin.davinci.storage.chunking.ChunkedValueManifestContainer;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.rocksdb.RocksDBStorageEngine;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import java.nio.ByteBuffer;


/**
 * Similar to {@link InMemoryChunkAssembler} except is uses a {@link RocksDBStorageEngine} as the buffer to buffer
 * chunks before the full record can be assembled. The evict policy is also different. Only keys belonging to the
 * assembled record's manifest are removed. This could result in larger buffer size, but it's necessary for consuming
 * topics that could have multiple ongoing chunks such as the materialized view topics. TTL or eviction by source
 * partition (VT partition) could be adopted as an improvement to clean up any leaked chunks due to producer failures.
 */
public class RocksDBChunkAssembler extends ChunkAssembler {
  public RocksDBChunkAssembler(AbstractStorageEngine bufferStorageEngine, boolean skipFailedToAssembleRecords) {
    super(bufferStorageEngine, skipFailedToAssembleRecords);
  }

  /**
   * Remove the manifest and its chunks from the {@link RocksDBStorageEngine} backed buffer.
   */
  @Override
  void evictChunks(int partitionId, byte[] keyBytes, ChunkedValueManifestContainer manifestContainer) {
    ChunkedValueManifest manifest = manifestContainer.getManifest();
    if (manifest != null) {
      for (ByteBuffer chunkKeyByte: manifest.getKeysWithChunkIdSuffix()) {
        bufferStorageEngine.delete(partitionId, ByteUtils.extractByteArray(chunkKeyByte));
      }
    }
    bufferStorageEngine.delete(partitionId, keyBytes);
  }
}
