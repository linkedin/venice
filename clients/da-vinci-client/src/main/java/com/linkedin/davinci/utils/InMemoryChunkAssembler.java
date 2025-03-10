package com.linkedin.davinci.utils;

import com.linkedin.davinci.storage.chunking.ChunkedValueManifestContainer;
import com.linkedin.davinci.store.AbstractStorageEngine;


/*
 * This class serves as a utility to deserialize and assemble chunks consumed from a Kafka topic
 */
public class InMemoryChunkAssembler extends ChunkAssembler {
  // This storage engine serves as a buffer for records which are chunked and have to be buffered before they can
  // be returned to the client. We leverage the storageEngine interface here in order to take better advantage
  // of the chunking and decompressing adapters that we've already built (which today are built around this interface)
  // as chunked records are assembled we will eagerly evict all keys from the storage engine in order to keep the memory
  // footprint as small as we can. We could use the object cache storage engine here in order to get LRU behavior
  // but then that runs the risk of a parallel subscription having record chunks getting evicted before we have a chance
  // to assemble them. So we rely on the simpler and concrete implementation as opposed to the abstraction in order
  // to control and guarantee the behavior we're expecting.
  public InMemoryChunkAssembler(AbstractStorageEngine bufferStorageEngine) {
    super(bufferStorageEngine, true);
  }

  /**
   * We only buffer one record at a time for a given partition. If we've made it this far we either just finished
   * assembling a large record, or, didn't specify anything. So we'll clear the cache. Kafka might give duplicate
   * delivery, but it won't give out of order delivery, so this is safe to do in all such contexts.
   */
  @Override
  void evictChunks(int partitionId, byte[] keyBytes, ChunkedValueManifestContainer manifestContainer) {
    bufferStorageEngine.dropPartition(partitionId);
  }
}
