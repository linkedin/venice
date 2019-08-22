package com.linkedin.venice.storage.chunking;

/**
 * Used to incrementally add a {@param valueChunk} into the {@param ASSEMBLED_VALUE_CONTAINER}
 * container.
 */
public interface AddChunkIntoContainer<ASSEMBLED_VALUE_CONTAINER> {
  void add(ASSEMBLED_VALUE_CONTAINER container, int chunkIndex, byte[] valueChunk);
}
