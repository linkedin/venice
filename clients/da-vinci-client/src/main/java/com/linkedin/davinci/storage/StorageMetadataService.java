package com.linkedin.davinci.storage;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.offsets.OffsetManager;
import java.nio.ByteBuffer;
import java.util.Optional;


/**
 * This is a superset of the the OffsetManager APIs, which also provide functions
 * for storing store-version level state.
 */
public interface StorageMetadataService extends OffsetManager {
  /**
   * Persist a new {@link StoreVersionState} for the given {@param topicName}.
   *
   * @param topicName for which to retrieve the current {@link StoreVersionState}.
   * @param record the {@link StoreVersionState} to persist
   */
  void put(String topicName, StoreVersionState record) throws VeniceException;

  /**
   * This will clear all metadata, including store-version state and partition states, tied to {@param topicName}.
   *
   * @param topicName to be cleared
   */
  void clearStoreVersionState(String topicName);

  /**
   * Gets the currently-persisted {@link StoreVersionState} for this topic.
   *
   * @param topicName  kafka topic to which the consumer thread is registered to.
   * @return an instance of {@link StoreVersionState} corresponding to this topic.
   */
  Optional<StoreVersionState> getStoreVersionState(String topicName) throws VeniceException;

  /**
   * Tailored function for retrieving chunking setting. Specific implementations can optionally
   * implement a more optimized version of this API, since it is expected to be used at greater
   * frequency than the others.
   */
  default boolean isStoreVersionChunked(String topicName) {
    return getStoreVersionState(topicName).map(storeVersionState -> storeVersionState.chunked).orElse(false);
  }

  /**
   * Tailored function for retrieving version's compression strategy setting.
   */
  default CompressionStrategy getStoreVersionCompressionStrategy(String topicName) {
    return getStoreVersionState(topicName)
        .map(storeVersionState -> CompressionStrategy.valueOf(storeVersionState.compressionStrategy))
        .orElse(CompressionStrategy.NO_OP);
  }

  /**
   * Tailored function for retrieving version's compression dictionary.
   */
  default ByteBuffer getStoreVersionCompressionDictionary(String topicName) {
    return getStoreVersionState(topicName).map(storeVersionState -> storeVersionState.compressionDictionary)
        .orElse(null);
  }
}
