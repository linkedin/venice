package com.linkedin.davinci.storage;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.offsets.OffsetManager;
import java.nio.ByteBuffer;
import java.util.function.Function;


/**
 * This is a superset of the OffsetManager APIs, which also provide functions for storing store-version level state.
 */
public interface StorageMetadataService extends OffsetManager {
  void computeStoreVersionState(String topicName, Function<StoreVersionState, StoreVersionState> mapFunction)
      throws VeniceException;

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
   * @return an instance of {@link StoreVersionState} corresponding to this topic, or null if there isn't any.
   */
  StoreVersionState getStoreVersionState(String topicName) throws VeniceException;

  /**
   * Tailored function for retrieving version's compression dictionary.
   */
  default ByteBuffer getStoreVersionCompressionDictionary(String topicName) {
    StoreVersionState svs = getStoreVersionState(topicName);
    return svs == null ? null : svs.compressionDictionary;
  }
}
