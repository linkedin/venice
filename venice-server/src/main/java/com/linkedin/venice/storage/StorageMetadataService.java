package com.linkedin.venice.storage;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.offsets.OffsetManager;

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
  StoreVersionState getStoreVersionState(String topicName) throws VeniceException;
}
