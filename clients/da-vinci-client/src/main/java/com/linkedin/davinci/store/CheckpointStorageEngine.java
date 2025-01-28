package com.linkedin.davinci.store;

import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.offsets.OffsetRecord;


/**
 * Interface to read and write the metadata needed for checkpointing, including:
 *
 * - {@link com.linkedin.venice.offsets.OffsetRecord}
 * - {@link com.linkedin.venice.kafka.protocol.state.StoreVersionState}
 */
public interface CheckpointStorageEngine {
  void putPartitionOffset(int partitionId, OffsetRecord offsetRecord);

  OffsetRecord getPartitionOffset(int partitionId);

  void clearPartitionOffset(int partitionId);

  void putStoreVersionState(StoreVersionState versionState);

  StoreVersionState getStoreVersionState();

  void clearStoreVersionState();
}
