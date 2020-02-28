package com.linkedin.venice.storage;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.server.StorageEngineRepository;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.AbstractStoragePartition;
import java.util.Optional;


/**
 * StorageEngineMetadataService is wrapper service on top of storageEngineRepository to serve read/write to storage metadata.
 * It contains methods to read/write/clear the store version state and partition offset that are stored in metadata partition.
 */
public class StorageEngineMetadataService extends AbstractVeniceService implements StorageMetadataService  {
  private final StorageEngineRepository storageEngineRepository;

  public StorageEngineMetadataService(StorageEngineRepository storageEngineRepository) {
    this.storageEngineRepository = storageEngineRepository;
  }

  @Override
  public void put(String topicName, int partitionId, OffsetRecord record) throws VeniceException {
    getStorageEngine(topicName).putPartitionOffset(partitionId, record);
  }

  @Override
  public void clearOffset(String topicName, int partitionId) {
    getStorageEngine(topicName).clearPartitionOffset(partitionId);
  }

  @Override
  public OffsetRecord getLastOffset(String topicName, int partitionId) throws VeniceException {
    Optional<OffsetRecord> record = getStorageEngine(topicName).getPartitionOffset(partitionId);
    return record.orElseGet(OffsetRecord::new);
  }

  @Override
  public boolean startInner() throws Exception {
    return true;
  }

  @Override
  public void stopInner() throws Exception {
  }

  @Override
  public void put(String topicName, StoreVersionState record) throws VeniceException {
    getStorageEngine(topicName).putStoreVersionState(record);
  }

  @Override
  public void clearStoreVersionState(String topicName) {
    getStorageEngine(topicName).clearStoreVersionState();
  }

  @Override
  public Optional<StoreVersionState> getStoreVersionState(String topicName) throws VeniceException {
    return getStorageEngine(topicName).getStoreVersionState();
  }

  private AbstractStorageEngine<? extends AbstractStoragePartition> getStorageEngine(String topicName) {
    AbstractStorageEngine<?> storageEngine = this.storageEngineRepository.getLocalStorageEngine(topicName);
    if (storageEngine == null) {
      throw new VeniceClientException("Topic " + topicName + " not found in storageEngineRepository");
    }
    return storageEngine;
  }
}
