package com.linkedin.davinci.storage;

import com.linkedin.davinci.store.AbstractStoragePartition;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import java.util.Optional;
import org.apache.log4j.Logger;


/**
 * StorageEngineMetadataService is wrapper service on top of storageEngineRepository to serve read/write to storage metadata.
 * It contains methods to read/write/clear the store version state and partition offset that are stored in metadata partition.
 */
public class StorageEngineMetadataService extends AbstractVeniceService implements StorageMetadataService  {
  private static final Logger logger = Logger.getLogger(StorageEngineMetadataService.class);

  private final StorageEngineRepository storageEngineRepository;
  private final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer;

  public StorageEngineMetadataService(StorageEngineRepository storageEngineRepository) {
    this(storageEngineRepository, AvroProtocolDefinition.PARTITION_STATE.getSerializer());
  }

  public StorageEngineMetadataService(StorageEngineRepository storageEngineRepository, InternalAvroSpecificSerializer<PartitionState> serializer) {
    this.storageEngineRepository = storageEngineRepository;
    this.partitionStateSerializer = serializer;
  }

  @Override
  public void put(String topicName, int partitionId, OffsetRecord record) throws VeniceException {
    getStorageEngine(topicName).putPartitionOffset(partitionId, record);
  }

  @Override
  public void clearOffset(String topicName, int partitionId) {
    AbstractStorageEngine<?> storageEngine = this.storageEngineRepository.getLocalStorageEngine(topicName);
    if (storageEngine == null) {
      logger.info("Store " + topicName +  " could not be located, ignoring the reset partition message.");
      return;
    }
    storageEngine.clearPartitionOffset(partitionId);
  }

  @Override
  public OffsetRecord getLastOffset(String topicName, int partitionId) throws VeniceException {
    Optional<OffsetRecord> record = getStorageEngine(topicName).getPartitionOffset(partitionId);
    return record.orElseGet(() -> new OffsetRecord(partitionStateSerializer));
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
