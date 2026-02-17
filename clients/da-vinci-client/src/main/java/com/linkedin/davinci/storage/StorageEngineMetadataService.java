package com.linkedin.davinci.storage;

import com.linkedin.davinci.store.AbstractStoragePartition;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubContext;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.service.AbstractVeniceService;
import java.util.Optional;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * StorageEngineMetadataService is wrapper service on top of storageEngineRepository to serve read/write to storage metadata.
 * It contains methods to read/write/clear the store version state and partition offset that are stored in metadata partition.
 */
public class StorageEngineMetadataService extends AbstractVeniceService implements StorageMetadataService {
  private static final Logger LOGGER = LogManager.getLogger(StorageEngineMetadataService.class);

  private final StorageEngineRepository storageEngineRepository;
  private final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer;

  public StorageEngineMetadataService(
      StorageEngineRepository storageEngineRepository,
      InternalAvroSpecificSerializer<PartitionState> serializer) {
    this.storageEngineRepository = storageEngineRepository;
    this.partitionStateSerializer = serializer;
  }

  @Override
  public void put(String topicName, int partitionId, OffsetRecord record) throws VeniceException {
    getStorageEngineOrThrow(topicName).putPartitionOffset(partitionId, record);
  }

  @Override
  public void clearOffset(String topicName, int partitionId) {
    StorageEngine<? extends AbstractStoragePartition> storageEngine =
        this.storageEngineRepository.getLocalStorageEngine(topicName);
    if (storageEngine == null) {
      LOGGER.info("Store: {} could not be located, ignoring the reset partition message.", topicName);
      return;
    }
    storageEngine.clearPartitionOffset(partitionId);
  }

  @Override
  public OffsetRecord getLastOffset(String topicName, int partitionId, PubSubContext pubSubContext)
      throws VeniceException {
    Optional<OffsetRecord> record = getStorageEngineOrThrow(topicName).getPartitionOffset(partitionId, pubSubContext);
    return record.orElseGet(() -> new OffsetRecord(partitionStateSerializer, pubSubContext));
  }

  @Override
  public boolean startInner() throws Exception {
    return true;
  }

  @Override
  public void stopInner() throws Exception {
  }

  @Override
  public StoreVersionState computeStoreVersionState(
      String topicName,
      Function<StoreVersionState, StoreVersionState> mapFunction) throws VeniceException {
    StorageEngine<? extends AbstractStoragePartition> engine = getStorageEngineOrThrow(topicName);
    synchronized (engine) {
      StoreVersionState previousSVS = engine.getStoreVersionState();
      StoreVersionState newSVS = mapFunction.apply(previousSVS);
      engine.putStoreVersionState(newSVS);
      return newSVS;
    }
  }

  @Override
  public void clearStoreVersionState(String topicName) {
    getStorageEngineOrThrow(topicName).clearStoreVersionState();
  }

  @Override
  public StoreVersionState getStoreVersionState(String topicName) throws VeniceException {
    try {
      return getStorageEngineOrThrow(topicName).getStoreVersionState();
    } catch (VeniceException e) {
      return null;
    }
  }

  @Override
  public void putGlobalRtDivState(String topicName, int partitionId, String brokerUrl, byte[] valueBytes)
      throws VeniceException {
    getStorageEngineOrThrow(topicName).putGlobalRtDivState(partitionId, brokerUrl, valueBytes);
  }

  @Override
  public Optional<byte[]> getGlobalRtDivState(String topicName, int partitionId, String brokerUrl)
      throws VeniceException {
    return getStorageEngineOrThrow(topicName).getGlobalRtDivState(partitionId, brokerUrl);
  }

  @Override
  public void clearGlobalRtDivState(String topicName, int partitionId, String brokerUrl) {
    getStorageEngineOrThrow(topicName).clearGlobalRtDivState(partitionId, brokerUrl);
  }

  private StorageEngine<? extends AbstractStoragePartition> getStorageEngineOrThrow(String topicName) {
    StorageEngine<? extends AbstractStoragePartition> storageEngine =
        this.storageEngineRepository.getLocalStorageEngine(topicName);
    if (storageEngine == null) {
      throw new VeniceException("Topic " + topicName + " not found in storageEngineRepository");
    }
    return storageEngine;
  }
}
