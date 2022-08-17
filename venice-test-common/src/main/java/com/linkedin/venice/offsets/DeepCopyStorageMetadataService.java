package com.linkedin.venice.offsets;

import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is used to systematically copy {@link OffsetRecord} instances rather than
 * passing them as is. This is necessary in StoreConsumptionTaskTest.
 */
public class DeepCopyStorageMetadataService extends DeepCopyOffsetManager implements StorageMetadataService {
  private static final Logger LOGGER = LogManager.getLogger(DeepCopyStorageMetadataService.class);

  private final InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer =
      AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer();

  private final StorageMetadataService delegateStorageMetadataService;

  public DeepCopyStorageMetadataService(StorageMetadataService delegate) {
    super(delegate);
    this.delegateStorageMetadataService = delegate;
  }

  /**
   * Persist a new {@link StoreVersionState} for the given {@param topicName}.
   *
   * @param topicName for which to retrieve the current {@link StoreVersionState}.
   * @param record    the {@link StoreVersionState} to persist
   */
  @Override
  public void put(String topicName, StoreVersionState record) throws VeniceException {
    LOGGER.info(
        "DeepCopyStorageMetadataService.put(StoreVersionState) called with topicName: " + topicName + ", record: "
            + record);
    StoreVersionState deepCopy =
        storeVersionStateSerializer.deserialize(topicName, storeVersionStateSerializer.serialize(topicName, record));
    delegateStorageMetadataService.put(topicName, deepCopy);
  }

  /**
   * This will clear all metadata, including store-version state and partition states, tied to {@param topicName}.
   *
   * @param topicName to be cleared
   */
  @Override
  public void clearStoreVersionState(String topicName) {
    LOGGER.info("DeepCopyStorageMetadataService.clearStoreVersionState called with topicName: " + topicName);
    delegateStorageMetadataService.clearStoreVersionState(topicName);

  }

  /**
   * Gets the currently-persisted {@link StoreVersionState} for this topic.
   *
   * @param topicName kafka topic to which the consumer thread is registered to.
   * @return an instance of {@link StoreVersionState} corresponding to this topic.
   */
  @Override
  public Optional<StoreVersionState> getStoreVersionState(String topicName) throws VeniceException {
    Optional<StoreVersionState> recordToReturn = delegateStorageMetadataService.getStoreVersionState(topicName);
    LOGGER.info(
        "DeepCopyStorageMetadataService.getStoreVersionState called with topicName: " + topicName + ", recordToReturn: "
            + recordToReturn);
    return recordToReturn;
  }
}
