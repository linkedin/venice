package com.linkedin.venice.offsets;

import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import java.util.Optional;
import java.util.function.Function;
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

  @Override
  public StoreVersionState computeStoreVersionState(
      String topicName,
      Function<StoreVersionState, StoreVersionState> mapFunction) throws VeniceException {
    return delegateStorageMetadataService.computeStoreVersionState(topicName, previousStoreVersionState -> {
      StoreVersionState newSVS = mapFunction.apply(
          previousStoreVersionState == null
              ? null
              : storeVersionStateSerializer
                  .deserialize(topicName, storeVersionStateSerializer.serialize(topicName, previousStoreVersionState)));
      LOGGER.info(
          "DeepCopyStorageMetadataService.compute() called for topicName: {}, previousSVS: {}, newSVS: {}",
          topicName,
          previousStoreVersionState,
          newSVS);
      return newSVS;
    });
  }

  /**
   * This will clear all metadata, including store-version state and partition states, tied to {@param topicName}.
   *
   * @param topicName to be cleared
   */
  @Override
  public void clearStoreVersionState(String topicName) {
    LOGGER.info("DeepCopyStorageMetadataService.clearStoreVersionState called with topicName: {}", topicName);
    delegateStorageMetadataService.clearStoreVersionState(topicName);

  }

  /**
   * Gets the currently-persisted {@link StoreVersionState} for this topic.
   *
   * @param topicName kafka topic to which the consumer thread is registered to.
   * @return an instance of {@link StoreVersionState} corresponding to this topic.
   */
  @Override
  public StoreVersionState getStoreVersionState(String topicName) throws VeniceException {
    StoreVersionState recordToReturn = delegateStorageMetadataService.getStoreVersionState(topicName);
    LOGGER.info(
        "DeepCopyStorageMetadataService.getStoreVersionState called with topicName: {}, recordToReturn: {}",
        topicName,
        recordToReturn);
    return recordToReturn;
  }

  @Override
  public void putGlobalRtDivState(String topicName, int partitionId, String brokerUrl, byte[] valueBytes)
      throws VeniceException {
    delegateStorageMetadataService.putGlobalRtDivState(topicName, partitionId, brokerUrl, valueBytes.clone());
  }

  @Override
  public Optional<byte[]> getGlobalRtDivState(String topicName, int partitionId, String brokerUrl)
      throws VeniceException {
    return delegateStorageMetadataService.getGlobalRtDivState(topicName, partitionId, brokerUrl).map(byte[]::clone);
  }

  @Override
  public void clearGlobalRtDivState(String topicName, int partitionId, String brokerUrl) {
    delegateStorageMetadataService.clearGlobalRtDivState(topicName, partitionId, brokerUrl);
  }
}
