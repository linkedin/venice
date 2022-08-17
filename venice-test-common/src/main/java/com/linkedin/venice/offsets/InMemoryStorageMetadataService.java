package com.linkedin.venice.offsets;

import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * In memory implementation of StorageMetadataService, should really only be used for tests
 */
public class InMemoryStorageMetadataService extends InMemoryOffsetManager implements StorageMetadataService {
  private static final Logger LOGGER = LogManager.getLogger(InMemoryStorageMetadataService.class);

  private final ConcurrentMap<String, StoreVersionState> topicToStoreVersionStateMap = new ConcurrentHashMap<>();

  @Override
  public void put(String topicName, StoreVersionState record) throws VeniceException {
    LOGGER.info(
        "InMemoryStorageMetadataService.put(StoreVersionState) called with topicName: " + topicName + ", record: "
            + record);
    topicToStoreVersionStateMap.put(topicName, record);
  }

  @Override
  public void clearStoreVersionState(String topicName) {
    LOGGER.info("InMemoryStorageMetadataService.clearStoreVersionState called with topicName: " + topicName);
    topicToStoreVersionStateMap.remove(topicName);
  }

  @Override
  public Optional<StoreVersionState> getStoreVersionState(String topicName) throws VeniceException {
    Optional<StoreVersionState> recordToReturn = Optional.ofNullable(topicToStoreVersionStateMap.get(topicName));
    LOGGER.info(
        "InMemoryStorageMetadataService.getStoreVersionState called with topicName: " + topicName + ", recordToReturn: "
            + recordToReturn);
    return recordToReturn;
  }
}
