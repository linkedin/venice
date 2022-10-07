package com.linkedin.venice.offsets;

import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * In memory implementation of StorageMetadataService, should really only be used for tests
 */
public class InMemoryStorageMetadataService extends InMemoryOffsetManager implements StorageMetadataService {
  private static final Logger LOGGER = LogManager.getLogger(InMemoryStorageMetadataService.class);

  private final ConcurrentMap<String, StoreVersionState> topicToStoreVersionStateMap = new ConcurrentHashMap<>();

  @Override
  public void computeStoreVersionState(String topicName, Function<StoreVersionState, StoreVersionState> mapFunction)
      throws VeniceException {
    LOGGER.info("InMemoryStorageMetadataService.compute(StoreVersionState) called for topicName: {}", topicName);
    topicToStoreVersionStateMap.compute(topicName, (s, storeVersionState) -> mapFunction.apply(storeVersionState));
  }

  @Override
  public void clearStoreVersionState(String topicName) {
    LOGGER.info("InMemoryStorageMetadataService.clearStoreVersionState called with topicName: {}", topicName);
    topicToStoreVersionStateMap.remove(topicName);
  }

  @Override
  public StoreVersionState getStoreVersionState(String topicName) throws VeniceException {
    StoreVersionState recordToReturn = topicToStoreVersionStateMap.get(topicName);
    LOGGER.info(
        "InMemoryStorageMetadataService.getStoreVersionState called with topicName: {}, recordToReturn: {}",
        topicName,
        recordToReturn);
    return recordToReturn;
  }
}
