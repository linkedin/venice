package com.linkedin.venice.offsets;

import com.linkedin.venice.exceptions.VeniceException;
import org.apache.log4j.Logger;

/**
 * This class is used to systematically copy {@link OffsetRecord} instances rather than
 * passing them as is. This is necessary in TestAdminConsumptionTask and StoreConsumptionTaskTest.
 */
public class DeepCopyOffsetManager implements OffsetManager {
  private static final Logger LOGGER = Logger.getLogger(DeepCopyOffsetManager.class);

  private final OffsetManager delegate;

  public DeepCopyOffsetManager(OffsetManager delegate) {
    this.delegate = delegate;
  }

  @Override
  public void put(String topicName, int partitionId, OffsetRecord record) throws VeniceException {
    LOGGER.info("OffsetManager.put(OffsetRecord) called with topicName: " + topicName +
        ", partitionId: " + partitionId + ", record: " + record);

    // Doing a deep copy, otherwise Mockito keeps a handle on the reference only, which can mutate and lead to confusing verify() semantics
    OffsetRecord deepCopy = new OffsetRecord(record.toBytes());
    delegate.put(topicName, partitionId, deepCopy);
  }

  @Override
  public void clearOffset(String topicName, int partitionId) {
    LOGGER.info("OffsetManager.clearOffset called with topicName: " + topicName +
        ", partitionId: " + partitionId);
    delegate.clearOffset(topicName, partitionId);
  }

  @Override
  public OffsetRecord getLastOffset(String topicName, int partitionId) throws VeniceException {
    OffsetRecord recordToReturn = delegate.getLastOffset(topicName, partitionId);
    LOGGER.info("OffsetManager.getLastOffset called with topicName: " + topicName +
        ", partitionId: " + partitionId + ", recordToReturn: " + recordToReturn);
    return recordToReturn;
  }
}