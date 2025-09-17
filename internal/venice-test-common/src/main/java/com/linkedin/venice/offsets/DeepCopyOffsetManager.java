package com.linkedin.venice.offsets;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.pubsub.PubSubContext;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is used to systematically copy {@link OffsetRecord} instances rather than
 * passing them as is. This is necessary in TestAdminConsumptionTask and StoreConsumptionTaskTest.
 */
public class DeepCopyOffsetManager implements OffsetManager {
  private static final Logger LOGGER = LogManager.getLogger(DeepCopyOffsetManager.class);

  private static final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
      AvroProtocolDefinition.PARTITION_STATE.getSerializer();

  private final OffsetManager delegate;

  public DeepCopyOffsetManager(OffsetManager delegate) {
    this.delegate = delegate;
  }

  @Override
  public void put(String topicName, int partitionId, OffsetRecord record) throws VeniceException {
    LOGGER.info(
        "OffsetManager.put(OffsetRecord) called with topicName: {}, partitionId: {}, record: {}",
        topicName,
        partitionId,
        record);

    // Doing a deep copy, otherwise Mockito keeps a handle on the reference only, which can mutate and lead to confusing
    // verify() semantics
    OffsetRecord deepCopy = new OffsetRecord(record.toBytes(), partitionStateSerializer, record.getPubSubContext());
    delegate.put(topicName, partitionId, deepCopy);
  }

  @Override
  public void clearOffset(String topicName, int partitionId) {
    LOGGER.info("OffsetManager.clearOffset called with topicName: {}, partitionId: {}", topicName, partitionId);
    delegate.clearOffset(topicName, partitionId);
  }

  @Override
  public OffsetRecord getLastOffset(String topicName, int partitionId, PubSubContext pubSubContext)
      throws VeniceException {
    OffsetRecord recordToReturn = delegate.getLastOffset(topicName, partitionId, pubSubContext);
    LOGGER.info(
        "OffsetManager.getLastOffset called with topic-partition: {}, recordToReturn: {}",
        Utils.getReplicaId(topicName, partitionId),
        recordToReturn);
    return recordToReturn;
  }
}
