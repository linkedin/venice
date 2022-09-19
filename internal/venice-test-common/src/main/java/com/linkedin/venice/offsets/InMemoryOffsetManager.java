package com.linkedin.venice.offsets;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * In memory implementation of OffsetManager, should really only be used for tests
 */
public class InMemoryOffsetManager implements OffsetManager {
  private static final InternalAvroSpecificSerializer<PartitionState> serializer =
      AvroProtocolDefinition.PARTITION_STATE.getSerializer();
  private ConcurrentMap<String, ConcurrentMap<Integer, OffsetRecord>> topicToPartitionToOffsetMap =
      new ConcurrentHashMap<>();

  @Override
  public void put(String topicName, int partitionId, OffsetRecord record) throws VeniceException {
    topicToPartitionToOffsetMap.compute(topicName, (topic, map) -> {
      if (map == null) {
        ConcurrentMap<Integer, OffsetRecord> newMap = new ConcurrentHashMap<>();
        newMap.put(partitionId, record);
        return newMap;
      } else {
        map.compute(partitionId, (partition, oldRecord) -> {
          if (oldRecord == null || oldRecord.getLocalVersionTopicOffset() < record.getLocalVersionTopicOffset()) {
            return record;
          } else {
            return oldRecord;
          }
        });
        return map;
      }
    });
  }

  @Override
  public void clearOffset(String topicName, int partitionId) {
    topicToPartitionToOffsetMap.computeIfPresent(topicName, (topic, map) -> {
      map.computeIfPresent(partitionId, (partition, record) -> null);
      return map;
    });
  }

  @Override
  public OffsetRecord getLastOffset(String topicName, int partitionId) throws VeniceException {
    OffsetRecord returnOffset = null;
    ConcurrentMap<Integer, OffsetRecord> map = topicToPartitionToOffsetMap.get(topicName);
    if (map != null) {
      returnOffset = map.get(partitionId);
    }
    if (returnOffset != null) {
      return returnOffset;
    } else {
      return new OffsetRecord(serializer);
    }
  }
}
