package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.utils.TestUtils.DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;

import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;
import org.apache.avro.Schema;


/**
 * Shared test helpers for UniqueKeyCountTest and UniqueKeyCountScenarioTest.
 * Centralizes PCS construction, checkpoint/restore, batch simulation, and reflection utilities.
 */
class UniqueKeyCountTestUtils {
  static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();
  static final PubSubTopicPartition DEFAULT_TP = new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic("store_v1"), 0);

  /** Creates a fresh PCS with default topic partition, no checkpoint state. */
  static PartitionConsumptionState freshPcs() {
    return freshPcs(DEFAULT_TP);
  }

  /** Creates a fresh PCS with a custom topic partition. */
  static PartitionConsumptionState freshPcs(PubSubTopicPartition tp) {
    return new PartitionConsumptionState(
        tp,
        new OffsetRecord(
            AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
            DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING,
        true,
        Schema.create(Schema.Type.STRING));
  }

  /** Creates a fresh PCS with the given leader/follower state for the default topic partition. */
  static PartitionConsumptionState freshPcs(LeaderFollowerStateType lfState) {
    PartitionConsumptionState pcs = freshPcs();
    pcs.setLeaderFollowerState(lfState);
    return pcs;
  }

  /** Creates a fresh PCS with the given leader/follower state for a specific partition number. */
  static PartitionConsumptionState freshPcs(LeaderFollowerStateType lfState, int partition) {
    PubSubTopicPartition tp = new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic("store_v2"), partition);
    PartitionConsumptionState pcs = freshPcs(tp);
    pcs.setLeaderFollowerState(lfState);
    return pcs;
  }

  /** Creates a PCS restored from a checkpoint with the given unique key count. */
  static PartitionConsumptionState pcsFromCheckpoint(long uniqueKeyCount) {
    OffsetRecord or = new OffsetRecord(
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    or.setUniqueKeyCount(uniqueKeyCount);
    return new PartitionConsumptionState(
        DEFAULT_TP,
        or,
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING,
        true,
        Schema.create(Schema.Type.STRING));
  }

  /** Restores a PCS from a deserialized OffsetRecord checkpoint. */
  static PartitionConsumptionState restoreFrom(OffsetRecord checkpoint) {
    return new PartitionConsumptionState(
        DEFAULT_TP,
        checkpoint,
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING,
        true,
        Schema.create(Schema.Type.STRING));
  }

  /** Simulates syncOffset: copies PCS count into OffsetRecord, serializes, and deserializes. */
  static OffsetRecord checkpoint(PartitionConsumptionState pcs) {
    OffsetRecord or = pcs.getOffsetRecord();
    or.setUniqueKeyCount(pcs.getUniqueKeyCount());
    byte[] bytes = or.toBytes();
    return new OffsetRecord(
        bytes,
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
  }

  /** Simulates a non-chunked batch push of {@code count} records followed by finalization. */
  static void doBatch(PartitionConsumptionState pcs, int count) {
    for (int i = 0; i < count; i++) {
      pcs.incrementUniqueKeyCountForBatchRecord();
    }
    pcs.finalizeUniqueKeyCountForBatchPush();
  }

  /**
   * Simulates batch ingestion for a chunked store. For each logical key, there are
   * {@code chunksPerKey} chunk fragment records (schemaId=CHUNK, NOT counted) followed
   * by 1 manifest record (schemaId=CHUNKED_VALUE_MANIFEST, counted).
   * Only the manifest calls incrementUniqueKeyCountForBatchRecord -- this mirrors the filtering in
   * processKafkaDataMessage where schemaId != CHUNK_SCHEMA_ID gates the counter.
   */
  static void doBatchChunked(PartitionConsumptionState pcs, int logicalKeyCount, int chunksPerKey) {
    for (int key = 0; key < logicalKeyCount; key++) {
      // Chunk fragments: schemaId == CHUNK -> filtered out by processKafkaDataMessage, NOT counted
      // (we simply don't call incrementUniqueKeyCountForBatchRecord for these)

      // Manifest: schemaId == CHUNKED_VALUE_MANIFEST -> passes filter, counted
      pcs.incrementUniqueKeyCountForBatchRecord();
    }
    pcs.finalizeUniqueKeyCountForBatchPush();
  }

  /** Walks the class hierarchy to find a declared field by name. */
  static Field findField(Class<?> clazz, String fieldName) throws NoSuchFieldException {
    Class<?> current = clazz;
    while (current != null) {
      try {
        return current.getDeclaredField(fieldName);
      } catch (NoSuchFieldException e) {
        current = current.getSuperclass();
      }
    }
    throw new NoSuchFieldException(fieldName + " not found in hierarchy of " + clazz.getName());
  }

  /** Sets a field value on the target object, walking the class hierarchy to find the field. */
  static void setField(Object target, String fieldName, Object value) throws Exception {
    Field field = findField(target.getClass(), fieldName);
    AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
      field.setAccessible(true);
      return null;
    });
    field.set(target, value);
  }

  /** Walks the class hierarchy to find a declared method by name and parameter types. */
  static Method findMethod(Class<?> clazz, String methodName, Class<?>... paramTypes) throws NoSuchMethodException {
    Class<?> current = clazz;
    while (current != null) {
      try {
        return current.getDeclaredMethod(methodName, paramTypes);
      } catch (NoSuchMethodException e) {
        current = current.getSuperclass();
      }
    }
    throw new NoSuchMethodException(methodName + " not found in hierarchy of " + clazz.getName());
  }

  private UniqueKeyCountTestUtils() {
  }
}
