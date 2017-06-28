package com.linkedin.venice.serialization.avro;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import org.apache.avro.specific.SpecificRecord;

/**
 * This enum lays out the basic specs of the various stateful protocols used in Venice.
 *
 * Having these definitions in a single place makes it easy to ensure that magic bytes
 * are defined only once and do not conflict with each other.
 */
public enum AvroProtocolDefinition {
  /**
   * Used for the Kafka topics, including the main data topics as well as the admin topic.
   */
  KAFKA_MESSAGE_ENVELOPE(23, 3, KafkaMessageEnvelope.class),

  /**
   * Used to persist the state of a partition in Storage Nodes, including offset,
   * Data Ingest Validation state, etc.
   */
  PARTITION_STATE(24, 3, PartitionState.class),

  /**
   * Used to persist state related to a store-version, including Start of Buffer Replay
   * offsets and whether the input is sorted.
   */
  STORE_VERSION_STATE(25, 1, StoreVersionState.class);

  /**
   * The first byte at the beginning of a serialized byte array.
   *
   * IMPORTANT: The magic byte should be different for each protocol definition.
   */
  final byte magicByte;

  /**
   * The protocol version that is currently in use. Typically, this should be the
   * greatest number available, but that may not necessarily be true if some new
   * experimental version of a protocol is being designed and is not yet rolled out.
   */
  final byte currentProtocolVersion;

  /**
   * The {@link SpecificRecord} class that this protocol should provide as an API
   * in the code.
   */
  final Class<? extends SpecificRecord> specificRecordClass;

  AvroProtocolDefinition(int magicByte, int currentProtocolVersion, Class<? extends SpecificRecord> specificRecordClass) {
    this.magicByte = (byte) magicByte;
    this.currentProtocolVersion = (byte) currentProtocolVersion;
    this.specificRecordClass = specificRecordClass;
  }

  public <T extends SpecificRecord> InternalAvroSpecificSerializer<T> getSerializer() {
    return new InternalAvroSpecificSerializer<>(this);
  }
}
