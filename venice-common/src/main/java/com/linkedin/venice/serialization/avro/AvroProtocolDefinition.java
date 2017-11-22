package com.linkedin.venice.serialization.avro;

import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.kafka.protocol.*;
import com.linkedin.venice.kafka.protocol.state.*;
import com.linkedin.venice.storage.protocol.*;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
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
  KAFKA_MESSAGE_ENVELOPE(23, 4, KafkaMessageEnvelope.class),

  /**
   * Used to persist the state of a partition in Storage Nodes, including offset,
   * Data Ingest Validation state, etc.
   */
  PARTITION_STATE(24, 3, PartitionState.class),

  /**
   * Used to persist state related to a store-version, including Start of Buffer Replay
   * offsets and whether the input is sorted.
   */
  STORE_VERSION_STATE(25, 2, StoreVersionState.class),

  /**
   * Used to encode metadata changes about the system as a whole. Records of this type
   * are propagated via the "admin channel" special Kafka topic. It is stored in the
   * {@link Put} of a {@link KafkaMessageEnvelope}, and thus leverages the envelope
   * for versioning.
   *
   * TODO: Move AdminOperation to venice-common module so that we can properly reference it here.
   */
  ADMIN_OPERATION(11, SpecificData.get().getSchema(ByteBuffer.class), "AdminOperation"), // , AdminOperation.class),

  /**
   * Single chunk of a large multi-chunk value. Just a bunch of bytes.
   *
   * Uses a negative protocol version in order to avoid clashing with user-defined schemas.
   */
  CHUNK(-10, SpecificData.get().getSchema(ByteBuffer.class), "Chunk"),

  /**
   * Used to encode the manifest of a multi-chunk large value. It is stored in the
   * {@link Put} of a {@link KafkaMessageEnvelope}, and thus leverages the envelope
   * for versioning.
   *
   * Uses a negative protocol version in order to avoid clashing with user-defined schemas.
   */
  CHUNKED_VALUE_MANIFEST(-20, ChunkedValueManifest.class),

  /**
   * Suffix appended to the end of all keys in a store-version where chunking is enabled.
   *
   * This protocol is actually un-evolvable.
   */
  CHUNKED_KEY_SUFFIX(ChunkedKeySuffix.class);

  /**
   * The first byte at the beginning of a serialized byte array.
   *
   * IMPORTANT: The magic byte should be different for each protocol definition.
   *
   * If empty, then this protocol is already stored within an evolvable structure, and
   * thus does not need a magic byte.
   */
  final Optional<Byte> magicByte;

  /**
   * The protocol version that is currently in use. Typically, this should be the
   * greatest number available, but that may not necessarily be true if some new
   * experimental version of a protocol is being designed and is not yet rolled out.
   */
  final Optional<Integer> currentProtocolVersion;

  /**
   * The {@link SpecificRecord} class that this protocol should provide as an API
   * in the code.
   */
  final String className;

  final Schema schema;

  /**
   * Indicates whether the protocol version should prepend the payload, or whether
   * it will be stored outside of the payload, and provided at deserialization-time.
   */
  final boolean protocolVersionStoredInHeader;

  /**
   * Constructor for protocols where the Avro record is prepended by a protocol
   * definition header, which includes a magic byte and protocol version.
   */
  AvroProtocolDefinition(int magicByte, int currentProtocolVersion, Class<? extends SpecificRecord> specificRecordClass) {
    this.magicByte = Optional.of((byte) magicByte);
    this.currentProtocolVersion = Optional.of(currentProtocolVersion);
    this.protocolVersionStoredInHeader = true;
    this.className = specificRecordClass.getSimpleName();
    this.schema = SpecificData.get().getSchema(specificRecordClass);
  }

  /**
   * Constructor for protocols where the Avro records are stored standalone, with no
   * extra header prepended in front. These are either un-evolvable or have their
   * schema ID stored out of band from the record.
   *
   * For example, everything that goes inside of the Put of a {@link KafkaMessageEnvelope}
   * and uses the {@link com.linkedin.venice.kafka.protocol.Put#schemaId} to store
   * the protocol version.
   */
  AvroProtocolDefinition(int currentProtocolVersion, Class<? extends SpecificRecord> specificRecordClass) {
    this.magicByte = Optional.empty();
    this.currentProtocolVersion = Optional.of(currentProtocolVersion);
    this.protocolVersionStoredInHeader = false;
    this.className = specificRecordClass.getSimpleName();
    this.schema = SpecificData.get().getSchema(specificRecordClass);
  }

  /**
   * Constructor for protocols where the Avro records are stored standalone, with no
   * extra header prepended in front. These protocols are un-evolvable.
   */
  AvroProtocolDefinition(Class<? extends SpecificRecord> specificRecordClass) {
    this.magicByte = Optional.empty();
    this.currentProtocolVersion = Optional.empty();
    this.protocolVersionStoredInHeader = false;
    this.className = specificRecordClass.getSimpleName();
    this.schema = SpecificData.get().getSchema(specificRecordClass);
  }

  /**
   * Constructor for protocols where there is no SpecificRecord class, because the
   * schema defines only a primitive type, such as byte[].
   */
  AvroProtocolDefinition(int currentProtocolVersion, Schema schema, String name) {
    this.magicByte = Optional.empty();
    this.currentProtocolVersion = Optional.of(currentProtocolVersion);
    this.protocolVersionStoredInHeader = false;
    this.className = name;
    this.schema = schema;
  }

  public <T extends SpecificRecord> InternalAvroSpecificSerializer<T> getSerializer() {
    return new InternalAvroSpecificSerializer<>(this);
  }

  public int getCurrentProtocolVersion() {
    if (currentProtocolVersion.isPresent()) {
      return currentProtocolVersion.get();
    } else {
      throw new VeniceMessageException("This protocol does not have a protocol version.");
    }
  }
}
