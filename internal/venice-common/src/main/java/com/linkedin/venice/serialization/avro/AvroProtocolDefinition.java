package com.linkedin.venice.serialization.avro;

import com.linkedin.venice.admin.protocol.response.AdminResponseRecord;
import com.linkedin.venice.client.change.capture.protocol.RecordChangeEvent;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.ingestion.protocol.IngestionMetricsReport;
import com.linkedin.venice.ingestion.protocol.IngestionStorageMetadata;
import com.linkedin.venice.ingestion.protocol.IngestionTaskCommand;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.ingestion.protocol.LoadedStoreUserPartitionMapping;
import com.linkedin.venice.ingestion.protocol.ProcessShutdownCommand;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.metadata.response.MetadataResponseRecord;
import com.linkedin.venice.participant.protocol.ParticipantMessageValue;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import com.linkedin.venice.pushstatus.PushStatusKey;
import com.linkedin.venice.pushstatus.PushStatusValue;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatValue;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
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
  KAFKA_MESSAGE_ENVELOPE(23, 11, KafkaMessageEnvelope.class),

  /**
   * Used to persist the state of a partition in Storage Nodes, including offset,
   * Data Ingest Validation state, etc.
   */
  PARTITION_STATE(24, 14, PartitionState.class),

  /**
   * Used to persist state related to a store-version, including Start of Buffer Replay
   * offsets and whether the input is sorted.
   */
  STORE_VERSION_STATE(25, 7, StoreVersionState.class),

  /**
   * Used to encode push job details records to be written to the PushJobDetails system store.
   */
  PUSH_JOB_DETAILS(26, 4, PushJobDetails.class),

  /**
   * Used to encode metadata changes about the system as a whole. Records of this type
   * are propagated via the "admin channel" special Kafka topic. It is stored in the
   * {@link Put} of a {@link KafkaMessageEnvelope}, and thus leverages the envelope
   * for versioning.
   *
   * TODO: Move AdminOperation to venice-common module so that we can properly reference it here.
   */
  ADMIN_OPERATION(81, SpecificData.get().getSchema(ByteBuffer.class), "AdminOperation"),

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
  CHUNKED_KEY_SUFFIX(ChunkedKeySuffix.class),

  /**
   * Used to encode various kinds of ingestion task commands, which are used to control ingestion task in child process.
   */
  INGESTION_TASK_COMMAND(28, 1, IngestionTaskCommand.class),

  /**
   * Used to encode status of ingestion task, that are reported backed from child process to Storage Node / Da Vinci backend.
   */
  INGESTION_TASK_REPORT(29, 1, IngestionTaskReport.class),

  /**
   * Used to encode metrics collected from ingestion task, that are reported backed from child process to Storage Node / Da Vinci backend.
   */
  INGESTION_METRICS_REPORT(30, 1, IngestionMetricsReport.class),

  /**
   * Used to encode storage metadata updates that are reported backed from Storage Node / Da Vinci backend to child process.
   */
  INGESTION_STORAGE_METADATA(31, 1, IngestionStorageMetadata.class),

  /**
   * Used to encode various kinds of ingestion task commands, which are used to control ingestion task in child process.
   */
  PROCESS_SHUTDOWN_COMMAND(32, 1, ProcessShutdownCommand.class),

  BATCH_JOB_HEARTBEAT(33, 1, BatchJobHeartbeatValue.class),

  /**
   * Used to encode the position of a PubSub message.
   */
  PUBSUB_POSITION_WIRE_FORMAT(34, 1, PubSubPositionWireFormat.class),

  /**
   * Used to retrieve the loaded store partition mapping in the isolated process.
   * In theory, we don't need to use magicByte for the communication with II process.
   */
  LOADED_STORE_USER_PARTITION_MAPPING(35, 1, LoadedStoreUserPartitionMapping.class),

  /**
   * Key schema for metadata system store.
   */
  METADATA_SYSTEM_SCHEMA_STORE_KEY(StoreMetaKey.class),

  /**
   * Value schema for metadata system store.
   */
  METADATA_SYSTEM_SCHEMA_STORE(24, StoreMetaValue.class),

  /**
   * Key schema for push status system store.
   */
  PUSH_STATUS_SYSTEM_SCHEMA_STORE_KEY(PushStatusKey.class),

  /**
   * Value schema for push status system store.
   */
  PUSH_STATUS_SYSTEM_SCHEMA_STORE(1, PushStatusValue.class),

  /**
   * Value schema for participant system stores.
   */
  PARTICIPANT_MESSAGE_SYSTEM_STORE_VALUE(1, ParticipantMessageValue.class),

  /**
   * Response record for admin request.
   */
  SERVER_ADMIN_RESPONSE(2, AdminResponseRecord.class),

  /**
   * Response record for metadata fetch request.
   */
  SERVER_METADATA_RESPONSE(2, MetadataResponseRecord.class),

  /**
   * Value schema for change capture event.
   * TODO: Figure out a way to pull in protocol from different view class.
   */
  RECORD_CHANGE_EVENT(1, RecordChangeEvent.class);

  private static final Set<Byte> magicByteSet = validateMagicBytes();

  private static Set<Byte> validateMagicBytes() {
    Set<Byte> magicByteSet = new HashSet<>();
    for (AvroProtocolDefinition avroProtocolDefinition: AvroProtocolDefinition.values()) {
      if (avroProtocolDefinition.magicByte.isPresent()) {
        if (magicByteSet.contains(avroProtocolDefinition.magicByte.get())) {
          throw new VeniceException("Duplicate magic byte found for: " + avroProtocolDefinition.name());
        } else {
          magicByteSet.add(avroProtocolDefinition.magicByte.get());
        }
      }
    }
    return magicByteSet;
  }

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
  public final Optional<Integer> currentProtocolVersion;

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
  AvroProtocolDefinition(
      int magicByte,
      int currentProtocolVersion,
      Class<? extends SpecificRecord> specificRecordClass) {
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
   * For example, everything that goes inside the Put message of a {@link KafkaMessageEnvelope}
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
    if (magicByte.isPresent() || protocolVersionStoredInHeader) {
      return new InternalAvroSpecificSerializer<>(this);
    }
    return new InternalAvroSpecificSerializer<>(this, 0);
  }

  public int getCurrentProtocolVersion() {
    if (currentProtocolVersion.isPresent()) {
      return currentProtocolVersion.get();
    } else {
      throw new VeniceMessageException("This protocol does not have a protocol version.");
    }
  }

  public Schema getCurrentProtocolVersionSchema() {
    return schema;
  }

  public Optional<Byte> getMagicByte() {
    return magicByte;
  }

  public String getClassName() {
    return className;
  }

  public String getSystemStoreName() {
    return String.format(Store.SYSTEM_STORE_FORMAT, name());
  }
}
