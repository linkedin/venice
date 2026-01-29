package com.linkedin.davinci.client;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.annotation.Experimental;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.pubsub.PubSubContext;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import org.apache.avro.Schema;
import org.objectweb.asm.ClassReader;


/**
 * Plugin interface for Da Vinci that lets applications register callbacks (puts, deletes, lifecycle) and optionally
 * transform values during ingestion. Use it to mirror updates into external systems while still benefiting from
 * Da Vinci's local cache, or to change what gets stored locally.
 *
 * One transformer instance is created per store version, and lifecycle hooks are invoked per version. During
 * startup, Da Vinci replays records persisted on disk by invoking {@link #processPut(Lazy, Lazy, int, DaVinciRecordTransformerRecordMetadata)} so
 * external systems can be rehydrated.
 *
 * Typical setup for most users: keep persisting in Da Vinci (default) and implement callbacks that forward updates to
 * your own storage.
 *
 * Notes:
 * - Implementations must be thread-safe.
 * - Inputs are wrapped in {@link Lazy} to avoid deserialization unless used.
 *
 * @param <K> input key type from Venice
 * @param <V> input value type from Venice
 * @param <O> output value type (post-transformation) that's stored in Da Vinci and forwarded
 */
@Experimental
public abstract class DaVinciRecordTransformer<K, V, O> implements Closeable {
  /**
   * Name of the Venice store without version info.
   */
  private final String storeName;

  /**
   * Version of the store of when the transformer is initialized.
   */
  private final int storeVersion;

  /**
   * Boolean to determine if records should be stored in Da Vinci.
   */
  private final boolean storeRecordsInDaVinci;

  /**
   * Boolean to determine if we should always bootstrap from the Version Topic.
   */
  private final boolean alwaysBootstrapFromVersionTopic;

  /**
   * The key schema, which is immutable inside DaVinciClient. Users can modify the key if they are storing records in an external storage engine, but this must be managed by the user.
   */
  private final Schema keySchema;

  /**
   * The value schema before transformation, which is provided by the DaVinciClient.
   */
  private final Schema inputValueSchema;

  /**
   * The value schema after transformation, which is provided by the user.
   */
  private final Schema outputValueSchema;

  private final DaVinciRecordTransformerUtility<K, O> recordTransformerUtility;

  /**
   * @param storeName the name of the Venice store without version info
   * @param storeVersion the version of the store
   * @param keySchema the key schema, which is immutable inside DaVinciClient. Users can modify the key if they are storing records in an external storage engine, but this must be managed by the user
   * @param inputValueSchema the value schema before transformation
   * @param outputValueSchema the value schema after transformation
   * @param recordTransformerConfig the config for the record transformer
   */
  public DaVinciRecordTransformer(
      String storeName,
      int storeVersion,
      Schema keySchema,
      Schema inputValueSchema,
      Schema outputValueSchema,
      DaVinciRecordTransformerConfig recordTransformerConfig) {
    this.storeName = storeName;
    this.storeVersion = storeVersion;
    this.storeRecordsInDaVinci = recordTransformerConfig.getStoreRecordsInDaVinci();
    this.alwaysBootstrapFromVersionTopic = recordTransformerConfig.getAlwaysBootstrapFromVersionTopic();
    this.keySchema = keySchema;
    // ToDo: Make use of inputValueSchema to support reader/writer schemas
    this.inputValueSchema = inputValueSchema;
    this.outputValueSchema = outputValueSchema;
    this.recordTransformerUtility = new DaVinciRecordTransformerUtility<>(this, recordTransformerConfig);
  }

  /**
   * Callback to transform records before they are stored.
   * This can be useful for tasks such as filtering out unused fields to save storage space.
   *
   * Result types:
   * - SKIP: drop the record
   * - UNCHANGED: keep the original value; useful when only callbacks/notifications are needed
   * - TRANSFORMED: record has been transformed and should be persisted to disk
   *
   * @param key the key of the record to be transformed
   * @param value the value of the record to be transformed
   * @param partitionId what partition the record came from
   * @param recordMetadata returns {@link DaVinciRecordTransformerRecordMetadata} if enabled in {@link DaVinciRecordTransformerConfig}, null otherwise
   * @return {@link DaVinciRecordTransformerResult}
   */
  public abstract DaVinciRecordTransformerResult<O> transform(
      Lazy<K> key,
      Lazy<V> value,
      int partitionId,
      DaVinciRecordTransformerRecordMetadata recordMetadata);

  /**
   * Callback for put/update events (for example, write to an external system). Invoked after
   * {@link #transform(Lazy, Lazy, int, DaVinciRecordTransformerRecordMetadata)} when the result is UNCHANGED or TRANSFORMED. Also invoked during
   * startup/recovery when Da Vinci replays records persisted on disk to rehydrate external systems.
   *
   * @param key the key of the record to be put
   * @param value the value of the record to be put, either the original value or the transformed value
   * @param partitionId what partition the record came from
   * @param recordMetadata returns {@link DaVinciRecordTransformerRecordMetadata} if enabled in {@link DaVinciRecordTransformerConfig}, null otherwise.
   */
  public abstract void processPut(
      Lazy<K> key,
      Lazy<O> value,
      int partitionId,
      DaVinciRecordTransformerRecordMetadata recordMetadata);

  /**
   * Optional callback for delete events (for example, remove from an external system).
   *
   * By default, this is a no-op.
   *
   * @param key the key of the record to be deleted
   * @param partitionId what partition the record is being deleted from
   * @param recordMetadata returns {@link DaVinciRecordTransformerRecordMetadata} if enabled in {@link DaVinciRecordTransformerConfig}, null otherwise.
   */
  public void processDelete(Lazy<K> key, int partitionId, DaVinciRecordTransformerRecordMetadata recordMetadata) {
    return;
  };

  /**
   * Callback invoked before consuming records for {@link #storeVersion}. Use this to open connections, create
   * tables, or initialize resources.
   *
   * By default, it's a no-op.
   *
   * @param partitionId what partition is being subscribed
   * @param isCurrentVersion whether the version being started is the current serving version; if not it's a future version
   */
  public void onStartVersionIngestion(int partitionId, boolean isCurrentVersion) {
    return;
  }

  /**
   * Callback invoked when record consumption stops for {@link #storeVersion}. Use this to close connections,
   * drop tables, or release resources.
   *
   * This can be triggered when this {@link #storeVersion} is no longer the current serving version (for example, after
   * a version swap), or when the application is shutting down.
   *
   * For batch stores, this callback may be invoked even when the application is not shutting down, with
   * {@link #storeVersion} equaling the {@code currentVersion} parameter. This occurs because batch stores do not
   * continuously receive updates after batch ingestion completes unlike hybrid stores, so the consumer resources
   * are closed to free up resources once all data has been consumed.
   *
   * By default, it's a no-op.
   *
   * @param currentVersion the current serving version at the time this callback is invoked
   */
  public void onEndVersionIngestion(int currentVersion) {
    return;
  }

  /**
   * Whether to deserialize input values using a single, uniform schema.
   *
   * If true (recommended only when you need a consistent field set), it uses the latest registered value schema at
   * startup time to deserialize all records. This is useful when projecting into systems that expect a fixed schema (for example, relational tables).
   *
   * If false (default), each record is deserialized with its own writer schema.
   *
   * Note: This flag only applies to {@link org.apache.avro.generic.GenericRecord} values. When using {@link org.apache.avro.specific.SpecificRecord} for values via
   * {@link DaVinciRecordTransformerConfig.Builder#setOutputValueClass(Class)}, values will be deserialized based on the schema passed into
   * {@link DaVinciRecordTransformerConfig.Builder#setOutputValueSchema(org.apache.avro.Schema)}.
   */
  public boolean useUniformInputValueSchema() {
    return false;
  }

  // ===== User-facing getters below =====

  /**
   * @return {@link #storeName}
   */
  public final String getStoreName() {
    return storeName;
  }

  /**
   * @return {@link #storeVersion}
   */
  public final int getStoreVersion() {
    return storeVersion;
  }

  /**
   * Returns the schema for the key used in {@link DaVinciClient}'s operations.
   *
   * @return a {@link Schema} corresponding to the type of {@link K}.
   */
  public final Schema getKeySchema() {
    return keySchema;
  }

  /**
   * Returns the schema for the input value used in {@link DaVinciClient}'s operations.
   *
   * @return a {@link Schema} corresponding to the type of {@link V}.
   */
  public final Schema getInputValueSchema() {
    return inputValueSchema;
  }

  /**
   * Returns the schema for the output value used in {@link DaVinciClient}'s operations.
   *
   * @return a {@link Schema} corresponding to the type of {@link O}.
   */
  public final Schema getOutputValueSchema() {
    return outputValueSchema;
  }

  // ===== Internal use only methods below =====

  /**
   * @return {@link #storeRecordsInDaVinci}
   */
  public final boolean getStoreRecordsInDaVinci() {
    return storeRecordsInDaVinci;
  }

  /**
   * @return {@link #alwaysBootstrapFromVersionTopic}
   */
  public final boolean getAlwaysBootstrapFromVersionTopic() {
    return alwaysBootstrapFromVersionTopic;
  }

  /**
   * @return {@link #recordTransformerUtility}
   */
  public final DaVinciRecordTransformerUtility<K, O> getRecordTransformerUtility() {
    return recordTransformerUtility;
  }

  /**
   * Helper to serialize and compress the value and prepend the schema ID to the resulting ByteBuffer.
   *
   * @param value to be serialized and compressed
   * @param schemaId to prepend to the ByteBuffer
   * @return a ByteBuffer containing the schema ID followed by the serialized and compressed value
   */
  public final ByteBuffer prependSchemaIdToHeader(O value, int schemaId, VeniceCompressor compressor) {
    return recordTransformerUtility.prependSchemaIdToHeader(value, schemaId, compressor);
  }

  /**
   * Helper to prepend the given schema ID to the provided ByteBuffer.
   *
   * @param valueBytes the original serialized and compressed value
   * @param schemaId to prepend to the ByteBuffer
   * @return a ByteBuffer containing the schema ID followed by the serialized and compressed value
   */
  public final ByteBuffer prependSchemaIdToHeader(ByteBuffer valueBytes, int schemaId) {
    return recordTransformerUtility.prependSchemaIdToHeader(valueBytes, schemaId);
  }

  /**
   * Runs {@link #transform(Lazy, Lazy, int, DaVinciRecordTransformerRecordMetadata)} and then invokes {@link #processPut(Lazy, Lazy, int, DaVinciRecordTransformerRecordMetadata)} when appropriate.
   * Returns null when the record is skipped, unchanged-and-not-stored, or when {@link #getStoreRecordsInDaVinci()} is false.
   *
   * @param key the key of the record to be put
   * @param value the value of the record to be put
   * @param partitionId what partition the record came from
   * @param recordMetadata the metadata for this record
   * @return the {@link DaVinciRecordTransformerResult} to be stored in Da Vinci, or null if nothing should be stored
   */
  public final DaVinciRecordTransformerResult<O> transformAndProcessPut(
      Lazy<K> key,
      Lazy<V> value,
      int partitionId,
      DaVinciRecordTransformerRecordMetadata recordMetadata) {
    DaVinciRecordTransformerResult<O> transformerResult = transform(key, value, partitionId, recordMetadata);
    DaVinciRecordTransformerResult.Result result = transformerResult.getResult();
    if (result == DaVinciRecordTransformerResult.Result.SKIP) {
      return null;
    } else if (result == DaVinciRecordTransformerResult.Result.UNCHANGED) {
      processPut(key, (Lazy<O>) value, partitionId, recordMetadata);
    } else {
      O transformedRecord = transformerResult.getValue();
      processPut(key, Lazy.of(() -> transformedRecord), partitionId, recordMetadata);
    }

    if (!getStoreRecordsInDaVinci()) {
      return null;
    }
    return transformerResult;
  }

  /**
   * Used during startup/recovery to iterate over records persisted on disk and invoke {@link #processPut(Lazy, Lazy, int, DaVinciRecordTransformerRecordMetadata)}
   * to rehydrate external systems.
   */
  public final void onRecovery(
      StorageEngine storageEngine,
      int partitionId,
      InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer,
      Lazy<VeniceCompressor> compressor,
      PubSubContext pubSubContext,
      Map<Integer, Schema> schemaIdToSchemaMap,
      ReadOnlySchemaRepository schemaRepository) {
    recordTransformerUtility.onRecovery(
        storageEngine,
        partitionId,
        partitionStateSerializer,
        compressor,
        pubSubContext,
        schemaIdToSchemaMap,
        schemaRepository);
  }

  /**
   * Returns a hash of this class's bytecode. Used to detect implementation changes during bootstrapping and decide
   * whether local data should be considered stale and wiped.
   */
  @VisibleForTesting
  public final int getClassHash() {
    String className = this.getClass().getName().replace('.', '/') + ".class";
    try (InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(className)) {
      ClassReader classReader = new ClassReader(inputStream);
      byte[] bytecode = classReader.b;
      return Arrays.hashCode(bytecode);

    } catch (IOException e) {
      throw new VeniceException("Failed to get classHash", e);
    }
  }
}
