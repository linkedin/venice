package com.linkedin.davinci.client;

import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.annotation.Experimental;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.objectweb.asm.ClassReader;


/**
 * This abstract class can be extended in order to transform records stored in the Da Vinci Client,
 * or write to a custom storage of your choice.
 *
 * The input is what is consumed from the raw Venice data set, whereas the output is what is stored
 * into Da Vinci's local storage (e.g. RocksDB).
 *
 * Implementations should be thread-safe and support schema evolution.
 *
 * Note: Inputs are wrapped inside {@link Lazy} to avoid deserialization costs if not needed.
 *
 * @param <K> the type of the input key
 * @param <V> the type of the input value
 * @param <O> the type of the output value
 */
@Experimental
public abstract class DaVinciRecordTransformer<K, V, O> {
  /**
   * Version of the store of when the transformer is initialized.
   */
  private final int storeVersion;

  /**
   * Boolean to determine if records should be stored in Da Vinci.
   */
  private final boolean storeRecordsInDaVinci;

  private final DaVinciRecordTransformerUtility<K, O> recordTransformerUtility;

  /**
   * @param storeVersion the version of the store
   * @param storeRecordsInDaVinci set this to false if you intend to store records in a custom storage,
   *                              and not in the Da Vinci Client.
   */
  public DaVinciRecordTransformer(int storeVersion, boolean storeRecordsInDaVinci) {
    this.storeVersion = storeVersion;
    this.storeRecordsInDaVinci = storeRecordsInDaVinci;
    this.recordTransformerUtility = new DaVinciRecordTransformerUtility<>(this);
  }

  /**
   * Returns the schema for the key used in {@link DaVinciClient}'s operations.
   *
   * @return a {@link Schema} corresponding to the type of {@link K}.
   */
  public abstract Schema getKeySchema();

  /**
   * Returns the schema for the output value used in {@link DaVinciClient}'s operations.
   *
   * @return a {@link Schema} corresponding to the type of {@link O}.
   */
  public abstract Schema getOutputValueSchema();

  /**
   * Implement this method to transform records before they are stored.
   * This can be useful for tasks such as filtering out unused fields to save storage space.
   *
   * @param key the key of the record to be transformed
   * @param value the value of the record to be transformed
   * @return {@link DaVinciRecordTransformerResult}
   */
  public abstract DaVinciRecordTransformerResult<O> transform(Lazy<K> key, Lazy<V> value);

  /**
   * Implement this method to manage custom state outside the Da Vinci Client.
   *
   * @param key the key of the record to be put
   * @param value the value of the record to be put,
   *              derived from the output of {@link #transform(Lazy key, Lazy value)}
   */
  public abstract void processPut(Lazy<K> key, Lazy<O> value);

  /**
   * Override this method to customize the behavior for record deletions.
   * For example, you can use this method to delete records from a custom storage outside the Da Vinci Client.
   * By default, it performs no operation.
   *
   * @param key the key of the record to be deleted
   */
  public void processDelete(Lazy<K> key) {
    return;
  };

  /**
   * Lifecycle event triggered before consuming records for {@link #storeVersion}.
   * Use this method to perform setup operations such as opening database connections or creating tables.
   *
   * By default, it performs no operation.
   */
  public void onStartVersionIngestion() {
    return;
  }

  /**
   * Lifecycle event triggered when record consumption is stopped for {@link #storeVersion}.
   * Use this method to perform cleanup operations such as closing database connections or dropping tables.
   *
   * By default, it performs no operation.
   */
  public void onEndVersionIngestion() {
    return;
  }

  /**
   * Transforms and processes the given record.
   *
   * @param key the key of the record to be put
   * @param value the value of the record to be put
   * @return {@link DaVinciRecordTransformerResult}
   */
  public final DaVinciRecordTransformerResult<O> transformAndProcessPut(Lazy<K> key, Lazy<V> value) {
    DaVinciRecordTransformerResult<O> transformerResult = transform(key, value);
    DaVinciRecordTransformerResult.Result result = transformerResult.getResult();
    if (result == DaVinciRecordTransformerResult.Result.SKIP) {
      return null;
    } else if (result == DaVinciRecordTransformerResult.Result.UNCHANGED) {
      processPut(key, (Lazy<O>) value);
    } else {
      O transformedRecord = transformerResult.getValue();
      processPut(key, Lazy.of(() -> transformedRecord));
    }

    if (!storeRecordsInDaVinci) {
      return null;
    }
    return transformerResult;
  }

  /**
   * Serializes and compresses the value and prepends the schema ID to the resulting ByteBuffer.
   *
   * @param value to be serialized and compressed
   * @param schemaId to prepend to the ByteBuffer
   * @return a ByteBuffer containing the schema ID followed by the serialized and compressed value
   */
  public final ByteBuffer prependSchemaIdToHeader(O value, int schemaId, VeniceCompressor compressor) {
    return recordTransformerUtility.prependSchemaIdToHeader(value, schemaId, compressor);
  }

  /**
   * Prepends the given schema ID to the provided ByteBuffer
   *
   * @param valueBytes the original serialized and compressed value
   * @param schemaId to prepend to the ByteBuffer
   * @return a ByteBuffer containing the schema ID followed by the serialized and compressed value
   */
  public final ByteBuffer prependSchemaIdToHeader(ByteBuffer valueBytes, int schemaId) {
    return recordTransformerUtility.prependSchemaIdToHeader(valueBytes, schemaId);
  }

  /**
   * @return {@link #storeVersion}
   */
  public final int getStoreVersion() {
    return storeVersion;
  }

  /**
   * @return the hash of the class bytecode
   */
  // Visible for testing
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

  /**
   * Bootstraps the client after it comes online.
   */
  public final void onRecovery(
      AbstractStorageEngine storageEngine,
      Integer partition,
      Lazy<VeniceCompressor> compressor) {
    recordTransformerUtility.onRecovery(storageEngine, partition, compressor);
  }

  /**
   * @return {@link #storeRecordsInDaVinci}
   */
  public final boolean getStoreRecordsInDaVinci() {
    return storeRecordsInDaVinci;
  }

  /**
   * @return {@link #recordTransformerUtility}
   */
  public final DaVinciRecordTransformerUtility<K, O> getRecordTransformerUtility() {
    return recordTransformerUtility;
  }
}
