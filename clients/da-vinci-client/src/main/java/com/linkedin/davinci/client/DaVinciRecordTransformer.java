package com.linkedin.davinci.client;

import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.annotation.Experimental;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
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
  public abstract Schema getKeyOutputSchema();

  /**
   * Returns the schema for the output value used in {@link DaVinciClient}'s operations.
   *
   * @return a {@link Schema} corresponding to the type of {@link O}.
   */
  public abstract Schema getValueOutputSchema();

  /**
   * Implement this method to transform records before they are stored.
   * This can be useful for tasks such as filtering out unused fields to save storage space.
   *
   * @param key the key of the record to be transformed
   * @param value the value of the record to be transformed
   * @return the transformed value
   */
  public abstract O transform(Lazy<K> key, Lazy<V> value);

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
   * By default, records will be deleted. Return a non-null object to keep the record in storage.
   *
   * @param key the key of the record to be deleted
   * @return the object to keep in storage, or null to proceed with the deletion
   */
  public O processDelete(Lazy<K> key) {
    return null;
  }

  /**
   * Lifecycle event triggered before records are consumed.
   * Use this method to perform setup operations such as opening database connections or creating tables.
   *
   * By default, it performs no operation.
   */
  public void onStart() {
    return;
  }

  /**
   * Lifecycle event triggered after records are done consuming.
   * Use this method to perform cleanup operations such as closing database connections or dropping tables.
   *
   * By default, it performs no operation.
   */
  public void onEnd() {
    return;
  }

  /**
   * Transforms and processes the given record.
   *
   * @param key the key of the record to be put
   * @param value the value of the record to be put
   * @return the transformed record
   */
  public final O transformAndProcessPut(Lazy<K> key, Lazy<V> value) {
    O transformedRecord = transform(key, value);
    processPut(key, Lazy.of(() -> transformedRecord));

    if (!storeRecordsInDaVinci) {
      return null;
    }
    return transformedRecord;
  }

  /**
   * Takes a value, serializes it and wraps it in a ByteByffer.
   *
   * @param value the value to be serialized
   * @return a ByteBuffer containing the serialized value wrapped according to Avro specifications
   */
  public final ByteBuffer getValueBytes(O value) {
    return recordTransformerUtility.getValueBytes(value);
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
  public final void onRecovery(AbstractStorageEngine storageEngine, Integer partition) {
    recordTransformerUtility.onRecovery(storageEngine, partition);
  }

  /**
   * @return {@link #storeRecordsInDaVinci}
   */
  public final boolean getStoreRecordsInDaVinci() {
    return storeRecordsInDaVinci;
  }

  public final Class<O> getOutputValueClass() {
    Type superclass = getClass().getGenericSuperclass();
    if (superclass instanceof ParameterizedType) {
      return (Class<O>) ((ParameterizedType) superclass).getActualTypeArguments()[2];
    }
    throw new VeniceException("Invalid DaVinciRecordTransformer class definition");
  }

  public final DaVinciRecordTransformerUtility<K, O> getRecordTransformerUtility() {
    return recordTransformerUtility;
  }
}
