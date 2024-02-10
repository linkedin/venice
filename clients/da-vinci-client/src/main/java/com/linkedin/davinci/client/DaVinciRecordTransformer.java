package com.linkedin.davinci.client;

import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.avro.Schema;


/**
 * This interface can be implemented in order to transform records stored in the Da Vinci Client.
 *
 * The input is what is consumed from the raw Venice data set, whereas the output is what is stored
 * into Da Vinci's local storage (e.g. RocksDB).
 *
 * N.B.: The inputs are wrapped inside {@link Lazy} so that if the implementation need not look at
 * them, the deserialization cost is not paid.
 *
 * @param <K> type of the input key
 * @param <V> type of the input value
 * @param <O> type of the output value
 */
public interface DaVinciRecordTransformer<K, V, O extends TransformedRecord> {
  /**
   * This will be the type returned by the {@link DaVinciClient}'s read operations.
   *
   * @return a {@link Schema} corresponding to the type of {@link O}.
   */
  Schema getKeyOutputSchema();

  Schema getValueOutputSchema();

  /**
   * @param key to be put
   * @param value to be put
   * @return the object to keep in storage, or null if the put should be skipped
   */
  O put(Lazy<K> key, Lazy<V> value);

  /**
   * By default, calls the put function to be implemented.
   * @param key to be put
   * @param value to be put
   * @param versionNumber store version number
   * @param readyToServe boolean of whether or not PartitionConsumptionState is ready to serve
   * @return the object to keep in storage, or null if the put should be skipped
   */
  default O put(Lazy<K> key, Lazy<V> value, int versionNumber, boolean readyToServe) {
    return put(key, value);
  }

  /**
   * By default, deletes will proceed. This can be overridden if some deleted records should be kept.
   *
   * @param key to be deleted
   * @return the object to keep in storage, or null to proceed with the deletion
   */
  default O delete(Lazy<K> key) {
    return null;
  }

  /**
   * By default, calls the default implementation of delete.
   *
   * @param key to be deleted
   * @param versionNumber store version number
   * @param readyToServe boolean of whether or not PartitionConsumptionState is ready to serve
   * @return the object to keep in storage, or null to proceed with the deletion
   */
  default O delete(Lazy<K> key, int versionNumber, boolean readyToServe) {
    return delete(key);
  }
}
