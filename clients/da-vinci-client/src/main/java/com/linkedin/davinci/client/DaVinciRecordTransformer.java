package com.linkedin.davinci.client;

import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.utils.lazy.Lazy;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;


/**
 * This abstract class can be extended in order to transform records stored in the Da Vinci Client.
 *
 * The input is what is consumed from the raw Venice data set, whereas the output is what is stored
 * into Da Vinci's local storage (e.g. RocksDB).
 * 
 * Please note that your implementation should be thread safe, and that schema evolution is possible.
 *
 * N.B.: The inputs are wrapped inside {@link Lazy} so that if the implementation need not look at
 * them, the deserialization cost is not paid.
 *
 * @param <K> type of the key value
 * @param <V> type of the input value
 * @param <O> type of the output value
 */
public abstract class DaVinciRecordTransformer<K, V, O> {
  /**
   * Version of the store of when the transformer is initialized.
   */
  public final int storeVersion;

  public DaVinciRecordTransformer(int storeVersion) {
    this.storeVersion = storeVersion;
  }

  /**
   * This will be the type returned by the {@link DaVinciClient}'s read operations.
   *
   * @return a {@link Schema} corresponding to the type of {@link O}.
   */
  public abstract Schema getKeyOutputSchema();

  public abstract Schema getValueOutputSchema();

  /**
   * @param key to be put
   * @param value to be put
   * @return the object to keep in storage, or null if the put should be skipped
   */
  public abstract O put(Lazy<V> value);

  /**
   * By default, deletes will proceed. This can be overridden if some deleted records should be kept.
   *
   * @param key to be deleted
   * @return the object to keep in storage, or null to proceed with the deletion
   */
  public O delete(Lazy<K> key) {
    return null;
  }

  /**
   * By default, takes a value, serializes it and wrap it in a ByteByffer.
   * 
   * @param schema the Avro schema defining the serialization format
   * @param value value the value to be serialized
   * @return a ByteBuffer containing the serialized value wrapped according to Avro specifications
   */
  public ByteBuffer getValueBytes(Schema schema, V value) {
    ByteBuffer transformedBytes = ByteBuffer.wrap(new AvroSerializer(schema).serialize(value));
    ByteBuffer newBuffer = ByteBuffer.allocate(Integer.BYTES + transformedBytes.remaining());
    newBuffer.putInt(1);
    newBuffer.put(transformedBytes);
    newBuffer.flip();
    return newBuffer;
  }
}
