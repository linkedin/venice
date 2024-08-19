package com.linkedin.davinci.client;

import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.serializer.AvroGenericDeserializer;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.rocksdb.RocksIterator;


/**
 * Utility class for {@link DaVinciRecordTransformer}.
 *
 * @param <K> the type of the input key
 * @param <O> the type of the output value
 */
public class DaVinciRecordTransformerUtility<K, O> {
  private final DaVinciRecordTransformer recordTransformer;
  private AvroGenericDeserializer<K> keyDeserializer;
  private AvroGenericDeserializer<O> outputValueDeserializer;
  private AvroSerializer<O> outputValueSerializer;

  public DaVinciRecordTransformerUtility(DaVinciRecordTransformer recordTransformer) {
    this.recordTransformer = recordTransformer;
  }

  /**
   * Takes a value, serializes it and wraps it in a ByteByffer.
   *
   * @param value the value to be serialized
   * @return a ByteBuffer containing the serialized value wrapped according to Avro specifications
   */
  public final ByteBuffer getValueBytes(O value) {
    ByteBuffer transformedBytes = ByteBuffer.wrap(getOutputValueSerializer().serialize(value));
    ByteBuffer newBuffer = ByteBuffer.allocate(Integer.BYTES + transformedBytes.remaining());
    newBuffer.putInt(1);
    newBuffer.put(transformedBytes);
    newBuffer.flip();
    return newBuffer;
  }

  /**
   * @return true if the transformation logic has changed since the last time the class was loaded
   */
  public boolean hasTransformationLogicChanged(int classHash) {
    try {
      String classHashPath = String.format("./classHash-%d.txt", recordTransformer.getStoreVersion());
      File f = new File(classHashPath);
      if (f.exists()) {
        try (BufferedReader br = new BufferedReader(new FileReader(classHashPath))) {
          int storedClassHash = Integer.parseInt(br.readLine());
          if (storedClassHash == classHash) {
            return false;
          }
        }
      }

      try (FileWriter fw = new FileWriter(classHashPath)) {
        fw.write(String.valueOf(classHash));
      }
      return true;
    } catch (IOException e) {
      throw new VeniceException("Failed to check if transformation logic has changed", e);
    }
  }

  /**
   * Bootstraps the client after it comes online.
   */
  public final void onRecovery(AbstractStorageEngine storageEngine, Integer partition) {
    // ToDo: Store class hash in RocksDB to support blob transfer
    int classHash = recordTransformer.getClassHash();
    boolean transformationLogicChanged = hasTransformationLogicChanged(classHash);

    if (!recordTransformer.getStoreRecordsInDaVinci() || transformationLogicChanged) {
      // Bootstrap from VT
      storageEngine.clearPartitionOffset(partition);
    } else {
      // Bootstrap from local storage
      RocksIterator iterator = storageEngine.getRocksDBIterator(partition);
      for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
        byte[] keyBytes = iterator.key();
        byte[] valueBytes = iterator.value();
        Lazy<K> lazyKey = Lazy.of(() -> getKeyDeserializer().deserialize(ByteBuffer.wrap(keyBytes)));
        Lazy<O> lazyValue = Lazy.of(() -> getOutputValueDeserializer().deserialize(ByteBuffer.wrap(valueBytes)));
        recordTransformer.processPut(lazyKey, lazyValue);
      }
    }
  }

  public AvroGenericDeserializer<K> getKeyDeserializer() {
    if (outputValueDeserializer == null) {
      Schema keySchema = recordTransformer.getKeyOutputSchema();
      keyDeserializer = new AvroGenericDeserializer<>(keySchema, keySchema);
    }
    return keyDeserializer;
  }

  public AvroGenericDeserializer<O> getOutputValueDeserializer() {
    if (outputValueDeserializer == null) {
      Schema outputValueSchema = recordTransformer.getValueOutputSchema();
      outputValueDeserializer = new AvroGenericDeserializer<>(outputValueSchema, outputValueSchema);
    }
    return outputValueDeserializer;
  }

  public AvroSerializer<O> getOutputValueSerializer() {
    if (outputValueSerializer == null) {
      Schema outputValueSchema = recordTransformer.getValueOutputSchema();
      outputValueSerializer = new AvroSerializer<>(outputValueSchema);
    }
    return outputValueSerializer;
  }
}
