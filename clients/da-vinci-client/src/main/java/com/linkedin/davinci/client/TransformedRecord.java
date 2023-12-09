package com.linkedin.davinci.client;

import com.linkedin.venice.serializer.AvroSerializer;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;


public class TransformedRecord<K, V> {
  private K key;
  private V value;

  public K getKey() {
    return key;
  }

  public void setKey(K key) {
    this.key = key;
  }

  public byte[] getKeyBytes(Schema schema) {
    return new AvroSerializer(schema).serialize(key);
  }

  public V getValue() {
    return value;
  }

  public void setValue(V value) {
    this.value = value;
  }

  public ByteBuffer getValueBytes(Schema schema) {
    ByteBuffer transformedBytes = ByteBuffer.wrap(new AvroSerializer(schema).serialize(value));
    ByteBuffer newBuffer = ByteBuffer.allocate(Integer.BYTES + transformedBytes.remaining());
    newBuffer.putInt(1);
    newBuffer.put(transformedBytes);
    newBuffer.flip();
    return newBuffer;
  }

}
