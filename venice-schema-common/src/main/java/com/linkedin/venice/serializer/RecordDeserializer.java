package com.linkedin.venice.serializer;

import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.avro.io.BinaryDecoder;


public interface RecordDeserializer<T> {
  T deserialize(byte[] bytes) throws VeniceSerializationException;

  T deserialize(ByteBuffer byteBuffer) throws VeniceSerializationException;

  T deserialize(T reuse, ByteBuffer byteBuffer, BinaryDecoder reusedDecoder) throws VeniceSerializationException;

  T deserialize(T reuse, byte[] bytes) throws VeniceSerializationException;

  T deserialize(BinaryDecoder binaryDecoder) throws VeniceSerializationException;

  T deserialize(T reuse, BinaryDecoder binaryDecoder) throws VeniceSerializationException;

  T deserialize(T reuse, InputStream in, BinaryDecoder reusedDecoder) throws VeniceSerializationException;

  Iterable<T> deserializeObjects(byte[] bytes) throws VeniceSerializationException;

  Iterable<T> deserializeObjects(BinaryDecoder binaryDecoder) throws VeniceSerializationException;
}
