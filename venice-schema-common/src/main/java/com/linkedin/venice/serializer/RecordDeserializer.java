package com.linkedin.venice.serializer;


import com.linkedin.venice.exceptions.VeniceException;
import java.io.InputStream;
import org.apache.avro.io.BinaryDecoder;


public interface RecordDeserializer<T> {

  T deserialize(byte[] bytes) throws VeniceException;

  Iterable<T> deserializeObjects(byte[] bytes) throws VeniceException;

  Iterable<T> deserializeObjects(BinaryDecoder binaryDecoder) throws VeniceException;
}
