package com.linkedin.venice.serializer;


import com.linkedin.venice.exceptions.VeniceException;
import java.io.InputStream;


public interface RecordDeserializer<T> {

  T deserialize(byte[] bytes) throws VeniceException;

  Iterable<T> deserializeObjects(byte[] bytes) throws VeniceException;

  Iterable<T> deserializeObjects(InputStream is) throws VeniceException;
}
