package com.linkedin.venice.serializer;


import com.linkedin.venice.exceptions.VeniceException;

import java.util.Iterator;

public interface RecordDeserializer<T> {

  T deserialize(byte[] bytes) throws VeniceException;

  Iterable<T> deserializeObjects(byte[] bytes) throws VeniceException;
}
