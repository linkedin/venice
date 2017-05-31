package com.linkedin.venice.serializer;


import com.linkedin.venice.exceptions.VeniceException;

import java.util.Arrays;

public interface RecordSerializer<T> {

  byte[] serialize(T object) throws VeniceException;

  byte[] serializeObjects(Iterable<T> objects) throws VeniceException;
}
