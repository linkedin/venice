package com.linkedin.venice.meta;

import java.io.IOException;


/**
 * Interface defines how to serialize and deserialize the venice object.
 */
public interface VeniceSerializer<T> {
  public byte[] serialize(T object, String path) throws IOException;

  public T deserialize(byte[] bytes, String path) throws IOException;
}
