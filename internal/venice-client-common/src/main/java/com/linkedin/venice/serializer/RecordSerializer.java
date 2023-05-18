package com.linkedin.venice.serializer;

import com.linkedin.venice.exceptions.VeniceException;
import java.nio.ByteBuffer;


public interface RecordSerializer<T> {
  byte[] serialize(T object) throws VeniceException;

  byte[] serializeObjects(Iterable<T> objects) throws VeniceException;

  /**
   * Serialize a list of objects and put the prefix before the serialized objects.
   * This function could avoid unnecessary byte array copy when you want to serialize
   * two different kinds of objects together.
   * Essentially, the {@param prefix} will be the serialized byte array of the first
   * kind of objects.
   *
   * @param objects
   * @param prefix
   * @return
   * @throws VeniceException
   */
  byte[] serializeObjects(Iterable<T> objects, ByteBuffer prefix) throws VeniceException;
}
