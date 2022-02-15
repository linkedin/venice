package com.linkedin.venice.serializer;


import com.linkedin.venice.exceptions.VeniceException;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import org.apache.avro.io.BinaryEncoder;


public interface RecordSerializer<T> {
  interface ReusableObjects {
    BinaryEncoder getBinaryEncoder();
    ByteArrayOutputStream getByteArrayOutputStream();
  }

  byte[] serialize(T object) throws VeniceException;

  byte[] serialize(T object, ReusableObjects reuse) throws VeniceException;

  byte[] serialize(T object, BinaryEncoder reusedEncoder, ByteArrayOutputStream reusedOutputStream) throws VeniceException;

  byte[] serializeObjects(Iterable<T> objects) throws VeniceException;

  byte[] serializeObjects(Iterable<T> objects, ReusableObjects reuse) throws VeniceException;

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

  byte[] serializeObjects(Iterable<T> objects, ByteBuffer prefix, ReusableObjects reuse) throws VeniceException;

  byte[] serializeObjects(Iterable<T> objects, ByteBuffer prefix, BinaryEncoder reusedEncoder, ByteArrayOutputStream reusedOutputStream) throws VeniceException;
}
