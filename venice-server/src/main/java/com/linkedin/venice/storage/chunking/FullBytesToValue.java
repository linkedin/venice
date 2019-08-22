package com.linkedin.venice.storage.chunking;

/**
 * Used to wrap a small {@param fullBytes} value fetched from the storage engine into the right type
 * of {@param VALUE} class needed by the query code.
 */
public interface FullBytesToValue<VALUE> {
  VALUE construct(int schemaId, byte[] fullBytes);
}
