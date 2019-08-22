package com.linkedin.venice.storage.chunking;

/**
 * Used to wrap a large value re-assembled with the use of a {@param ASSEMBLED_VALUE_CONTAINER}
 * into the right type of {@param VALUE} class needed by the query code.
 */
public interface ContainerToValue<ASSEMBLED_VALUE_CONTAINER, VALUE> {
  VALUE construct(int schemaId, ASSEMBLED_VALUE_CONTAINER container);
}
