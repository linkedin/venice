package com.linkedin.venice.storage.chunking;

/**
 * Used to get the size in bytes of a fully assembled {@param ASSEMBLED_VALUE_CONTAINER}, in
 * order to do a sanity check before returning it to the query code.
 */
public interface GetSizeOfContainer<ASSEMBLED_VALUE_CONTAINER> {
  int sizeOf(ASSEMBLED_VALUE_CONTAINER container);
}
