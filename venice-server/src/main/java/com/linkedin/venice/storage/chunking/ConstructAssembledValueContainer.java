package com.linkedin.venice.storage.chunking;

import com.linkedin.venice.storage.protocol.ChunkedValueManifest;


/**
 * Used to construct the right kind of {@param ASSEMBLED_VALUE_CONTAINER} container (according to
 * the query code) to hold a large value which needs to be incrementally re-assembled from many
 * smaller chunks.
 */
public interface ConstructAssembledValueContainer<ASSEMBLED_VALUE_CONTAINER> {
  ASSEMBLED_VALUE_CONTAINER construct(ChunkedValueManifest chunkedValueManifest);
}
