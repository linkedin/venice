package com.linkedin.venice.hadoop.input.kafka.chunk;

import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;


/**
 * This interface provides methods to split a key into raw key/value byte array and {@link ChunkedKeySuffix}.
 */
public interface ChunkKeyValueTransformer {
  enum KeyType {
    WITH_FULL_VALUE, // A complete/full regular key
    WITH_VALUE_CHUNK, // Composite key carrying value chunk information
    WITH_CHUNK_MANIFEST // Composite key carrying chunk manifest information
  }

  RawKeyBytesAndChunkedKeySuffix splitChunkedKey(byte[] keyBytes, KeyType keyType);
}
