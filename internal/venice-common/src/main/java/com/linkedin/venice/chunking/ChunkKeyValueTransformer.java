package com.linkedin.venice.chunking;

import static com.linkedin.venice.chunking.ChunkKeyValueTransformer.KeyType.WITH_CHUNK_MANIFEST;
import static com.linkedin.venice.chunking.ChunkKeyValueTransformer.KeyType.WITH_FULL_VALUE;
import static com.linkedin.venice.chunking.ChunkKeyValueTransformer.KeyType.WITH_VALUE_CHUNK;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
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

  static ChunkKeyValueTransformer.KeyType getKeyType(final MessageType messageType, final int schemaId) {
    if (schemaId == AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion()) {
      return WITH_VALUE_CHUNK;
    } else if (schemaId == AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion()) {
      return WITH_CHUNK_MANIFEST;
    } else if (schemaId > 0 || messageType == MessageType.DELETE) {
      return WITH_FULL_VALUE;
    } else {
      throw new VeniceException("Cannot categorize key type with schema ID: " + schemaId);
    }
  }
}
