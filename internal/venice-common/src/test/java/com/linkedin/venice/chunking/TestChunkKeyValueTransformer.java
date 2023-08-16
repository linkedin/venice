package com.linkedin.venice.chunking;

import static com.linkedin.venice.chunking.ChunkKeyValueTransformer.KeyType.WITH_CHUNK_MANIFEST;
import static com.linkedin.venice.chunking.ChunkKeyValueTransformer.KeyType.WITH_FULL_VALUE;
import static com.linkedin.venice.chunking.ChunkKeyValueTransformer.KeyType.WITH_VALUE_CHUNK;
import static com.linkedin.venice.kafka.protocol.enums.MessageType.DELETE;
import static com.linkedin.venice.kafka.protocol.enums.MessageType.PUT;
import static com.linkedin.venice.serialization.avro.AvroProtocolDefinition.CHUNK;
import static com.linkedin.venice.serialization.avro.AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST;

import com.linkedin.venice.exceptions.VeniceException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestChunkKeyValueTransformer {
  @Test
  public void testGetKeyType() {
    Assert.assertEquals(ChunkKeyValueTransformer.getKeyType(PUT, CHUNK.getCurrentProtocolVersion()), WITH_VALUE_CHUNK);
    Assert.assertEquals(
        ChunkKeyValueTransformer.getKeyType(PUT, CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion()),
        WITH_CHUNK_MANIFEST);
    Assert.assertEquals(ChunkKeyValueTransformer.getKeyType(PUT, 10), WITH_FULL_VALUE);
    Assert.assertEquals(ChunkKeyValueTransformer.getKeyType(DELETE, -1), WITH_FULL_VALUE);

    // For PUT message, any negative schema id other than the ones for chunk and manifest should throw an exception
    Assert.assertThrows(VeniceException.class, () -> ChunkKeyValueTransformer.getKeyType(PUT, -4));
  }
}
