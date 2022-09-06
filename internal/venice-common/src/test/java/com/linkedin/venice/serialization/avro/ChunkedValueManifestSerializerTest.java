package com.linkedin.venice.serialization.avro;

import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ChunkedValueManifestSerializerTest {
  private static final ChunkedValueManifestSerializer CHUNKED_VALUE_MANIFEST_SERIALIZER =
      new ChunkedValueManifestSerializer(false);

  @Test
  public void testSerializerWithHeader() {
    byte[] keyWithChunkIdSuffix = "keyChunkedKeySuffix".getBytes();

    ChunkedValueManifest chunkedValueManifest = new ChunkedValueManifest();
    chunkedValueManifest.keysWithChunkIdSuffix = new ArrayList<>();
    chunkedValueManifest.keysWithChunkIdSuffix.add(ByteBuffer.wrap(keyWithChunkIdSuffix));
    chunkedValueManifest.schemaId = 1;
    chunkedValueManifest.size = 1;
    byte[] serialized = CHUNKED_VALUE_MANIFEST_SERIALIZER.serialize("", chunkedValueManifest);
    ChunkedValueManifest deserialized = CHUNKED_VALUE_MANIFEST_SERIALIZER
        .deserialize(serialized, AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion());

    Assert.assertEquals(deserialized.keysWithChunkIdSuffix.size(), 1);
    Assert.assertTrue(Arrays.equals(deserialized.keysWithChunkIdSuffix.get(0).array(), keyWithChunkIdSuffix));
    Assert.assertEquals(deserialized.schemaId, 1);
    Assert.assertEquals(deserialized.size, 1);
    // Assert that serializer with header adds desired padding at the beginning
    for (int i = 0; i < ByteUtils.SIZE_OF_INT; i++) {
      Assert.assertEquals(serialized[i], 0);
    }
  }
}
