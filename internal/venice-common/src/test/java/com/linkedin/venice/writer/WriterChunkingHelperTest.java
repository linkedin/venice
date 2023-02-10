package com.linkedin.venice.writer;

import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import org.testng.Assert;
import org.testng.annotations.Test;


public class WriterChunkingHelperTest {
  @Test
  public void testChunkPayload() {
    byte[] keyBytes = new byte[10];
    byte[] valueBytes = new byte[100];
    int maxSizeForUserPayloadPerMessageInBytes = 30;
    ChunkedPayloadAndManifest result = WriterChunkingHelper.chunkPayloadAndSend(
        keyBytes,
        valueBytes,
        true,
        1,
        0,
        true,
        () -> "",
        maxSizeForUserPayloadPerMessageInBytes,
        new KeyWithChunkingSuffixSerializer(),
        (x, y) -> {});
    Assert.assertEquals(result.getPayloadChunks().length, 5);
  }
}
