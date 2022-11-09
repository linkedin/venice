package com.linkedin.venice.writer;

import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import java.util.concurrent.CompletableFuture;
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
        true,
        () -> "",
        maxSizeForUserPayloadPerMessageInBytes,
        new KeyWithChunkingSuffixSerializer(),
        (x, y) -> CompletableFuture.completedFuture(null));
    Assert.assertEquals(result.getPayloadChunks().length, 5);
  }
}
