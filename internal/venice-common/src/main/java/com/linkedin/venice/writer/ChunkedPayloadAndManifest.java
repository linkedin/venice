package com.linkedin.venice.writer;

import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import java.nio.ByteBuffer;


/**
 * This class contains both chunked results and manifest for a specific payload.
 */
public class ChunkedPayloadAndManifest {
  private final ByteBuffer[] payloadChunks;
  private final ChunkedValueManifest chunkedValueManifest;

  public ChunkedPayloadAndManifest(ByteBuffer[] payloadChunks, ChunkedValueManifest chunkedValueManifest) {
    this.payloadChunks = payloadChunks;
    this.chunkedValueManifest = chunkedValueManifest;
  }

  public ByteBuffer[] getPayloadChunks() {
    return payloadChunks;
  }

  public ChunkedValueManifest getChunkedValueManifest() {
    return chunkedValueManifest;
  }
}
