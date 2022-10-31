package com.linkedin.venice.writer;

import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import java.nio.ByteBuffer;


public class ChunkedPayloadAndManifest {
  ByteBuffer[] payloadChunks;
  ChunkedValueManifest chunkedValueManifest;

  public ChunkedPayloadAndManifest(ChunkedValueManifest chunkedValueManifest, ByteBuffer[] payloadChunks) {
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
