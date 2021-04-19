package com.linkedin.venice.hadoop.input.kafka.chunk;

import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import com.linkedin.venice.utils.Utils;
import java.nio.ByteBuffer;


/**
 * A POJO containing a byte array and a serialized {@link ChunkedKeySuffix} or an object of {@link ChunkedKeySuffix}
 */
public class RawKeyBytesAndChunkedKeySuffix {
  private final ByteBuffer rawKeyBytes;
  private final ByteBuffer chunkedKeySuffixBytes;

  public RawKeyBytesAndChunkedKeySuffix(ByteBuffer rawKeyBytes, ByteBuffer chunkedKeySuffixBytes) {
    this.rawKeyBytes =  Utils.notNull(rawKeyBytes);
    this.chunkedKeySuffixBytes = Utils.notNull(chunkedKeySuffixBytes);
  }

  public ByteBuffer getRawKeyBytes() {
    return rawKeyBytes;
  }

  public ByteBuffer getChunkedKeySuffixBytes() {
    return chunkedKeySuffixBytes;
  }
}
