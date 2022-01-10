package com.linkedin.venice.hadoop.input.kafka.chunk;

import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;

import org.apache.commons.lang.Validate;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;


/**
 * A POJO containing a byte array and a serialized {@link ChunkedKeySuffix} or an object of {@link ChunkedKeySuffix}
 */
public class RawKeyBytesAndChunkedKeySuffix {
  private final ByteBuffer rawKeyBytes;
  private final ByteBuffer chunkedKeySuffixBytes;

  public RawKeyBytesAndChunkedKeySuffix(@Nonnull ByteBuffer rawKeyBytes, @Nonnull ByteBuffer chunkedKeySuffixBytes) {
    Validate.notNull(rawKeyBytes);
    Validate.notNull(chunkedKeySuffixBytes);
    this.rawKeyBytes = rawKeyBytes;
    this.chunkedKeySuffixBytes = chunkedKeySuffixBytes;
  }

  public ByteBuffer getRawKeyBytes() {
    return rawKeyBytes;
  }

  public ByteBuffer getChunkedKeySuffixBytes() {
    return chunkedKeySuffixBytes;
  }
}
