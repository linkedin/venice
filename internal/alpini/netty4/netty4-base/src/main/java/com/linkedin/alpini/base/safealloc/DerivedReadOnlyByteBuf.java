package com.linkedin.alpini.base.safealloc;

import io.netty.buffer.ByteBuf;


/**
 * Immutable {@linkplain ByteBuf}.
 */
public final class DerivedReadOnlyByteBuf extends DerivedReadableByteBuf {
  public DerivedReadOnlyByteBuf(ByteBuf buf) {
    super(buf);
  }

  public DerivedReadOnlyByteBuf(ByteBuf buf, int offset, int length) {
    super(buf, offset, length);
  }

  @Override
  public boolean isReadOnly() {
    return true;
  }

  @Override
  public ByteBuf asReadOnly() {
    return this;
  }

  @Override
  public boolean isWritable() {
    return false;
  }

  @Override
  public boolean isWritable(int numBytes) {
    return false;
  }

  @Override
  public int writableBytes() {
    return 0;
  }

  @Override
  public int maxWritableBytes() {
    return 0;
  }

  @Override
  public ByteBuf ensureWritable(int minWritableBytes) {
    if (minWritableBytes > 0) {
      return readOnly();
    }
    return this;
  }

  @Override
  public int ensureWritable(int minWritableBytes, boolean force) {
    return 1;
  }

  @Override
  protected ByteBuf duplicate0() {
    return new DerivedReadOnlyByteBuf(unwrap0(), start(0, capacity()), -1);
  }

  @Override
  protected ByteBuf slice0(int index, int length) {
    return new DerivedReadOnlyByteBuf(unwrap0(), start(index, length), length).setIndex(0, Math.min(capacity(), length))
        .markReaderIndex()
        .markWriterIndex();
  }

  @Override
  public boolean hasMemoryAddress() {
    return false;
  }

  @Override
  public long memoryAddress() {
    throw new UnsupportedOperationException();
  }
}
