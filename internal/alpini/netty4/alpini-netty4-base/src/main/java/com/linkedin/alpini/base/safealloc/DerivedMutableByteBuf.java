package com.linkedin.alpini.base.safealloc;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ScatteringByteChannel;


/**
 * Mutable {@linkplain ByteBuf}.
 */
public final class DerivedMutableByteBuf extends DerivedReadableByteBuf {
  public DerivedMutableByteBuf(ByteBuf buf) {
    this(buf, 0, -1);
    super.setIndex(buf.readerIndex(), buf.writerIndex());
    super.markReaderIndex();
    super.markWriterIndex();
  }

  public DerivedMutableByteBuf(ByteBuf buf, int offset, int length) {
    super(buf, offset, length);
  }

  @Override
  public final boolean isReadOnly() {
    return unwrap0().isReadOnly();
  }

  @Override
  public final ByteBuf asReadOnly() {
    if (isReadOnly()) {
      return this;
    }
    return new DerivedReadOnlyByteBuf(unwrap0(), start(0, capacity()), -1).setIndex(readerIndex(), writerIndex())
        .markReaderIndex()
        .markWriterIndex()
        .asReadOnly();
  }

  @Override
  protected final void _setByte(int index, int value) {
    unwrap0().setByte(start(index, 1), value);
  }

  @Override
  protected final void _setShort(int index, int value) {
    unwrap0().setShort(start(index, 2), value);
  }

  @Override
  protected final void _setShortLE(int index, int value) {
    unwrap0().setShortLE(start(index, 2), value);
  }

  @Override
  protected final void _setMedium(int index, int value) {
    unwrap0().setMedium(start(index, 3), value);
  }

  @Override
  protected final void _setMediumLE(int index, int value) {
    unwrap0().setMediumLE(start(index, 3), value);
  }

  @Override
  protected final void _setInt(int index, int value) {
    unwrap0().setInt(start(index, 4), value);
  }

  @Override
  protected final void _setIntLE(int index, int value) {
    unwrap0().setIntLE(start(index, 4), value);
  }

  @Override
  protected final void _setLong(int index, long value) {
    unwrap0().setLong(start(index, 8), value);
  }

  @Override
  protected final void _setLongLE(int index, long value) {
    unwrap0().setLongLE(start(index, 8), value);
  }

  protected ByteBuf duplicate0() {
    return new DerivedMutableByteBuf(unwrap0(), start(0, capacity()), -1);
  }

  @Override
  protected ByteBuf slice0(int index, int length) {
    return new DerivedMutableByteBuf(unwrap0(), start(index, length), length).markReaderIndex()
        .markWriterIndex()
        .setIndex(0, length);
  }

  @Override
  public final ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
    unwrap0().setBytes(start(index, length), src, srcIndex, length);
    return this;
  }

  @Override
  public final ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
    unwrap0().setBytes(start(index, length), src, srcIndex, length);
    return this;
  }

  @Override
  public final ByteBuf setBytes(int index, ByteBuffer src) {
    unwrap0().setBytes(start(index, src.remaining()), src);
    return this;
  }

  @Override
  public final int setBytes(int index, InputStream in, int length) throws IOException {
    return unwrap0().setBytes(start(index, length), in, length);
  }

  @Override
  public final int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
    return unwrap0().setBytes(start(index, length), in, length);
  }

  @Override
  public final int setBytes(int index, FileChannel in, long position, int length) throws IOException {
    return unwrap0().setBytes(start(index, length), in, position, length);
  }

  @Override
  public ByteBuf copy(int index, int length) {
    return new DerivedMutableByteBuf(unwrap0().copy(start(index, length), length));
  }
}
