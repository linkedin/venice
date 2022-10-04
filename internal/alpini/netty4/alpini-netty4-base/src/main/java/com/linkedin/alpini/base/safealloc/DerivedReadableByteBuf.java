package com.linkedin.alpini.base.safealloc;

import io.netty.buffer.AbstractByteBuf;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ByteProcessor;
import io.netty.util.IllegalReferenceCountException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ReadOnlyBufferException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;
import java.util.Optional;


/**
 * Abstract base {@linkplain ByteBuf} used to point to either a {@linkplain SafeByteBuf} or another instance of
 * {@linkplain DerivedReadableByteBuf}.
 */
abstract class DerivedReadableByteBuf extends AbstractByteBuf {
  private ByteBuf _buf;
  private int _offset;
  private int _length;

  protected DerivedReadableByteBuf(ByteBuf buf) {
    this(buf, 0, -1);
  }

  protected DerivedReadableByteBuf(ByteBuf buf, int offset, int length) {
    super(length >= 0 ? Math.min(buf.maxCapacity() - offset, length) : buf.maxCapacity() - offset);
    _buf = buf;
    _offset = offset;
    _length = length;
  }

  protected final int start(int index, int length) {
    if (index < 0 || length < 0) {
      throw new ArrayIndexOutOfBoundsException();
    } else {
      int capacity = capacity();
      if (Math.addExact(index, length) > capacity) {
        throw new ArrayIndexOutOfBoundsException("index=" + index + ", length=" + length + ", capacity=" + capacity);
      }
    }
    return Math.addExact(index, _offset);
  }

  private ByteBuf checkReaderIndex() {
    if (readerIndex() > writerIndex()) {
      throw new IndexOutOfBoundsException();
    }
    return this;
  }

  @Override
  public ByteBuf resetReaderIndex() {
    super.resetReaderIndex();
    return checkReaderIndex();
  }

  @Override
  public ByteBuf resetWriterIndex() {
    super.resetWriterIndex();
    return checkReaderIndex();
  }

  @Override
  protected final byte _getByte(int index) {
    return unwrap0().getByte(start(index, 1));
  }

  @Override
  protected final short _getShort(int index) {
    return unwrap0().getShort(start(index, 2));
  }

  @Override
  protected final short _getShortLE(int index) {
    return unwrap0().getShortLE(start(index, 2));
  }

  @Override
  protected final int _getUnsignedMedium(int index) {
    return unwrap0().getUnsignedMedium(start(index, 3));
  }

  @Override
  protected final int _getUnsignedMediumLE(int index) {
    return unwrap0().getUnsignedMediumLE(start(index, 3));
  }

  @Override
  protected final int _getInt(int index) {
    return unwrap0().getInt(start(index, 4));
  }

  @Override
  protected final int _getIntLE(int index) {
    return unwrap0().getIntLE(start(index, 4));
  }

  @Override
  protected final long _getLong(int index) {
    return unwrap0().getLong(start(index, 8));
  }

  @Override
  protected final long _getLongLE(int index) {
    return unwrap0().getLongLE(start(index, 8));
  }

  final <T> T readOnly() {
    throw new ReadOnlyBufferException();
  }

  @Override
  protected void _setByte(int index, int value) {
    readOnly();
  }

  @Override
  protected void _setShort(int index, int value) {
    readOnly();
  }

  @Override
  protected void _setShortLE(int index, int value) {
    readOnly();
  }

  @Override
  protected void _setMedium(int index, int value) {
    readOnly();
  }

  @Override
  protected void _setMediumLE(int index, int value) {
    readOnly();
  }

  @Override
  protected void _setInt(int index, int value) {
    readOnly();
  }

  @Override
  protected void _setIntLE(int index, int value) {
    readOnly();
  }

  @Override
  protected void _setLong(int index, long value) {
    readOnly();
  }

  @Override
  protected void _setLongLE(int index, long value) {
    readOnly();
  }

  @Override
  public final ByteBuf duplicate() {
    return duplicate0().setIndex(readerIndex(), writerIndex()).markReaderIndex().markWriterIndex();
  }

  protected abstract ByteBuf duplicate0();

  @Override
  public final ByteBuf slice(int index, int length) {
    if (isReadOnly() && length == 0) {
      ensureAccessible();
      return Unpooled.EMPTY_BUFFER.slice(index, length);
    }
    return slice0(index, length);
  }

  protected abstract ByteBuf slice0(int index, int length);

  @Override
  public final String toString(int index, int length, Charset charset) {
    return unwrap0().toString(start(index, length), length, charset);
  }

  @Override
  public final int indexOf(int fromIndex, int toIndex, byte value) {
    return Math.max(unwrap0().indexOf(start(fromIndex, 0), start(toIndex, 0), value) - _offset, -1);
  }

  @Override
  public int bytesBefore(byte value) {
    return bytesBefore(readableBytes(), value);
  }

  @Override
  public int bytesBefore(int length, byte value) {
    return bytesBefore(readerIndex(), length, value);
  }

  @Override
  public int bytesBefore(int index, int length, byte value) {
    return Math.max(unwrap0().bytesBefore(start(index, length), length, value) - _offset, -1);
  }

  @Override
  public int forEachByte(ByteProcessor processor) {
    return forEachByte(readerIndex(), readableBytes(), processor);
  }

  @Override
  public int forEachByte(int index, int length, ByteProcessor processor) {
    return Math.max(unwrap0().forEachByte(start(index, length), length, processor) - _offset, -1);
  }

  @Override
  public int forEachByteDesc(ByteProcessor processor) {
    return forEachByteDesc(readerIndex(), readableBytes(), processor);
  }

  @Override
  public int forEachByteDesc(int index, int length, ByteProcessor processor) {
    return Math.max(unwrap0().forEachByteDesc(start(index, length), length, processor) - _offset, -1);
  }

  @Override
  public int capacity() {
    if (_length >= 0) {
      return _length;
    } else {
      return Math.max(unwrap0().capacity() - _offset, 0);
    }
  }

  @Override
  public ByteBuf capacity(int newCapacity) {
    if (newCapacity > maxCapacity()) {
      String message = "cannot increase capacity from " + capacity() + " to " + newCapacity + " (maxCapacity="
          + maxCapacity() + ", length=" + _length + ", unwrap.capacity=" + unwrap0().capacity() + ")";
      if (_length == -1) {
        throw new IllegalArgumentException(message);
      } else {
        throw new UnsupportedOperationException(message);
      }
    } else if (Math.addExact(newCapacity, _offset) > unwrap0().capacity()) {
      unwrap0().capacity(Math.addExact(newCapacity, _offset));
    } else if (newCapacity < 0) {
      throw new IllegalArgumentException();
    } else if (newCapacity < _length || _length == -1) {
      _length = newCapacity;
    }
    return this;
  }

  @Override
  public final ByteBufAllocator alloc() {
    return unwrap0().alloc();
  }

  @Override
  public final ByteOrder order() {
    return unwrap0().order();
  }

  @Override
  public ByteBuf unwrap() {
    return _offset == 0 ? unwrap0() : unwrap0().slice(_offset, maxCapacity()).setIndex(0, 0);
  }

  protected ByteBuf unwrap0() {
    if (_buf != null && _buf.refCnt() >= 1) {
      return _buf;
    } else {
      throw new IllegalReferenceCountException();
    }
  }

  @Override
  public final boolean isDirect() {
    return unwrap0().isDirect();
  }

  @Override
  public final ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
    unwrap0().getBytes(start(index, length), dst, dstIndex, length);
    return this;
  }

  @Override
  public final ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
    unwrap0().getBytes(start(index, length), dst, dstIndex, length);
    return this;
  }

  @Override
  public final ByteBuf getBytes(int index, ByteBuffer dst) {
    unwrap0().getBytes(start(index, dst.remaining()), dst);
    return this;
  }

  @Override
  public final ByteBuf getBytes(int index, OutputStream out, int length) throws IOException {
    unwrap0().getBytes(start(index, length), out, length);
    return this;
  }

  @Override
  public final int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
    return unwrap0().getBytes(start(index, length), out, length);
  }

  @Override
  public final int getBytes(int index, FileChannel out, long position, int length) throws IOException {
    return unwrap0().getBytes(start(index, length), out, position, length);
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
    return readOnly();
  }

  @Override
  public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
    return readOnly();
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuffer src) {
    return readOnly();
  }

  @Override
  public int setBytes(int index, InputStream in, int length) throws IOException {
    return readOnly();
  }

  @Override
  public int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
    return readOnly();
  }

  @Override
  public int setBytes(int index, FileChannel in, long position, int length) throws IOException {
    return readOnly();
  }

  @Override
  public ByteBuf copy(int index, int length) {
    if (length == 0) {
      return Unpooled.EMPTY_BUFFER;
    }
    ByteBuf c = new DerivedReadOnlyByteBuf(unwrap0().copy(start(index, length), length));
    c.writerIndex(length);
    return c;
  }

  @Override
  public final int nioBufferCount() {
    return unwrap0().nioBufferCount();
  }

  @Override
  public final ByteBuffer nioBuffer(int index, int length) {
    return unwrap0().nioBuffer(start(index, length), length);
  }

  @Override
  public final ByteBuffer internalNioBuffer(int index, int length) {
    return unwrap0().internalNioBuffer(start(index, length), length);
  }

  @Override
  public final ByteBuffer[] nioBuffers(int index, int length) {
    return unwrap0().nioBuffers(start(index, length), length);
  }

  @Override
  public final boolean hasArray() {
    return Optional.ofNullable(_buf).map(ByteBuf::hasArray).orElse(false);
  }

  @Override
  public final byte[] array() {
    return unwrap0().array();
  }

  @Override
  public final int arrayOffset() {
    return unwrap0().arrayOffset() + start(0, capacity());
  }

  @Override
  public boolean hasMemoryAddress() {
    return Optional.ofNullable(_buf).map(ByteBuf::hasMemoryAddress).orElse(false);
  }

  @Override
  public long memoryAddress() {
    return unwrap0().memoryAddress() + start(0, capacity());
  }

  @Override
  public ByteBuf retain(int increment) {
    unwrap0().retain(increment);
    return this;
  }

  @Override
  public int refCnt() {
    return Optional.ofNullable(_buf).map(ByteBuf::refCnt).orElse(0);
  }

  @Override
  public ByteBuf retain() {
    unwrap0().retain();
    return this;
  }

  @Override
  public ByteBuf touch() {
    unwrap0().touch();
    return this;
  }

  @Override
  public ByteBuf touch(Object hint) {
    unwrap0().touch(hint);
    return this;
  }

  @Override
  public boolean release() {
    return unwrap0().release();
  }

  @Override
  public boolean release(int decrement) {
    return unwrap0().release(decrement);
  }

  @Override
  public ByteBuf discardReadBytes() {
    ensureAccessible();
    return this;
  }

  @Override
  public ByteBuf discardSomeReadBytes() {
    ensureAccessible();
    return this;
  }
}
