package com.linkedin.alpini.base.safealloc;

import io.netty.buffer.AbstractReferenceCountedByteBuf;
import io.netty.buffer.ByteBuf;
import io.netty.util.ByteProcessor;
import io.netty.util.IllegalReferenceCountException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.ReferenceQueue;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.util.Optional;


/**
 * A {@linkplain AbstractReferenceCountedByteBuf} which maintains the reference count
 * for the memory object so that we can {@link #deallocate()} it later when the
 * reference count drops to zero.
 */
final class SafeByteBuf extends AbstractReferenceCountedByteBuf {
  private final SafeAllocator _alloc;
  private SafeReference _ref;
  private boolean _hasArray;
  private boolean _hasMemoryAddress;

  SafeByteBuf(SafeAllocator alloc, ReferenceQueue<SafeByteBuf> queue, ByteBuf buf) {
    super(buf.maxCapacity());
    _alloc = alloc;
    _ref = alloc.makeReference(this, queue, buf);
    _hasArray = buf.hasArray();
    _hasMemoryAddress = buf.hasMemoryAddress();
  }

  SafeByteBuf(SafeByteBuf source) {
    super(source._ref.store().maxCapacity());
    _alloc = source._alloc;
    _ref = source._ref;
    _hasArray = source._hasArray;
    _hasMemoryAddress = source._hasMemoryAddress;
    super.setIndex(source.readerIndex(), source.writerIndex());
  }

  private SafeByteBuf(SafeAllocator alloc, ByteBuf copy) {
    this(alloc, alloc.referenceQueue(), copy);
  }

  @Override
  public ByteBuf retain() {
    touch();
    return super.retain();
  }

  @Override
  public ByteBuf retain(int increment) {
    touch();
    return super.retain(increment);
  }

  private SafeReference ref() {
    return Optional.ofNullable(_ref).orElseThrow(IllegalReferenceCountException::new);
  }

  @Override
  public ByteBuf touch() {
    ref().touch();
    return this;
  }

  @Override
  public ByteBuf touch(Object hint) {
    ref().touch(hint);
    return this;
  }

  @Override
  public boolean release() {
    touch();
    return super.release();
  }

  @Override
  public boolean release(int decrement) {
    touch();
    return super.release(decrement);
  }

  @Override
  protected void deallocate() {
    SafeReference ref = ref();
    ref.clear();
    unwrap0().release();
    alloc()._active.remove(ref);
    _ref = null;
  }

  private ByteBuf unwrap0() {
    return ref().store();
  }

  private ByteBuf unwrap0(ByteBuf store) {
    _hasArray = store.hasArray();
    _hasMemoryAddress = store.hasMemoryAddress();
    return ref().store(store);
  }

  @Override
  public ByteBuf readerIndex(int readerIndex) {
    try {
      return super.readerIndex(readerIndex);
    } finally {
      unwrap0().setIndex(readerIndex(), writerIndex());
    }
  }

  @Override
  public ByteBuf writerIndex(int writerIndex) {
    try {
      return super.writerIndex(writerIndex);
    } finally {
      unwrap0().setIndex(readerIndex(), writerIndex());
    }
  }

  @Override
  public ByteBuf setIndex(int readerIndex, int writerIndex) {
    unwrap0().setIndex(readerIndex, writerIndex);
    return super.setIndex(readerIndex, writerIndex);
  }

  @Override
  public ByteBuf discardReadBytes() {
    int readerIndex = readerIndex();
    ByteBuf buf = unwrap0().setIndex(readerIndex, writerIndex());
    buf = unwrap0(buf.discardReadBytes());
    super.setIndex(buf.readerIndex(), buf.writerIndex());
    adjustMarkers(readerIndex - readerIndex());
    return this;
  }

  @Override
  public ByteBuf discardSomeReadBytes() {
    int readerIndex = readerIndex();
    ByteBuf buf = unwrap0().setIndex(readerIndex, writerIndex());
    buf = unwrap0(buf.discardSomeReadBytes());
    super.setIndex(buf.readerIndex(), buf.writerIndex());
    adjustMarkers(readerIndex - readerIndex());
    return this;
  }

  @Override
  public ByteBuf clear() {
    unwrap0().clear();
    return super.clear();
  }

  @Override
  protected byte _getByte(int index) {
    return unwrap0().getByte(index);
  }

  @Override
  protected short _getShort(int index) {
    return unwrap0().getShort(index);
  }

  @Override
  protected short _getShortLE(int index) {
    return unwrap0().getShortLE(index);
  }

  @Override
  protected int _getUnsignedMedium(int index) {
    return unwrap0().getUnsignedMedium(index);
  }

  @Override
  protected int _getUnsignedMediumLE(int index) {
    return unwrap0().getUnsignedMediumLE(index);
  }

  @Override
  protected int _getInt(int index) {
    return unwrap0().getInt(index);
  }

  @Override
  protected int _getIntLE(int index) {
    return unwrap0().getIntLE(index);
  }

  @Override
  protected long _getLong(int index) {
    return unwrap0().getLong(index);
  }

  @Override
  protected long _getLongLE(int index) {
    return unwrap0().getLongLE(index);
  }

  @Override
  protected void _setByte(int index, int value) {
    unwrap0().setByte(index, value);
  }

  @Override
  protected void _setShort(int index, int value) {
    unwrap0().setShort(index, value);
  }

  @Override
  protected void _setShortLE(int index, int value) {
    unwrap0().setShortLE(index, value);
  }

  @Override
  protected void _setMedium(int index, int value) {
    unwrap0().setMedium(index, value);
  }

  @Override
  protected void _setMediumLE(int index, int value) {
    unwrap0().setMediumLE(index, value);
  }

  @Override
  protected void _setInt(int index, int value) {
    unwrap0().setInt(index, value);
  }

  @Override
  protected void _setIntLE(int index, int value) {
    unwrap0().setIntLE(index, value);
  }

  @Override
  protected void _setLong(int index, long value) {
    unwrap0().setLong(index, value);
  }

  @Override
  protected void _setLongLE(int index, long value) {
    unwrap0().setLongLE(index, value);
  }

  @Override
  public ByteBuf duplicate() {
    ByteBuf dup = new DerivedMutableByteBuf(this, 0, -1);
    dup.setIndex(readerIndex(), writerIndex());
    dup.markReaderIndex();
    dup.markWriterIndex();
    return dup;
  }

  @Override
  public int capacity() {
    return unwrap0().capacity();
  }

  @Override
  public SafeByteBuf capacity(int newCapacity) {
    unwrap0(unwrap0().capacity(newCapacity));
    return this;
  }

  @Override
  public SafeAllocator alloc() {
    return _alloc;
  }

  @Override
  public ByteOrder order() {
    return unwrap0().order();
  }

  @Override
  public ByteBuf unwrap() {
    return unwrap0().unwrap();
  }

  @Override
  public boolean isDirect() {
    return unwrap0().isDirect();
  }

  @Override
  public SafeByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
    unwrap0(unwrap0().getBytes(index, dst, dstIndex, length));
    return this;
  }

  @Override
  public SafeByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
    unwrap0(unwrap0().getBytes(index, dst, dstIndex, length));
    return this;
  }

  @Override
  public SafeByteBuf getBytes(int index, ByteBuffer dst) {
    unwrap0(unwrap0().getBytes(index, dst));
    return this;
  }

  @Override
  public SafeByteBuf getBytes(int index, OutputStream out, int length) throws IOException {
    unwrap0(unwrap0().getBytes(index, out, length));
    return this;
  }

  @Override
  public int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
    return unwrap0().getBytes(index, out, length);
  }

  @Override
  public int getBytes(int index, FileChannel out, long position, int length) throws IOException {
    return unwrap0().getBytes(index, out, position, length);
  }

  @Override
  public SafeByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
    unwrap0(unwrap0().setBytes(index, src, srcIndex, length));
    return this;
  }

  @Override
  public SafeByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
    unwrap0(unwrap0().setBytes(index, src, srcIndex, length));
    return this;
  }

  @Override
  public SafeByteBuf setBytes(int index, ByteBuffer src) {
    unwrap0(unwrap0().setBytes(index, src));
    return this;
  }

  @Override
  public int setBytes(int index, InputStream in, int length) throws IOException {
    return unwrap0().setBytes(index, in, length);
  }

  @Override
  public int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
    return unwrap0().setBytes(index, in, length);
  }

  @Override
  public int setBytes(int index, FileChannel in, long position, int length) throws IOException {
    return unwrap0().setBytes(index, in, position, length);
  }

  @Override
  public ByteBuf copy(int index, int length) {
    ByteBuf dup = new SafeByteBuf(alloc(), unwrap0().copy(index, length));
    dup.setIndex(0, length);
    dup.markReaderIndex();
    dup.markWriterIndex();
    return dup;
  }

  @Override
  public int nioBufferCount() {
    return unwrap0().nioBufferCount();
  }

  @Override
  public ByteBuffer nioBuffer(int index, int length) {
    return unwrap0().nioBuffer(index, length);
  }

  @Override
  public ByteBuffer internalNioBuffer(int index, int length) {
    return unwrap0().internalNioBuffer(index, length);
  }

  @Override
  public ByteBuffer[] nioBuffers(int index, int length) {
    return unwrap0().nioBuffers(index, length);
  }

  @Override
  public boolean hasArray() {
    return _hasArray;
  }

  @Override
  public byte[] array() {
    return unwrap0().array();
  }

  @Override
  public int arrayOffset() {
    return unwrap0().arrayOffset();
  }

  @Override
  public boolean hasMemoryAddress() {
    return _hasMemoryAddress;
  }

  @Override
  public long memoryAddress() {
    return unwrap0().memoryAddress();
  }

  @Override
  public int forEachByte(int index, int length, ByteProcessor processor) {
    return unwrap0().forEachByte(index, length, processor);
  }

  @Override
  public int forEachByteDesc(int index, int length, ByteProcessor processor) {
    return unwrap0().forEachByteDesc(index, length, processor);
  }
}
