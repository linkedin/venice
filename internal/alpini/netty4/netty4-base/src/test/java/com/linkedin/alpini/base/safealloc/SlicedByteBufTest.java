/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.linkedin.alpini.base.safealloc;

import static org.testng.AssertJUnit.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.PlatformDependent;
import java.nio.ByteBuffer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class SlicedByteBufTest extends AbstractByteBufTest {
  ByteBufAllocator _safeAlloc;

  @BeforeClass(groups = "unit")
  public void beforeClass() {
    _safeAlloc = new SafeAllocator(PooledByteBufAllocator.DEFAULT);
  }

  @Override
  protected final ByteBuf newBuffer(int length, int maxCapacity) {
    assumeTrue(maxCapacity == Integer.MAX_VALUE);
    int offset = length == 0 ? 0 : PlatformDependent.threadLocalRandom().nextInt(length);
    ByteBuf buffer = _safeAlloc.buffer(length * 2);
    ByteBuf slice = newSlice(buffer, offset, length);
    assertEquals(0, slice.readerIndex());
    assertEquals(length, slice.writerIndex());
    return slice;
  }

  protected ByteBuf newSlice(ByteBuf buffer, int offset, int length) {
    return buffer.slice(offset, length);
  }

  @Test(groups = "unit", expectedExceptions = NullPointerException.class)
  public void shouldNotAllowNullInConstructor() {
    new SafeByteBuf(null);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  @Override
  public void testInternalNioBuffer() {
    super.testInternalNioBuffer();
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  @Override
  public void testDuplicateReadGatheringByteChannelMultipleThreads() throws Exception {
    super.testDuplicateReadGatheringByteChannelMultipleThreads();
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  @Override
  public void testSliceReadGatheringByteChannelMultipleThreads() throws Exception {
    super.testSliceReadGatheringByteChannelMultipleThreads();
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  @Override
  public void testDuplicateReadOutputStreamMultipleThreads() throws Exception {
    super.testDuplicateReadOutputStreamMultipleThreads();
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  @Override
  public void testSliceReadOutputStreamMultipleThreads() throws Exception {
    super.testSliceReadOutputStreamMultipleThreads();
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  @Override
  public void testDuplicateBytesInArrayMultipleThreads() throws Exception {
    super.testDuplicateBytesInArrayMultipleThreads();
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  @Override
  public void testSliceBytesInArrayMultipleThreads() throws Exception {
    super.testSliceBytesInArrayMultipleThreads();
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  @Override
  public void testNioBufferExposeOnlyRegion() {
    super.testNioBufferExposeOnlyRegion();
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  @Override
  public void testGetReadOnlyDirectDst() {
    super.testGetReadOnlyDirectDst();
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  @Override
  public void testGetReadOnlyHeapDst() {
    super.testGetReadOnlyHeapDst();
  }

  @Test(groups = "unit", enabled = false)
  @Override
  public void testLittleEndianWithExpand() {
    // ignore for SlicedByteBuf
  }

  @Test(groups = "unit", enabled = false)
  @Override
  public void testReadBytes() {
    // ignore for SlicedByteBuf
  }

  @Test(groups = "unit", enabled = false)
  @Override
  public void testForEachByteDesc2() {
    // Ignore for SlicedByteBuf
  }

  @Test(groups = "unit", enabled = false)
  @Override
  public void testForEachByte2() {
    // Ignore for SlicedByteBuf
  }

  @Test(groups = "unit", enabled = false)
  @Override
  public void testDiscardReadBytes() {
  }

  @Test(groups = "unit", enabled = false)
  @Override
  public void testDiscardReadBytes2() {
  }

  @Test(groups = "unit", enabled = false)
  @Override
  public void testDuplicateCapacityChange() {
  }

  @Test(groups = "unit", enabled = false)
  @Override
  public void testRetainedDuplicateCapacityChange() {
  }

  @Test(groups = "unit")
  public void testReaderIndexAndMarks() {
    ByteBuf wrapped = _safeAlloc.buffer(16);
    try {
      wrapped.writerIndex(14);
      wrapped.readerIndex(2);
      wrapped.markWriterIndex();
      wrapped.markReaderIndex();
      ByteBuf slice = wrapped.slice(4, 4);
      assertEquals(0, slice.readerIndex());
      assertEquals(4, slice.writerIndex());

      slice.readerIndex(slice.readerIndex() + 1);
      slice.resetReaderIndex();
      assertEquals(0, slice.readerIndex());

      slice.writerIndex(slice.writerIndex() - 1);
      slice.resetWriterIndex();
      assertEquals(0, slice.writerIndex());
    } finally {
      wrapped.release();
    }
  }

  @Test(groups = "unit")
  public void sliceEmptyNotLeak() {
    ByteBuf buffer = _safeAlloc.buffer(8).retain();
    assertEquals(2, buffer.refCnt());

    ByteBuf slice1 = buffer.slice();
    assertEquals(2, slice1.refCnt());

    ByteBuf slice2 = slice1.slice();
    assertEquals(2, slice2.refCnt());

    assertFalse(slice2.release());
    assertEquals(1, buffer.refCnt());
    assertEquals(1, slice1.refCnt());
    assertEquals(1, slice2.refCnt());

    assertTrue(slice2.release());

    assertEquals(0, buffer.refCnt());
    assertEquals(0, slice1.refCnt());
    assertEquals(0, slice2.refCnt());
  }

  @Override
  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void testGetBytesByteBuffer() {
    byte[] bytes = { 'a', 'b', 'c', 'd', 'e', 'f', 'g' };
    // Ensure destination buffer is bigger then what is wrapped in the ByteBuf.
    ByteBuffer nioBuffer = ByteBuffer.allocate(bytes.length + 1);
    ByteBuf wrappedBuffer = Unpooled.wrappedBuffer(bytes).slice(0, bytes.length - 1);
    try {
      wrappedBuffer.getBytes(wrappedBuffer.readerIndex(), nioBuffer);
    } finally {
      wrappedBuffer.release();
    }
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  @Override
  public void testWriteUsAsciiCharSequenceExpand() {
    super.testWriteUsAsciiCharSequenceExpand();
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  @Override
  public void testWriteUtf8CharSequenceExpand() {
    super.testWriteUtf8CharSequenceExpand();
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  @Override
  public void testWriteIso88591CharSequenceExpand() {
    super.testWriteIso88591CharSequenceExpand();
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  @Override
  public void testWriteUtf16CharSequenceExpand() {
    super.testWriteUtf16CharSequenceExpand();
  }

  @Test(groups = "unit")
  public void ensureWritableWithEnoughSpaceShouldNotThrow() {
    ByteBuf slice = newBuffer(10);
    ByteBuf unwrapped = slice.unwrap();
    unwrapped.writerIndex(unwrapped.writerIndex() + 5);
    slice.writerIndex(slice.readerIndex());

    // Run ensureWritable and verify this doesn't change any indexes.
    int originalWriterIndex = slice.writerIndex();
    int originalReadableBytes = slice.readableBytes();
    slice.ensureWritable(originalWriterIndex - slice.writerIndex());
    assertEquals(originalWriterIndex, slice.writerIndex());
    assertEquals(originalReadableBytes, slice.readableBytes());
    slice.release();
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void ensureWritableWithNotEnoughSpaceShouldThrow() {
    ByteBuf slice = newBuffer(10);
    ByteBuf unwrapped = slice.unwrap();
    unwrapped.writerIndex(unwrapped.writerIndex() + 5);
    try {
      slice.ensureWritable(1);
      fail();
    } finally {
      slice.release();
    }
  }

}
