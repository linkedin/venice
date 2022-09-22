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

import static org.testng.AssertJUnit.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DuplicatedByteBufTest extends AbstractByteBufTest {
  ByteBufAllocator _safeAlloc;

  @BeforeClass(groups = "unit")
  public void beforeClass() {
    _safeAlloc = new SafeAllocator(PooledByteBufAllocator.DEFAULT);
  }

  @Override
  protected ByteBuf newBuffer(int length, int maxCapacity) {
    ByteBuf wrapped = _safeAlloc.heapBuffer(length, maxCapacity);
    ByteBuf buffer = wrapped.duplicate();
    assertEquals(wrapped.writerIndex(), buffer.writerIndex());
    assertEquals(wrapped.readerIndex(), buffer.readerIndex());
    return buffer;
  }

  // See https://github.com/netty/netty/issues/1800
  @Test(groups = "unit")
  public void testIncreaseCapacityWrapped() {
    ByteBuf buffer = newBuffer(8);
    ByteBuf wrapped = buffer.unwrap();
    wrapped.writeByte(0);
    wrapped.readerIndex(wrapped.readerIndex() + 1);
    buffer.writerIndex(buffer.writerIndex() + 1);
    wrapped.capacity(wrapped.capacity() * 2);

    assertEquals((byte) 0, buffer.readByte());
  }

  @Test(groups = "unit")
  public void testMarksInitialized() {
    ByteBuf wrapped = _safeAlloc.heapBuffer(8);
    try {
      wrapped.writerIndex(6);
      wrapped.readerIndex(1);
      ByteBuf duplicate = wrapped.duplicate();

      // Test writer mark
      duplicate.writerIndex(duplicate.writerIndex() + 1);
      duplicate.resetWriterIndex();
      assertEquals(wrapped.writerIndex(), duplicate.writerIndex());

      // Test reader mark
      duplicate.readerIndex(duplicate.readerIndex() + 1);
      duplicate.resetReaderIndex();
      assertEquals(wrapped.readerIndex(), duplicate.readerIndex());
    } finally {
      wrapped.release();
    }
  }

  @Test(groups = "unit", enabled = false)
  public void testSliceCapacityChange() {
  }

  @Test(groups = "unit", enabled = false)
  public void testRetainedSliceCapacityChange() {
  }

  @Test(groups = "unit", enabled = false)
  public void testDiscardReadBytes() {
  }

  @Test(groups = "unit", enabled = false)
  public void testDiscardReadBytes2() {
  }
}
