/*
 * Copyright 2013 The Netty Project
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
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.PlatformDependent;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.nio.channels.FileChannel;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ReadOnlyDirectByteBufferBufTest {
  ByteBufAllocator _safeAlloc;

  @BeforeClass(groups = "unit")
  public void beforeClass() {
    _safeAlloc = new SafeAllocator(PooledByteBufAllocator.DEFAULT);
  }

  protected ByteBuf buffer(ByteBuffer buffer) {
    if (!buffer.isReadOnly()) {
      throw new IllegalArgumentException();
    }
    return (buffer.isDirect() ? _safeAlloc.directBuffer(buffer.remaining()) : _safeAlloc.heapBuffer(buffer.remaining()))
        .writeBytes(buffer)
        .asReadOnly();
  }

  protected ByteBuffer allocate(int size) {
    return ByteBuffer.allocateDirect(size);
  }

  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
  public void testConstructWithWritable() {
    buffer(allocate(1));
  }

  @Test(groups = "unit")
  public void shouldIndicateNotWritable() {
    ByteBuf buf = buffer(allocate(8).asReadOnlyBuffer()).clear();
    try {
      assertFalse(buf.isWritable());
    } finally {
      buf.release();
    }
  }

  @Test(groups = "unit")
  public void shouldIndicateNotWritableAnyNumber() {
    ByteBuf buf = buffer(allocate(8).asReadOnlyBuffer()).clear();
    try {
      assertFalse(buf.isWritable(1));
    } finally {
      buf.release();
    }
  }

  @Test(groups = "unit")
  public void ensureWritableIntStatusShouldFailButNotThrow() {
    ByteBuf buf = buffer(allocate(8).asReadOnlyBuffer()).clear();
    try {
      int result = buf.ensureWritable(1, false);
      assertEquals(1, result);
    } finally {
      buf.release();
    }
  }

  @Test(groups = "unit")
  public void ensureWritableForceIntStatusShouldFailButNotThrow() {
    ByteBuf buf = buffer(allocate(8).asReadOnlyBuffer()).clear();
    try {
      int result = buf.ensureWritable(1, true);
      assertEquals(1, result);
    } finally {
      buf.release();
    }
  }

  @Test(groups = "unit", expectedExceptions = ReadOnlyBufferException.class)
  public void ensureWritableShouldThrow() {
    ByteBuf buf = buffer(allocate(8).asReadOnlyBuffer()).clear();
    try {
      buf.ensureWritable(1);
    } finally {
      buf.release();
    }
  }

  @Test(groups = "unit", expectedExceptions = ReadOnlyBufferException.class)
  public void testSetByte() {
    ByteBuf buf = buffer(allocate(8).asReadOnlyBuffer());
    try {
      buf.setByte(0, 1);
    } finally {
      buf.release();
    }
  }

  @Test(groups = "unit", expectedExceptions = ReadOnlyBufferException.class)
  public void testSetInt() {
    ByteBuf buf = buffer(allocate(8).asReadOnlyBuffer());
    try {
      buf.setInt(0, 1);
    } finally {
      buf.release();
    }
  }

  @Test(groups = "unit", expectedExceptions = ReadOnlyBufferException.class)
  public void testSetShort() {
    ByteBuf buf = buffer(allocate(8).asReadOnlyBuffer());
    try {
      buf.setShort(0, 1);
    } finally {
      buf.release();
    }
  }

  @Test(groups = "unit", expectedExceptions = ReadOnlyBufferException.class)
  public void testSetMedium() {
    ByteBuf buf = buffer(allocate(8).asReadOnlyBuffer());
    try {
      buf.setMedium(0, 1);
    } finally {
      buf.release();
    }
  }

  @Test(groups = "unit", expectedExceptions = ReadOnlyBufferException.class)
  public void testSetLong() {
    ByteBuf buf = buffer(allocate(8).asReadOnlyBuffer());
    try {
      buf.setLong(0, 1);
    } finally {
      buf.release();
    }
  }

  @Test(groups = "unit", expectedExceptions = ReadOnlyBufferException.class)
  public void testSetBytesViaArray() {
    ByteBuf buf = buffer(allocate(8).asReadOnlyBuffer());
    try {
      buf.setBytes(0, "test".getBytes());
    } finally {
      buf.release();
    }
  }

  @Test(groups = "unit", expectedExceptions = ReadOnlyBufferException.class)
  public void testSetBytesViaBuffer() {
    ByteBuf buf = buffer(allocate(8).asReadOnlyBuffer());
    ByteBuf copy = Unpooled.copyInt(1);
    try {
      buf.setBytes(0, copy);
    } finally {
      buf.release();
      copy.release();
    }
  }

  @Test(groups = "unit", expectedExceptions = ReadOnlyBufferException.class)
  public void testSetBytesViaStream() throws IOException {
    ByteBuf buf = buffer(ByteBuffer.allocateDirect(8).asReadOnlyBuffer());
    try {
      buf.setBytes(0, new ByteArrayInputStream("test".getBytes()), 2);
    } finally {
      buf.release();
    }
  }

  @Test(groups = "unit")
  public void testGetReadByte() {
    ByteBuf buf = buffer(((ByteBuffer) allocate(2).put(new byte[] { (byte) 1, (byte) 2 }).flip()).asReadOnlyBuffer());

    assertEquals(1, buf.getByte(0));
    assertEquals(2, buf.getByte(1));

    assertEquals(1, buf.readByte());
    assertEquals(2, buf.readByte());
    assertFalse(buf.isReadable());

    buf.release();
  }

  @Test(groups = "unit")
  public void testGetReadInt() {
    ByteBuf buf = buffer(((ByteBuffer) allocate(8).putInt(1).putInt(2).flip()).asReadOnlyBuffer());

    assertEquals(1, buf.getInt(0));
    assertEquals(2, buf.getInt(4));

    assertEquals(1, buf.readInt());
    assertEquals(2, buf.readInt());
    assertFalse(buf.isReadable());

    buf.release();
  }

  @Test(groups = "unit")
  public void testGetReadShort() {
    ByteBuf buf = buffer(((ByteBuffer) allocate(8).putShort((short) 1).putShort((short) 2).flip()).asReadOnlyBuffer());

    assertEquals(1, buf.getShort(0));
    assertEquals(2, buf.getShort(2));

    assertEquals(1, buf.readShort());
    assertEquals(2, buf.readShort());
    assertFalse(buf.isReadable());

    buf.release();
  }

  @Test(groups = "unit")
  public void testGetReadLong() {
    ByteBuf buf = buffer(((ByteBuffer) allocate(16).putLong(1).putLong(2).flip()).asReadOnlyBuffer());

    assertEquals(1, buf.getLong(0));
    assertEquals(2, buf.getLong(8));

    assertEquals(1, buf.readLong());
    assertEquals(2, buf.readLong());
    assertFalse(buf.isReadable());

    buf.release();
  }

  @Test(groups = "unit")
  public void testCopy() {
    ByteBuf buf = buffer(((ByteBuffer) allocate(16).putLong(1).putLong(2).flip()).asReadOnlyBuffer());
    ByteBuf copy = buf.copy();

    assertEquals(buf, copy);

    buf.release();
    copy.release();
  }

  @Test(groups = "unit")
  public void testCopyWithOffset() {
    ByteBuf buf = buffer(((ByteBuffer) allocate(16).putLong(1).putLong(2).flip()).asReadOnlyBuffer());
    ByteBuf copy = buf.copy(1, 9);

    assertTrue(ByteBufUtil.equals(buf.slice(1, 9), copy));

    buf.release();
    copy.release();
  }

  // Test for https://github.com/netty/netty/issues/1708
  @Test(groups = "unit")
  public void testWrapBufferWithNonZeroPosition() {
    ByteBuf buf = buffer(((ByteBuffer) allocate(16).putLong(1).flip().position(1)).asReadOnlyBuffer());

    ByteBuf slice = buf.slice();
    assertEquals(buf, slice);

    buf.release();
  }

  @Test(groups = "unit")
  public void testWrapBufferRoundTrip() {
    ByteBuf buf = buffer(((ByteBuffer) allocate(16).putInt(1).putInt(2).flip()).asReadOnlyBuffer());

    assertEquals(1, buf.readInt());

    ByteBuffer nioBuffer = buf.nioBuffer();

    // Ensure this can be accessed without throwing a BufferUnderflowException
    assertEquals(2, nioBuffer.getInt());

    buf.release();
  }

  @Test(groups = "unit")
  public void testWrapMemoryMapped() throws Exception {
    File file = File.createTempFile("netty-test", "tmp");
    FileChannel output = null;
    FileChannel input = null;
    ByteBuf b1 = null;
    ByteBuf b2 = null;

    try {
      output = new FileOutputStream(file).getChannel();
      byte[] bytes = new byte[1024];
      PlatformDependent.threadLocalRandom().nextBytes(bytes);
      output.write(ByteBuffer.wrap(bytes));

      input = new FileInputStream(file).getChannel();
      ByteBuffer m = input.map(FileChannel.MapMode.READ_ONLY, 0, input.size());

      b1 = buffer(m);

      ByteBuffer dup = m.duplicate();
      dup.position(2);
      dup.limit(4);

      b2 = buffer(dup);

      assertEquals(b2, b1.slice(2, 2));
    } finally {
      if (b1 != null) {
        b1.release();
      }
      if (b2 != null) {
        b2.release();
      }
      if (output != null) {
        output.close();
      }
      if (input != null) {
        input.close();
      }
      file.delete();
    }
  }

  @Test(groups = "unit")
  public void testMemoryAddress() {
    ByteBuf buf = buffer(allocate(8).asReadOnlyBuffer());
    try {
      assertFalse(buf.hasMemoryAddress());
      try {
        buf.memoryAddress();
        fail();
      } catch (UnsupportedOperationException expected) {
        // expected
      }
    } finally {
      buf.release();
    }
  }
}
