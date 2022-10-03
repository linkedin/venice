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

import static io.netty.buffer.Unpooled.LITTLE_ENDIAN;
import static io.netty.buffer.Unpooled.buffer;
import static io.netty.buffer.Unpooled.copiedBuffer;
import static io.netty.buffer.Unpooled.directBuffer;
import static io.netty.buffer.Unpooled.unreleasableBuffer;
import static io.netty.buffer.Unpooled.wrappedBuffer;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import com.linkedin.alpini.base.misc.Time;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.ByteProcessor;
import io.netty.util.CharsetUtil;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.internal.EmptyArrays;
import io.netty.util.internal.PlatformDependent;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ReadOnlyBufferException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public abstract class AbstractByteBufTest {
  private static final int CAPACITY = 4096; // Must be even
  private static final int BLOCK_SIZE = 128;
  private static final int JAVA_BYTEBUFFER_CONSISTENCY_ITERATIONS = 100;

  private long seed;
  private Random random;
  private ByteBuf buffer;

  protected final ByteBuf newBuffer(int capacity) {
    return newBuffer(capacity, Integer.MAX_VALUE);
  }

  protected abstract ByteBuf newBuffer(int capacity, int maxCapacity);

  protected boolean discardReadBytesDoesNotMoveWritableBytes() {
    return true;
  }

  @BeforeMethod(groups = "unit")
  public void init() {
    buffer = newBuffer(CAPACITY);
    seed = Time.currentTimeMillis();
    random = new Random(seed);
  }

  @AfterMethod(groups = "unit", alwaysRun = true)
  public void dispose() throws InterruptedException {
    if (buffer != null) {
      assertTrue(buffer.release());
      assertEquals(buffer.refCnt(), 0);

      try {
        buffer.release();
      } catch (Exception e) {
        // Ignore.
      }
      buffer = null;
    }
    System.gc();
    Time.sleep(20);
  }

  public static void assumeTrue(boolean test) {
    if (!test) {
      throw new SkipException("assumeTrue");
    }
  }

  @Test(groups = "unit")
  public void comparableInterfaceNotViolated() {
    assumeTrue(buffer.isReadOnly());
    buffer.writerIndex(buffer.readerIndex());
    assumeTrue(buffer.writableBytes() >= 4);

    buffer.writeLong(0);
    ByteBuf buffer2 = newBuffer(CAPACITY);
    assumeTrue(buffer2.isReadOnly());
    buffer2.writerIndex(buffer2.readerIndex());
    // Write an unsigned integer that will cause buffer.getUnsignedInt() - buffer2.getUnsignedInt() to underflow the
    // int type and wrap around on the negative side.
    buffer2.writeLong(0xF0000000L);
    assertTrue(buffer.compareTo(buffer2) < 0);
    assertTrue(buffer2.compareTo(buffer) > 0);
    buffer2.release();
  }

  @Test(groups = "unit")
  public void initialState() {
    assertEquals(CAPACITY, buffer.capacity());
    assertEquals(0, buffer.readerIndex());
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void readerIndexBoundaryCheck1() {
    try {
      buffer.writerIndex(0);
    } catch (IndexOutOfBoundsException e) {
      fail();
    }
    buffer.readerIndex(-1);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void readerIndexBoundaryCheck2() {
    try {
      buffer.writerIndex(buffer.capacity());
    } catch (IndexOutOfBoundsException e) {
      fail();
    }
    buffer.readerIndex(buffer.capacity() + 1);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void readerIndexBoundaryCheck3() {
    try {
      buffer.writerIndex(CAPACITY / 2);
    } catch (IndexOutOfBoundsException e) {
      fail();
    }
    buffer.readerIndex(CAPACITY * 3 / 2);
  }

  @Test(groups = "unit")
  public void readerIndexBoundaryCheck4() {
    buffer.writerIndex(0);
    buffer.readerIndex(0);
    buffer.writerIndex(buffer.capacity());
    buffer.readerIndex(buffer.capacity());
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void writerIndexBoundaryCheck1() {
    buffer.writerIndex(-1);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void writerIndexBoundaryCheck2() {
    try {
      buffer.writerIndex(CAPACITY);
      buffer.readerIndex(CAPACITY);
    } catch (IndexOutOfBoundsException e) {
      fail();
    }
    buffer.writerIndex(buffer.capacity() + 1);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void writerIndexBoundaryCheck3() {
    try {
      buffer.writerIndex(CAPACITY);
      buffer.readerIndex(CAPACITY / 2);
    } catch (IndexOutOfBoundsException e) {
      fail();
    }
    buffer.writerIndex(CAPACITY / 4);
  }

  @Test(groups = "unit")
  public void writerIndexBoundaryCheck4() {
    buffer.writerIndex(0);
    buffer.readerIndex(0);
    buffer.writerIndex(CAPACITY);

    buffer.writeBytes(ByteBuffer.wrap(EmptyArrays.EMPTY_BYTES));
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void getBooleanBoundaryCheck1() {
    buffer.getBoolean(-1);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void getBooleanBoundaryCheck2() {
    buffer.getBoolean(buffer.capacity());
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void getByteBoundaryCheck1() {
    buffer.getByte(-1);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void getByteBoundaryCheck2() {
    buffer.getByte(buffer.capacity());
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void getShortBoundaryCheck1() {
    buffer.getShort(-1);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void getShortBoundaryCheck2() {
    buffer.getShort(buffer.capacity() - 1);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void getMediumBoundaryCheck1() {
    buffer.getMedium(-1);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void getMediumBoundaryCheck2() {
    buffer.getMedium(buffer.capacity() - 2);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void getIntBoundaryCheck1() {
    buffer.getInt(-1);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void getIntBoundaryCheck2() {
    buffer.getInt(buffer.capacity() - 3);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void getLongBoundaryCheck1() {
    buffer.getLong(-1);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void getLongBoundaryCheck2() {
    buffer.getLong(buffer.capacity() - 7);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void getByteArrayBoundaryCheck1() {
    buffer.getBytes(-1, EmptyArrays.EMPTY_BYTES);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void getByteArrayBoundaryCheck2() {
    buffer.getBytes(-1, EmptyArrays.EMPTY_BYTES, 0, 0);
  }

  @Test(groups = "unit")
  public void getByteArrayBoundaryCheck3() {
    byte[] dst = new byte[4];
    buffer.setInt(0, 0x01020304);
    try {
      buffer.getBytes(0, dst, -1, 4);
      fail();
    } catch (IndexOutOfBoundsException e) {
      // Success
    }

    // No partial copy is expected.
    assertEquals(0, dst[0]);
    assertEquals(0, dst[1]);
    assertEquals(0, dst[2]);
    assertEquals(0, dst[3]);
  }

  @Test(groups = "unit")
  public void getByteArrayBoundaryCheck4() {
    byte[] dst = new byte[4];
    buffer.setInt(0, 0x01020304);
    try {
      buffer.getBytes(0, dst, 1, 4);
      fail();
    } catch (IndexOutOfBoundsException e) {
      // Success
    }

    // No partial copy is expected.
    assertEquals(0, dst[0]);
    assertEquals(0, dst[1]);
    assertEquals(0, dst[2]);
    assertEquals(0, dst[3]);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void getByteBufferBoundaryCheck() {
    buffer.getBytes(-1, ByteBuffer.allocate(0));
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void copyBoundaryCheck1() {
    buffer.copy(-1, 0);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void copyBoundaryCheck2() {
    buffer.copy(0, buffer.capacity() + 1);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void copyBoundaryCheck3() {
    buffer.copy(buffer.capacity() + 1, 0);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void copyBoundaryCheck4() {
    buffer.copy(buffer.capacity(), 1);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void setIndexBoundaryCheck1() {
    buffer.setIndex(-1, CAPACITY);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void setIndexBoundaryCheck2() {
    buffer.setIndex(CAPACITY / 2, CAPACITY / 4);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void setIndexBoundaryCheck3() {
    buffer.setIndex(0, CAPACITY + 1);
  }

  @Test(groups = "unit")
  public void getByteBufferState() {
    ByteBuffer dst = ByteBuffer.allocate(4);
    dst.position(1);
    dst.limit(3);

    buffer.setByte(0, (byte) 1);
    buffer.setByte(1, (byte) 2);
    buffer.setByte(2, (byte) 3);
    buffer.setByte(3, (byte) 4);
    buffer.getBytes(1, dst);

    assertEquals(3, dst.position());
    assertEquals(3, dst.limit());

    dst.clear();
    assertEquals(0, dst.get(0));
    assertEquals(2, dst.get(1));
    assertEquals(3, dst.get(2));
    assertEquals(0, dst.get(3));
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void getDirectByteBufferBoundaryCheck() {
    buffer.getBytes(-1, ByteBuffer.allocateDirect(0));
  }

  @Test(groups = "unit")
  public void getDirectByteBufferState() {
    ByteBuffer dst = ByteBuffer.allocateDirect(4);
    dst.position(1);
    dst.limit(3);

    buffer.setByte(0, (byte) 1);
    buffer.setByte(1, (byte) 2);
    buffer.setByte(2, (byte) 3);
    buffer.setByte(3, (byte) 4);
    buffer.getBytes(1, dst);

    assertEquals(3, dst.position());
    assertEquals(3, dst.limit());

    dst.clear();
    assertEquals(0, dst.get(0));
    assertEquals(2, dst.get(1));
    assertEquals(3, dst.get(2));
    assertEquals(0, dst.get(3));
  }

  @Test(groups = "unit")
  public void testRandomByteAccess() {
    for (int i = 0; i < buffer.capacity(); i++) {
      byte value = (byte) random.nextInt();
      buffer.setByte(i, value);
    }

    random.setSeed(seed);
    for (int i = 0; i < buffer.capacity(); i++) {
      byte value = (byte) random.nextInt();
      assertEquals(value, buffer.getByte(i));
    }
  }

  @Test(groups = "unit")
  public void testRandomUnsignedByteAccess() {
    for (int i = 0; i < buffer.capacity(); i++) {
      byte value = (byte) random.nextInt();
      buffer.setByte(i, value);
    }

    random.setSeed(seed);
    for (int i = 0; i < buffer.capacity(); i++) {
      int value = random.nextInt() & 0xFF;
      assertEquals(value, buffer.getUnsignedByte(i));
    }
  }

  @Test(groups = "unit")
  public void testRandomShortAccess() {
    testRandomShortAccess(true);
  }

  @Test(groups = "unit")
  public void testRandomShortLEAccess() {
    testRandomShortAccess(false);
  }

  private void testRandomShortAccess(boolean testBigEndian) {
    for (int i = 0; i < buffer.capacity() - 1; i += 2) {
      short value = (short) random.nextInt();
      if (testBigEndian) {
        buffer.setShort(i, value);
      } else {
        buffer.setShortLE(i, value);
      }
    }

    random.setSeed(seed);
    for (int i = 0; i < buffer.capacity() - 1; i += 2) {
      short value = (short) random.nextInt();
      if (testBigEndian) {
        assertEquals(value, buffer.getShort(i));
      } else {
        assertEquals(value, buffer.getShortLE(i));
      }
    }
  }

  @Test(groups = "unit")
  public void testShortConsistentWithByteBuffer() {
    testShortConsistentWithByteBuffer(true, true);
    testShortConsistentWithByteBuffer(true, false);
    testShortConsistentWithByteBuffer(false, true);
    testShortConsistentWithByteBuffer(false, false);
  }

  private void testShortConsistentWithByteBuffer(boolean direct, boolean testBigEndian) {
    for (int i = 0; i < JAVA_BYTEBUFFER_CONSISTENCY_ITERATIONS; ++i) {
      ByteBuffer javaBuffer =
          direct ? ByteBuffer.allocateDirect(buffer.capacity()) : ByteBuffer.allocate(buffer.capacity());
      if (!testBigEndian) {
        javaBuffer = javaBuffer.order(ByteOrder.LITTLE_ENDIAN);
      }

      short expected = (short) (random.nextInt() & 0xFFFF);
      javaBuffer.putShort(expected);

      final int bufferIndex = buffer.capacity() - 2;
      if (testBigEndian) {
        buffer.setShort(bufferIndex, expected);
      } else {
        buffer.setShortLE(bufferIndex, expected);
      }
      javaBuffer.flip();

      short javaActual = javaBuffer.getShort();
      assertEquals(expected, javaActual);
      assertEquals(javaActual, testBigEndian ? buffer.getShort(bufferIndex) : buffer.getShortLE(bufferIndex));
    }
  }

  @Test(groups = "unit")
  public void testRandomUnsignedShortAccess() {
    testRandomUnsignedShortAccess(true);
  }

  @Test(groups = "unit")
  public void testRandomUnsignedShortLEAccess() {
    testRandomUnsignedShortAccess(false);
  }

  private void testRandomUnsignedShortAccess(boolean testBigEndian) {
    for (int i = 0; i < buffer.capacity() - 1; i += 2) {
      short value = (short) random.nextInt();
      if (testBigEndian) {
        buffer.setShort(i, value);
      } else {
        buffer.setShortLE(i, value);
      }
    }

    random.setSeed(seed);
    for (int i = 0; i < buffer.capacity() - 1; i += 2) {
      int value = random.nextInt() & 0xFFFF;
      if (testBigEndian) {
        assertEquals(value, buffer.getUnsignedShort(i));
      } else {
        assertEquals(value, buffer.getUnsignedShortLE(i));
      }
    }
  }

  @Test(groups = "unit")
  public void testRandomMediumAccess() {
    testRandomMediumAccess(true);
  }

  @Test(groups = "unit")
  public void testRandomMediumLEAccess() {
    testRandomMediumAccess(false);
  }

  private void testRandomMediumAccess(boolean testBigEndian) {
    for (int i = 0; i < buffer.capacity() - 2; i += 3) {
      int value = random.nextInt();
      if (testBigEndian) {
        buffer.setMedium(i, value);
      } else {
        buffer.setMediumLE(i, value);
      }
    }

    random.setSeed(seed);
    for (int i = 0; i < buffer.capacity() - 2; i += 3) {
      int value = random.nextInt() << 8 >> 8;
      if (testBigEndian) {
        assertEquals(value, buffer.getMedium(i));
      } else {
        assertEquals(value, buffer.getMediumLE(i));
      }
    }
  }

  @Test(groups = "unit")
  public void testRandomUnsignedMediumAccess() {
    testRandomUnsignedMediumAccess(true);
  }

  @Test(groups = "unit")
  public void testRandomUnsignedMediumLEAccess() {
    testRandomUnsignedMediumAccess(false);
  }

  private void testRandomUnsignedMediumAccess(boolean testBigEndian) {
    for (int i = 0; i < buffer.capacity() - 2; i += 3) {
      int value = random.nextInt();
      if (testBigEndian) {
        buffer.setMedium(i, value);
      } else {
        buffer.setMediumLE(i, value);
      }
    }

    random.setSeed(seed);
    for (int i = 0; i < buffer.capacity() - 2; i += 3) {
      int value = random.nextInt() & 0x00FFFFFF;
      if (testBigEndian) {
        assertEquals(value, buffer.getUnsignedMedium(i));
      } else {
        assertEquals(value, buffer.getUnsignedMediumLE(i));
      }
    }
  }

  @Test(groups = "unit")
  public void testMediumConsistentWithByteBuffer() {
    testMediumConsistentWithByteBuffer(true, true);
    testMediumConsistentWithByteBuffer(true, false);
    testMediumConsistentWithByteBuffer(false, true);
    testMediumConsistentWithByteBuffer(false, false);
  }

  private void testMediumConsistentWithByteBuffer(boolean direct, boolean testBigEndian) {
    for (int i = 0; i < JAVA_BYTEBUFFER_CONSISTENCY_ITERATIONS; ++i) {
      ByteBuffer javaBuffer =
          direct ? ByteBuffer.allocateDirect(buffer.capacity()) : ByteBuffer.allocate(buffer.capacity());
      if (!testBigEndian) {
        javaBuffer = javaBuffer.order(ByteOrder.LITTLE_ENDIAN);
      }

      int expected = random.nextInt() & 0x00FFFFFF;
      javaBuffer.putInt(expected);

      final int bufferIndex = buffer.capacity() - 3;
      if (testBigEndian) {
        buffer.setMedium(bufferIndex, expected);
      } else {
        buffer.setMediumLE(bufferIndex, expected);
      }
      javaBuffer.flip();

      int javaActual = javaBuffer.getInt();
      assertEquals(expected, javaActual);
      assertEquals(
          javaActual,
          testBigEndian ? buffer.getUnsignedMedium(bufferIndex) : buffer.getUnsignedMediumLE(bufferIndex));
    }
  }

  @Test(groups = "unit")
  public void testRandomIntAccess() {
    testRandomIntAccess(true);
  }

  @Test(groups = "unit")
  public void testRandomIntLEAccess() {
    testRandomIntAccess(false);
  }

  private void testRandomIntAccess(boolean testBigEndian) {
    for (int i = 0; i < buffer.capacity() - 3; i += 4) {
      int value = random.nextInt();
      if (testBigEndian) {
        buffer.setInt(i, value);
      } else {
        buffer.setIntLE(i, value);
      }
    }

    random.setSeed(seed);
    for (int i = 0; i < buffer.capacity() - 3; i += 4) {
      int value = random.nextInt();
      if (testBigEndian) {
        assertEquals(value, buffer.getInt(i));
      } else {
        assertEquals(value, buffer.getIntLE(i));
      }
    }
  }

  @Test(groups = "unit")
  public void testIntConsistentWithByteBuffer() {
    testIntConsistentWithByteBuffer(true, true);
    testIntConsistentWithByteBuffer(true, false);
    testIntConsistentWithByteBuffer(false, true);
    testIntConsistentWithByteBuffer(false, false);
  }

  private void testIntConsistentWithByteBuffer(boolean direct, boolean testBigEndian) {
    for (int i = 0; i < JAVA_BYTEBUFFER_CONSISTENCY_ITERATIONS; ++i) {
      ByteBuffer javaBuffer =
          direct ? ByteBuffer.allocateDirect(buffer.capacity()) : ByteBuffer.allocate(buffer.capacity());
      if (!testBigEndian) {
        javaBuffer = javaBuffer.order(ByteOrder.LITTLE_ENDIAN);
      }

      int expected = random.nextInt();
      javaBuffer.putInt(expected);

      final int bufferIndex = buffer.capacity() - 4;
      if (testBigEndian) {
        buffer.setInt(bufferIndex, expected);
      } else {
        buffer.setIntLE(bufferIndex, expected);
      }
      javaBuffer.flip();

      int javaActual = javaBuffer.getInt();
      assertEquals(expected, javaActual);
      assertEquals(javaActual, testBigEndian ? buffer.getInt(bufferIndex) : buffer.getIntLE(bufferIndex));
    }
  }

  @Test(groups = "unit")
  public void testRandomUnsignedIntAccess() {
    testRandomUnsignedIntAccess(true);
  }

  @Test(groups = "unit")
  public void testRandomUnsignedIntLEAccess() {
    testRandomUnsignedIntAccess(false);
  }

  private void testRandomUnsignedIntAccess(boolean testBigEndian) {
    for (int i = 0; i < buffer.capacity() - 3; i += 4) {
      int value = random.nextInt();
      if (testBigEndian) {
        buffer.setInt(i, value);
      } else {
        buffer.setIntLE(i, value);
      }
    }

    random.setSeed(seed);
    for (int i = 0; i < buffer.capacity() - 3; i += 4) {
      long value = random.nextInt() & 0xFFFFFFFFL;
      if (testBigEndian) {
        assertEquals(value, buffer.getUnsignedInt(i));
      } else {
        assertEquals(value, buffer.getUnsignedIntLE(i));
      }
    }
  }

  @Test(groups = "unit")
  public void testRandomLongAccess() {
    testRandomLongAccess(true);
  }

  @Test(groups = "unit")
  public void testRandomLongLEAccess() {
    testRandomLongAccess(false);
  }

  private void testRandomLongAccess(boolean testBigEndian) {
    for (int i = 0; i < buffer.capacity() - 7; i += 8) {
      long value = random.nextLong();
      if (testBigEndian) {
        buffer.setLong(i, value);
      } else {
        buffer.setLongLE(i, value);
      }
    }

    random.setSeed(seed);
    for (int i = 0; i < buffer.capacity() - 7; i += 8) {
      long value = random.nextLong();
      if (testBigEndian) {
        assertEquals(value, buffer.getLong(i));
      } else {
        assertEquals(value, buffer.getLongLE(i));
      }
    }
  }

  @Test(groups = "unit")
  public void testLongConsistentWithByteBuffer() {
    testLongConsistentWithByteBuffer(true, true);
    testLongConsistentWithByteBuffer(true, false);
    testLongConsistentWithByteBuffer(false, true);
    testLongConsistentWithByteBuffer(false, false);
  }

  private void testLongConsistentWithByteBuffer(boolean direct, boolean testBigEndian) {
    for (int i = 0; i < JAVA_BYTEBUFFER_CONSISTENCY_ITERATIONS; ++i) {
      ByteBuffer javaBuffer =
          direct ? ByteBuffer.allocateDirect(buffer.capacity()) : ByteBuffer.allocate(buffer.capacity());
      if (!testBigEndian) {
        javaBuffer = javaBuffer.order(ByteOrder.LITTLE_ENDIAN);
      }

      long expected = random.nextLong();
      javaBuffer.putLong(expected);

      final int bufferIndex = buffer.capacity() - 8;
      if (testBigEndian) {
        buffer.setLong(bufferIndex, expected);
      } else {
        buffer.setLongLE(bufferIndex, expected);
      }
      javaBuffer.flip();

      long javaActual = javaBuffer.getLong();
      assertEquals(expected, javaActual);
      assertEquals(javaActual, testBigEndian ? buffer.getLong(bufferIndex) : buffer.getLongLE(bufferIndex));
    }
  }

  @Test(groups = "unit")
  public void testRandomFloatAccess() {
    testRandomFloatAccess(true);
  }

  @Test(groups = "unit")
  public void testRandomFloatLEAccess() {
    testRandomFloatAccess(false);
  }

  private void testRandomFloatAccess(boolean testBigEndian) {
    for (int i = 0; i < buffer.capacity() - 7; i += 8) {
      float value = random.nextFloat();
      if (testBigEndian) {
        buffer.setFloat(i, value);
      } else {
        buffer.setFloatLE(i, value);
      }
    }

    random.setSeed(seed);
    for (int i = 0; i < buffer.capacity() - 7; i += 8) {
      float expected = random.nextFloat();
      float actual = testBigEndian ? buffer.getFloat(i) : buffer.getFloatLE(i);
      assertEquals(expected, actual, 0.01);
    }
  }

  @Test(groups = "unit")
  public void testRandomDoubleAccess() {
    testRandomDoubleAccess(true);
  }

  @Test(groups = "unit")
  public void testRandomDoubleLEAccess() {
    testRandomDoubleAccess(false);
  }

  private void testRandomDoubleAccess(boolean testBigEndian) {
    for (int i = 0; i < buffer.capacity() - 7; i += 8) {
      double value = random.nextDouble();
      if (testBigEndian) {
        buffer.setDouble(i, value);
      } else {
        buffer.setDoubleLE(i, value);
      }
    }

    random.setSeed(seed);
    for (int i = 0; i < buffer.capacity() - 7; i += 8) {
      double expected = random.nextDouble();
      double actual = testBigEndian ? buffer.getDouble(i) : buffer.getDoubleLE(i);
      assertEquals(expected, actual, 0.01);
    }
  }

  @Test(groups = "unit")
  public void testSetZero() {
    buffer.clear();
    while (buffer.isWritable()) {
      buffer.writeByte((byte) 0xFF);
    }

    for (int i = 0; i < buffer.capacity();) {
      int length = Math.min(buffer.capacity() - i, random.nextInt(32));
      buffer.setZero(i, length);
      i += length;
    }

    for (int i = 0; i < buffer.capacity(); i++) {
      assertEquals(0, buffer.getByte(i));
    }
  }

  @Test(groups = "unit")
  public void testSequentialByteAccess() {
    buffer.writerIndex(0);
    for (int i = 0; i < buffer.capacity(); i++) {
      byte value = (byte) random.nextInt();
      assertEquals(i, buffer.writerIndex());
      assertTrue(buffer.isWritable());
      buffer.writeByte(value);
    }

    assertEquals(0, buffer.readerIndex());
    assertEquals(buffer.capacity(), buffer.writerIndex());
    assertFalse(buffer.isWritable());

    random.setSeed(seed);
    for (int i = 0; i < buffer.capacity(); i++) {
      byte value = (byte) random.nextInt();
      assertEquals(i, buffer.readerIndex());
      assertTrue(buffer.isReadable());
      assertEquals(value, buffer.readByte());
    }

    assertEquals(buffer.capacity(), buffer.readerIndex());
    assertEquals(buffer.capacity(), buffer.writerIndex());
    assertFalse(buffer.isReadable());
    assertFalse(buffer.isWritable());
  }

  @Test(groups = "unit")
  public void testSequentialUnsignedByteAccess() {
    buffer.writerIndex(0);
    for (int i = 0; i < buffer.capacity(); i++) {
      byte value = (byte) random.nextInt();
      assertEquals(i, buffer.writerIndex());
      assertTrue(buffer.isWritable());
      buffer.writeByte(value);
    }

    assertEquals(0, buffer.readerIndex());
    assertEquals(buffer.capacity(), buffer.writerIndex());
    assertFalse(buffer.isWritable());

    random.setSeed(seed);
    for (int i = 0; i < buffer.capacity(); i++) {
      int value = random.nextInt() & 0xFF;
      assertEquals(i, buffer.readerIndex());
      assertTrue(buffer.isReadable());
      assertEquals(value, buffer.readUnsignedByte());
    }

    assertEquals(buffer.capacity(), buffer.readerIndex());
    assertEquals(buffer.capacity(), buffer.writerIndex());
    assertFalse(buffer.isReadable());
    assertFalse(buffer.isWritable());
  }

  @Test(groups = "unit")
  public void testSequentialShortAccess() {
    testSequentialShortAccess(true);
  }

  @Test(groups = "unit")
  public void testSequentialShortLEAccess() {
    testSequentialShortAccess(false);
  }

  private void testSequentialShortAccess(boolean testBigEndian) {
    buffer.writerIndex(0);
    for (int i = 0; i < buffer.capacity(); i += 2) {
      short value = (short) random.nextInt();
      assertEquals(i, buffer.writerIndex());
      assertTrue(buffer.isWritable());
      if (testBigEndian) {
        buffer.writeShort(value);
      } else {
        buffer.writeShortLE(value);
      }
    }

    assertEquals(0, buffer.readerIndex());
    assertEquals(buffer.capacity(), buffer.writerIndex());
    assertFalse(buffer.isWritable());

    random.setSeed(seed);
    for (int i = 0; i < buffer.capacity(); i += 2) {
      short value = (short) random.nextInt();
      assertEquals(i, buffer.readerIndex());
      assertTrue(buffer.isReadable());
      if (testBigEndian) {
        assertEquals(value, buffer.readShort());
      } else {
        assertEquals(value, buffer.readShortLE());
      }
    }

    assertEquals(buffer.capacity(), buffer.readerIndex());
    assertEquals(buffer.capacity(), buffer.writerIndex());
    assertFalse(buffer.isReadable());
    assertFalse(buffer.isWritable());
  }

  @Test(groups = "unit")
  public void testSequentialUnsignedShortAccess() {
    testSequentialUnsignedShortAccess(true);
  }

  @Test(groups = "unit")
  public void testSequentialUnsignedShortLEAccess() {
    testSequentialUnsignedShortAccess(true);
  }

  private void testSequentialUnsignedShortAccess(boolean testBigEndian) {
    buffer.writerIndex(0);
    for (int i = 0; i < buffer.capacity(); i += 2) {
      short value = (short) random.nextInt();
      assertEquals(i, buffer.writerIndex());
      assertTrue(buffer.isWritable());
      if (testBigEndian) {
        buffer.writeShort(value);
      } else {
        buffer.writeShortLE(value);
      }
    }

    assertEquals(0, buffer.readerIndex());
    assertEquals(buffer.capacity(), buffer.writerIndex());
    assertFalse(buffer.isWritable());

    random.setSeed(seed);
    for (int i = 0; i < buffer.capacity(); i += 2) {
      int value = random.nextInt() & 0xFFFF;
      assertEquals(i, buffer.readerIndex());
      assertTrue(buffer.isReadable());
      if (testBigEndian) {
        assertEquals(value, buffer.readUnsignedShort());
      } else {
        assertEquals(value, buffer.readUnsignedShortLE());
      }
    }

    assertEquals(buffer.capacity(), buffer.readerIndex());
    assertEquals(buffer.capacity(), buffer.writerIndex());
    assertFalse(buffer.isReadable());
    assertFalse(buffer.isWritable());
  }

  @Test(groups = "unit")
  public void testSequentialMediumAccess() {
    testSequentialMediumAccess(true);
  }

  @Test(groups = "unit")
  public void testSequentialMediumLEAccess() {
    testSequentialMediumAccess(false);
  }

  private void testSequentialMediumAccess(boolean testBigEndian) {
    buffer.writerIndex(0);
    for (int i = 0; i < buffer.capacity() / 3 * 3; i += 3) {
      int value = random.nextInt();
      assertEquals(i, buffer.writerIndex());
      assertTrue(buffer.isWritable());
      if (testBigEndian) {
        buffer.writeMedium(value);
      } else {
        buffer.writeMediumLE(value);
      }
    }

    assertEquals(0, buffer.readerIndex());
    assertEquals(buffer.capacity() / 3 * 3, buffer.writerIndex());
    assertEquals(buffer.capacity() % 3, buffer.writableBytes());

    random.setSeed(seed);
    for (int i = 0; i < buffer.capacity() / 3 * 3; i += 3) {
      int value = random.nextInt() << 8 >> 8;
      assertEquals(i, buffer.readerIndex());
      assertTrue(buffer.isReadable());
      if (testBigEndian) {
        assertEquals(value, buffer.readMedium());
      } else {
        assertEquals(value, buffer.readMediumLE());
      }
    }

    assertEquals(buffer.capacity() / 3 * 3, buffer.readerIndex());
    assertEquals(buffer.capacity() / 3 * 3, buffer.writerIndex());
    assertEquals(0, buffer.readableBytes());
    assertEquals(buffer.capacity() % 3, buffer.writableBytes());
  }

  @Test(groups = "unit")
  public void testSequentialUnsignedMediumAccess() {
    testSequentialUnsignedMediumAccess(true);
  }

  @Test(groups = "unit")
  public void testSequentialUnsignedMediumLEAccess() {
    testSequentialUnsignedMediumAccess(false);
  }

  private void testSequentialUnsignedMediumAccess(boolean testBigEndian) {
    buffer.writerIndex(0);
    for (int i = 0; i < buffer.capacity() / 3 * 3; i += 3) {
      int value = random.nextInt() & 0x00FFFFFF;
      assertEquals(i, buffer.writerIndex());
      assertTrue(buffer.isWritable());
      if (testBigEndian) {
        buffer.writeMedium(value);
      } else {
        buffer.writeMediumLE(value);
      }
    }

    assertEquals(0, buffer.readerIndex());
    assertEquals(buffer.capacity() / 3 * 3, buffer.writerIndex());
    assertEquals(buffer.capacity() % 3, buffer.writableBytes());

    random.setSeed(seed);
    for (int i = 0; i < buffer.capacity() / 3 * 3; i += 3) {
      int value = random.nextInt() & 0x00FFFFFF;
      assertEquals(i, buffer.readerIndex());
      assertTrue(buffer.isReadable());
      if (testBigEndian) {
        assertEquals(value, buffer.readUnsignedMedium());
      } else {
        assertEquals(value, buffer.readUnsignedMediumLE());
      }
    }

    assertEquals(buffer.capacity() / 3 * 3, buffer.readerIndex());
    assertEquals(buffer.capacity() / 3 * 3, buffer.writerIndex());
    assertEquals(0, buffer.readableBytes());
    assertEquals(buffer.capacity() % 3, buffer.writableBytes());
  }

  @Test(groups = "unit")
  public void testSequentialIntAccess() {
    testSequentialIntAccess(true);
  }

  @Test(groups = "unit")
  public void testSequentialIntLEAccess() {
    testSequentialIntAccess(false);
  }

  private void testSequentialIntAccess(boolean testBigEndian) {
    buffer.writerIndex(0);
    for (int i = 0; i < buffer.capacity(); i += 4) {
      int value = random.nextInt();
      assertEquals(i, buffer.writerIndex());
      assertTrue(buffer.isWritable());
      if (testBigEndian) {
        buffer.writeInt(value);
      } else {
        buffer.writeIntLE(value);
      }
    }

    assertEquals(0, buffer.readerIndex());
    assertEquals(buffer.capacity(), buffer.writerIndex());
    assertFalse(buffer.isWritable());

    random.setSeed(seed);
    for (int i = 0; i < buffer.capacity(); i += 4) {
      int value = random.nextInt();
      assertEquals(i, buffer.readerIndex());
      assertTrue(buffer.isReadable());
      if (testBigEndian) {
        assertEquals(value, buffer.readInt());
      } else {
        assertEquals(value, buffer.readIntLE());
      }
    }

    assertEquals(buffer.capacity(), buffer.readerIndex());
    assertEquals(buffer.capacity(), buffer.writerIndex());
    assertFalse(buffer.isReadable());
    assertFalse(buffer.isWritable());
  }

  @Test(groups = "unit")
  public void testSequentialUnsignedIntAccess() {
    testSequentialUnsignedIntAccess(true);
  }

  @Test(groups = "unit")
  public void testSequentialUnsignedIntLEAccess() {
    testSequentialUnsignedIntAccess(false);
  }

  private void testSequentialUnsignedIntAccess(boolean testBigEndian) {
    buffer.writerIndex(0);
    for (int i = 0; i < buffer.capacity(); i += 4) {
      int value = random.nextInt();
      assertEquals(i, buffer.writerIndex());
      assertTrue(buffer.isWritable());
      if (testBigEndian) {
        buffer.writeInt(value);
      } else {
        buffer.writeIntLE(value);
      }
    }

    assertEquals(0, buffer.readerIndex());
    assertEquals(buffer.capacity(), buffer.writerIndex());
    assertFalse(buffer.isWritable());

    random.setSeed(seed);
    for (int i = 0; i < buffer.capacity(); i += 4) {
      long value = random.nextInt() & 0xFFFFFFFFL;
      assertEquals(i, buffer.readerIndex());
      assertTrue(buffer.isReadable());
      if (testBigEndian) {
        assertEquals(value, buffer.readUnsignedInt());
      } else {
        assertEquals(value, buffer.readUnsignedIntLE());
      }
    }

    assertEquals(buffer.capacity(), buffer.readerIndex());
    assertEquals(buffer.capacity(), buffer.writerIndex());
    assertFalse(buffer.isReadable());
    assertFalse(buffer.isWritable());
  }

  @Test(groups = "unit")
  public void testSequentialLongAccess() {
    testSequentialLongAccess(true);
  }

  @Test(groups = "unit")
  public void testSequentialLongLEAccess() {
    testSequentialLongAccess(false);
  }

  private void testSequentialLongAccess(boolean testBigEndian) {
    buffer.writerIndex(0);
    for (int i = 0; i < buffer.capacity(); i += 8) {
      long value = random.nextLong();
      assertEquals(i, buffer.writerIndex());
      assertTrue(buffer.isWritable());
      if (testBigEndian) {
        buffer.writeLong(value);
      } else {
        buffer.writeLongLE(value);
      }
    }

    assertEquals(0, buffer.readerIndex());
    assertEquals(buffer.capacity(), buffer.writerIndex());
    assertFalse(buffer.isWritable());

    random.setSeed(seed);
    for (int i = 0; i < buffer.capacity(); i += 8) {
      long value = random.nextLong();
      assertEquals(i, buffer.readerIndex());
      assertTrue(buffer.isReadable());
      if (testBigEndian) {
        assertEquals(value, buffer.readLong());
      } else {
        assertEquals(value, buffer.readLongLE());
      }
    }

    assertEquals(buffer.capacity(), buffer.readerIndex());
    assertEquals(buffer.capacity(), buffer.writerIndex());
    assertFalse(buffer.isReadable());
    assertFalse(buffer.isWritable());
  }

  @Test(groups = "unit")
  public void testByteArrayTransfer() {
    byte[] value = new byte[BLOCK_SIZE * 2];
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(value);
      buffer.setBytes(i, value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE);
    }

    random.setSeed(seed);
    byte[] expectedValue = new byte[BLOCK_SIZE * 2];
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(expectedValue);
      int valueOffset = random.nextInt(BLOCK_SIZE);
      buffer.getBytes(i, value, valueOffset, BLOCK_SIZE);
      for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++) {
        assertEquals(expectedValue[j], value[j]);
      }
    }
  }

  @Test(groups = "unit")
  public void testRandomByteArrayTransfer1() {
    byte[] value = new byte[BLOCK_SIZE];
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(value);
      buffer.setBytes(i, value);
    }

    random.setSeed(seed);
    byte[] expectedValueContent = new byte[BLOCK_SIZE];
    ByteBuf expectedValue = wrappedBuffer(expectedValueContent);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(expectedValueContent);
      buffer.getBytes(i, value);
      for (int j = 0; j < BLOCK_SIZE; j++) {
        assertEquals(expectedValue.getByte(j), value[j]);
      }
    }
  }

  @Test(groups = "unit")
  public void testRandomByteArrayTransfer2() {
    byte[] value = new byte[BLOCK_SIZE * 2];
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(value);
      buffer.setBytes(i, value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE);
    }

    random.setSeed(seed);
    byte[] expectedValueContent = new byte[BLOCK_SIZE * 2];
    ByteBuf expectedValue = wrappedBuffer(expectedValueContent);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(expectedValueContent);
      int valueOffset = random.nextInt(BLOCK_SIZE);
      buffer.getBytes(i, value, valueOffset, BLOCK_SIZE);
      for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++) {
        assertEquals(expectedValue.getByte(j), value[j]);
      }
    }
  }

  @Test(groups = "unit")
  public void testRandomHeapBufferTransfer1() {
    byte[] valueContent = new byte[BLOCK_SIZE];
    ByteBuf value = wrappedBuffer(valueContent);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(valueContent);
      value.setIndex(0, BLOCK_SIZE);
      buffer.setBytes(i, value);
      assertEquals(BLOCK_SIZE, value.readerIndex());
      assertEquals(BLOCK_SIZE, value.writerIndex());
    }

    random.setSeed(seed);
    byte[] expectedValueContent = new byte[BLOCK_SIZE];
    ByteBuf expectedValue = wrappedBuffer(expectedValueContent);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(expectedValueContent);
      value.clear();
      buffer.getBytes(i, value);
      assertEquals(0, value.readerIndex());
      assertEquals(BLOCK_SIZE, value.writerIndex());
      for (int j = 0; j < BLOCK_SIZE; j++) {
        assertEquals(expectedValue.getByte(j), value.getByte(j));
      }
    }
  }

  @Test(groups = "unit")
  public void testRandomHeapBufferTransfer2() {
    byte[] valueContent = new byte[BLOCK_SIZE * 2];
    ByteBuf value = wrappedBuffer(valueContent);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(valueContent);
      buffer.setBytes(i, value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE);
    }

    random.setSeed(seed);
    byte[] expectedValueContent = new byte[BLOCK_SIZE * 2];
    ByteBuf expectedValue = wrappedBuffer(expectedValueContent);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(expectedValueContent);
      int valueOffset = random.nextInt(BLOCK_SIZE);
      buffer.getBytes(i, value, valueOffset, BLOCK_SIZE);
      for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++) {
        assertEquals(expectedValue.getByte(j), value.getByte(j));
      }
    }
  }

  @Test(groups = "unit")
  public void testRandomDirectBufferTransfer() {
    byte[] tmp = new byte[BLOCK_SIZE * 2];
    ByteBuf value = directBuffer(BLOCK_SIZE * 2);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(tmp);
      value.setBytes(0, tmp, 0, value.capacity());
      buffer.setBytes(i, value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE);
    }

    random.setSeed(seed);
    ByteBuf expectedValue = directBuffer(BLOCK_SIZE * 2);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(tmp);
      expectedValue.setBytes(0, tmp, 0, expectedValue.capacity());
      int valueOffset = random.nextInt(BLOCK_SIZE);
      buffer.getBytes(i, value, valueOffset, BLOCK_SIZE);
      for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++) {
        assertEquals(expectedValue.getByte(j), value.getByte(j));
      }
    }
    value.release();
    expectedValue.release();
  }

  @Test(groups = "unit")
  public void testRandomByteBufferTransfer() {
    ByteBuffer value = ByteBuffer.allocate(BLOCK_SIZE * 2);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(value.array());
      value.clear().position(random.nextInt(BLOCK_SIZE));
      value.limit(value.position() + BLOCK_SIZE);
      buffer.setBytes(i, value);
    }

    random.setSeed(seed);
    ByteBuffer expectedValue = ByteBuffer.allocate(BLOCK_SIZE * 2);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(expectedValue.array());
      int valueOffset = random.nextInt(BLOCK_SIZE);
      value.clear().position(valueOffset).limit(valueOffset + BLOCK_SIZE);
      buffer.getBytes(i, value);
      assertEquals(valueOffset + BLOCK_SIZE, value.position());
      for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++) {
        assertEquals(expectedValue.get(j), value.get(j));
      }
    }
  }

  @Test(groups = "unit")
  public void testSequentialByteArrayTransfer1() {
    byte[] value = new byte[BLOCK_SIZE];
    buffer.writerIndex(0);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(value);
      assertEquals(0, buffer.readerIndex());
      assertEquals(i, buffer.writerIndex());
      buffer.writeBytes(value);
    }

    random.setSeed(seed);
    byte[] expectedValue = new byte[BLOCK_SIZE];
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(expectedValue);
      assertEquals(i, buffer.readerIndex());
      assertEquals(CAPACITY, buffer.writerIndex());
      buffer.readBytes(value);
      for (int j = 0; j < BLOCK_SIZE; j++) {
        assertEquals(expectedValue[j], value[j]);
      }
    }
  }

  @Test(groups = "unit")
  public void testSequentialByteArrayTransfer2() {
    byte[] value = new byte[BLOCK_SIZE * 2];
    buffer.writerIndex(0);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(value);
      assertEquals(0, buffer.readerIndex());
      assertEquals(i, buffer.writerIndex());
      int readerIndex = random.nextInt(BLOCK_SIZE);
      buffer.writeBytes(value, readerIndex, BLOCK_SIZE);
    }

    random.setSeed(seed);
    byte[] expectedValue = new byte[BLOCK_SIZE * 2];
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(expectedValue);
      int valueOffset = random.nextInt(BLOCK_SIZE);
      assertEquals(i, buffer.readerIndex());
      assertEquals(CAPACITY, buffer.writerIndex());
      buffer.readBytes(value, valueOffset, BLOCK_SIZE);
      for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++) {
        assertEquals(expectedValue[j], value[j]);
      }
    }
  }

  @Test(groups = "unit")
  public void testSequentialHeapBufferTransfer1() {
    byte[] valueContent = new byte[BLOCK_SIZE * 2];
    ByteBuf value = wrappedBuffer(valueContent);
    buffer.writerIndex(0);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(valueContent);
      assertEquals(0, buffer.readerIndex());
      assertEquals(i, buffer.writerIndex());
      buffer.writeBytes(value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE);
      assertEquals(0, value.readerIndex());
      assertEquals(valueContent.length, value.writerIndex());
    }

    random.setSeed(seed);
    byte[] expectedValueContent = new byte[BLOCK_SIZE * 2];
    ByteBuf expectedValue = wrappedBuffer(expectedValueContent);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(expectedValueContent);
      int valueOffset = random.nextInt(BLOCK_SIZE);
      assertEquals(i, buffer.readerIndex());
      assertEquals(CAPACITY, buffer.writerIndex());
      buffer.readBytes(value, valueOffset, BLOCK_SIZE);
      for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++) {
        assertEquals(expectedValue.getByte(j), value.getByte(j));
      }
      assertEquals(0, value.readerIndex());
      assertEquals(valueContent.length, value.writerIndex());
    }
  }

  @Test(groups = "unit")
  public void testSequentialHeapBufferTransfer2() {
    byte[] valueContent = new byte[BLOCK_SIZE * 2];
    ByteBuf value = wrappedBuffer(valueContent);
    buffer.writerIndex(0);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(valueContent);
      assertEquals(0, buffer.readerIndex());
      assertEquals(i, buffer.writerIndex());
      int readerIndex = random.nextInt(BLOCK_SIZE);
      value.readerIndex(readerIndex);
      value.writerIndex(readerIndex + BLOCK_SIZE);
      buffer.writeBytes(value);
      assertEquals(readerIndex + BLOCK_SIZE, value.writerIndex());
      assertEquals(value.writerIndex(), value.readerIndex());
    }

    random.setSeed(seed);
    byte[] expectedValueContent = new byte[BLOCK_SIZE * 2];
    ByteBuf expectedValue = wrappedBuffer(expectedValueContent);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(expectedValueContent);
      int valueOffset = random.nextInt(BLOCK_SIZE);
      assertEquals(i, buffer.readerIndex());
      assertEquals(CAPACITY, buffer.writerIndex());
      value.readerIndex(valueOffset);
      value.writerIndex(valueOffset);
      buffer.readBytes(value, BLOCK_SIZE);
      for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++) {
        assertEquals(expectedValue.getByte(j), value.getByte(j));
      }
      assertEquals(valueOffset, value.readerIndex());
      assertEquals(valueOffset + BLOCK_SIZE, value.writerIndex());
    }
  }

  @Test(groups = "unit")
  public void testSequentialDirectBufferTransfer1() {
    byte[] valueContent = new byte[BLOCK_SIZE * 2];
    ByteBuf value = directBuffer(BLOCK_SIZE * 2);
    buffer.writerIndex(0);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(valueContent);
      value.setBytes(0, valueContent);
      assertEquals(0, buffer.readerIndex());
      assertEquals(i, buffer.writerIndex());
      buffer.writeBytes(value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE);
      assertEquals(0, value.readerIndex());
      assertEquals(0, value.writerIndex());
    }

    random.setSeed(seed);
    byte[] expectedValueContent = new byte[BLOCK_SIZE * 2];
    ByteBuf expectedValue = wrappedBuffer(expectedValueContent);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(expectedValueContent);
      int valueOffset = random.nextInt(BLOCK_SIZE);
      value.setBytes(0, valueContent);
      assertEquals(i, buffer.readerIndex());
      assertEquals(CAPACITY, buffer.writerIndex());
      buffer.readBytes(value, valueOffset, BLOCK_SIZE);
      for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++) {
        assertEquals(expectedValue.getByte(j), value.getByte(j));
      }
      assertEquals(0, value.readerIndex());
      assertEquals(0, value.writerIndex());
    }
    value.release();
    expectedValue.release();
  }

  @Test(groups = "unit")
  public void testSequentialDirectBufferTransfer2() {
    byte[] valueContent = new byte[BLOCK_SIZE * 2];
    ByteBuf value = directBuffer(BLOCK_SIZE * 2);
    buffer.writerIndex(0);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(valueContent);
      value.setBytes(0, valueContent);
      assertEquals(0, buffer.readerIndex());
      assertEquals(i, buffer.writerIndex());
      int readerIndex = random.nextInt(BLOCK_SIZE);
      value.readerIndex(0);
      value.writerIndex(readerIndex + BLOCK_SIZE);
      value.readerIndex(readerIndex);
      buffer.writeBytes(value);
      assertEquals(readerIndex + BLOCK_SIZE, value.writerIndex());
      assertEquals(value.writerIndex(), value.readerIndex());
    }

    random.setSeed(seed);
    byte[] expectedValueContent = new byte[BLOCK_SIZE * 2];
    ByteBuf expectedValue = wrappedBuffer(expectedValueContent);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(expectedValueContent);
      value.setBytes(0, valueContent);
      int valueOffset = random.nextInt(BLOCK_SIZE);
      assertEquals(i, buffer.readerIndex());
      assertEquals(CAPACITY, buffer.writerIndex());
      value.readerIndex(valueOffset);
      value.writerIndex(valueOffset);
      buffer.readBytes(value, BLOCK_SIZE);
      for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++) {
        assertEquals(expectedValue.getByte(j), value.getByte(j));
      }
      assertEquals(valueOffset, value.readerIndex());
      assertEquals(valueOffset + BLOCK_SIZE, value.writerIndex());
    }
    value.release();
    expectedValue.release();
  }

  @Test(groups = "unit")
  public void testSequentialByteBufferBackedHeapBufferTransfer1() {
    byte[] valueContent = new byte[BLOCK_SIZE * 2];
    ByteBuf value = wrappedBuffer(ByteBuffer.allocate(BLOCK_SIZE * 2));
    value.writerIndex(0);
    buffer.writerIndex(0);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(valueContent);
      value.setBytes(0, valueContent);
      assertEquals(0, buffer.readerIndex());
      assertEquals(i, buffer.writerIndex());
      buffer.writeBytes(value, random.nextInt(BLOCK_SIZE), BLOCK_SIZE);
      assertEquals(0, value.readerIndex());
      assertEquals(0, value.writerIndex());
    }

    random.setSeed(seed);
    byte[] expectedValueContent = new byte[BLOCK_SIZE * 2];
    ByteBuf expectedValue = wrappedBuffer(expectedValueContent);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(expectedValueContent);
      int valueOffset = random.nextInt(BLOCK_SIZE);
      value.setBytes(0, valueContent);
      assertEquals(i, buffer.readerIndex());
      assertEquals(CAPACITY, buffer.writerIndex());
      buffer.readBytes(value, valueOffset, BLOCK_SIZE);
      for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++) {
        assertEquals(expectedValue.getByte(j), value.getByte(j));
      }
      assertEquals(0, value.readerIndex());
      assertEquals(0, value.writerIndex());
    }
  }

  @Test(groups = "unit")
  public void testSequentialByteBufferBackedHeapBufferTransfer2() {
    byte[] valueContent = new byte[BLOCK_SIZE * 2];
    ByteBuf value = wrappedBuffer(ByteBuffer.allocate(BLOCK_SIZE * 2));
    value.writerIndex(0);
    buffer.writerIndex(0);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(valueContent);
      value.setBytes(0, valueContent);
      assertEquals(0, buffer.readerIndex());
      assertEquals(i, buffer.writerIndex());
      int readerIndex = random.nextInt(BLOCK_SIZE);
      value.readerIndex(0);
      value.writerIndex(readerIndex + BLOCK_SIZE);
      value.readerIndex(readerIndex);
      buffer.writeBytes(value);
      assertEquals(readerIndex + BLOCK_SIZE, value.writerIndex());
      assertEquals(value.writerIndex(), value.readerIndex());
    }

    random.setSeed(seed);
    byte[] expectedValueContent = new byte[BLOCK_SIZE * 2];
    ByteBuf expectedValue = wrappedBuffer(expectedValueContent);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(expectedValueContent);
      value.setBytes(0, valueContent);
      int valueOffset = random.nextInt(BLOCK_SIZE);
      assertEquals(i, buffer.readerIndex());
      assertEquals(CAPACITY, buffer.writerIndex());
      value.readerIndex(valueOffset);
      value.writerIndex(valueOffset);
      buffer.readBytes(value, BLOCK_SIZE);
      for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++) {
        assertEquals(expectedValue.getByte(j), value.getByte(j));
      }
      assertEquals(valueOffset, value.readerIndex());
      assertEquals(valueOffset + BLOCK_SIZE, value.writerIndex());
    }
  }

  @Test(groups = "unit")
  public void testSequentialByteBufferTransfer() {
    buffer.writerIndex(0);
    ByteBuffer value = ByteBuffer.allocate(BLOCK_SIZE * 2);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(value.array());
      value.clear().position(random.nextInt(BLOCK_SIZE));
      value.limit(value.position() + BLOCK_SIZE);
      buffer.writeBytes(value);
    }

    random.setSeed(seed);
    ByteBuffer expectedValue = ByteBuffer.allocate(BLOCK_SIZE * 2);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(expectedValue.array());
      int valueOffset = random.nextInt(BLOCK_SIZE);
      value.clear().position(valueOffset).limit(valueOffset + BLOCK_SIZE);
      buffer.readBytes(value);
      assertEquals(valueOffset + BLOCK_SIZE, value.position());
      for (int j = valueOffset; j < valueOffset + BLOCK_SIZE; j++) {
        assertEquals(expectedValue.get(j), value.get(j));
      }
    }
  }

  @Test(groups = "unit")
  public void testSequentialCopiedBufferTransfer1() {
    buffer.writerIndex(0);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      byte[] value = new byte[BLOCK_SIZE];
      random.nextBytes(value);
      assertEquals(0, buffer.readerIndex());
      assertEquals(i, buffer.writerIndex());
      buffer.writeBytes(value);
    }

    random.setSeed(seed);
    byte[] expectedValue = new byte[BLOCK_SIZE];
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(expectedValue);
      assertEquals(i, buffer.readerIndex());
      assertEquals(CAPACITY, buffer.writerIndex());
      ByteBuf actualValue = buffer.readBytes(BLOCK_SIZE);
      assertEquals(wrappedBuffer(expectedValue), actualValue);

      // Make sure if it is a copied buffer.
      actualValue.setByte(0, (byte) (actualValue.getByte(0) + 1));
      assertFalse(buffer.getByte(i) == actualValue.getByte(0));
      actualValue.release();
    }
  }

  @Test(groups = "unit")
  public void testSequentialSlice1() {
    buffer.writerIndex(0);
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      byte[] value = new byte[BLOCK_SIZE];
      random.nextBytes(value);
      assertEquals(0, buffer.readerIndex());
      assertEquals(i, buffer.writerIndex());
      buffer.writeBytes(value);
    }

    random.setSeed(seed);
    byte[] expectedValue = new byte[BLOCK_SIZE];
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      random.nextBytes(expectedValue);
      assertEquals(i, buffer.readerIndex());
      assertEquals(CAPACITY, buffer.writerIndex());
      ByteBuf actualValue = buffer.readSlice(BLOCK_SIZE);
      assertEquals(buffer.order(), actualValue.order());
      assertEquals(wrappedBuffer(expectedValue), actualValue);

      // Make sure if it is a sliced buffer.
      actualValue.setByte(0, (byte) (actualValue.getByte(0) + 1));
      assertEquals(buffer.getByte(i), actualValue.getByte(0));
    }
  }

  @Test(groups = "unit")
  public void testWriteZero() {
    try {
      buffer.writeZero(-1);
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }

    buffer.clear();
    while (buffer.isWritable()) {
      buffer.writeByte((byte) 0xFF);
    }

    buffer.clear();
    for (int i = 0; i < buffer.capacity();) {
      int length = Math.min(buffer.capacity() - i, random.nextInt(32));
      buffer.writeZero(length);
      i += length;
    }

    assertEquals(0, buffer.readerIndex());
    assertEquals(buffer.capacity(), buffer.writerIndex());

    for (int i = 0; i < buffer.capacity(); i++) {
      assertEquals(0, buffer.getByte(i));
    }
  }

  @Test(groups = "unit")
  public void testDiscardReadBytes() {
    buffer.writerIndex(0);
    for (int i = 0; i < buffer.capacity(); i += 4) {
      buffer.writeInt(i);
    }
    ByteBuf copy = copiedBuffer(buffer);

    // Make sure there's no effect if called when readerIndex is 0.
    buffer.readerIndex(CAPACITY / 4);
    buffer.markReaderIndex();
    buffer.writerIndex(CAPACITY / 3);
    buffer.markWriterIndex();
    buffer.readerIndex(0);
    buffer.writerIndex(CAPACITY / 2);
    buffer.discardReadBytes();

    assertEquals(0, buffer.readerIndex());
    assertEquals(CAPACITY / 2, buffer.writerIndex());
    assertEquals(copy.slice(0, CAPACITY / 2), buffer.slice(0, CAPACITY / 2));
    buffer.resetReaderIndex();
    assertEquals(CAPACITY / 4, buffer.readerIndex());
    buffer.resetWriterIndex();
    assertEquals(CAPACITY / 3, buffer.writerIndex());

    // Make sure bytes after writerIndex is not copied.
    buffer.readerIndex(1);
    buffer.writerIndex(CAPACITY / 2);
    buffer.discardReadBytes();

    assertEquals(0, buffer.readerIndex());
    assertEquals(CAPACITY / 2 - 1, buffer.writerIndex());
    assertEquals(copy.slice(1, CAPACITY / 2 - 1), buffer.slice(0, CAPACITY / 2 - 1));

    if (discardReadBytesDoesNotMoveWritableBytes()) {
      // If writable bytes were copied, the test should fail to avoid unnecessary memory bandwidth consumption.
      assertFalse(copy.slice(CAPACITY / 2, CAPACITY / 2).equals(buffer.slice(CAPACITY / 2 - 1, CAPACITY / 2)));
    } else {
      assertEquals(copy.slice(CAPACITY / 2, CAPACITY / 2), buffer.slice(CAPACITY / 2 - 1, CAPACITY / 2));
    }

    // Marks also should be relocated.
    buffer.resetReaderIndex();
    assertEquals(CAPACITY / 4 - 1, buffer.readerIndex());
    buffer.resetWriterIndex();
    assertEquals(CAPACITY / 3 - 1, buffer.writerIndex());
    copy.release();
  }

  /**
   * The similar test case with {@link #testDiscardReadBytes()} but this one
   * discards a large chunk at once.
   */
  @Test(groups = "unit")
  public void testDiscardReadBytes2() {
    buffer.writerIndex(0);
    for (int i = 0; i < buffer.capacity(); i++) {
      buffer.writeByte((byte) i);
    }
    ByteBuf copy = copiedBuffer(buffer);

    // Discard the first (CAPACITY / 2 - 1) bytes.
    buffer.setIndex(CAPACITY / 2 - 1, CAPACITY - 1);
    buffer.discardReadBytes();
    assertEquals(0, buffer.readerIndex());
    assertEquals(CAPACITY / 2, buffer.writerIndex());
    for (int i = 0; i < CAPACITY / 2; i++) {
      assertEquals(copy.slice(CAPACITY / 2 - 1 + i, CAPACITY / 2 - i), buffer.slice(i, CAPACITY / 2 - i));
    }
    copy.release();
  }

  @Test(groups = "unit")
  public void testStreamTransfer1() throws Exception {
    byte[] expected = new byte[buffer.capacity()];
    random.nextBytes(expected);

    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      ByteArrayInputStream in = new ByteArrayInputStream(expected, i, BLOCK_SIZE);
      assertEquals(BLOCK_SIZE, buffer.setBytes(i, in, BLOCK_SIZE));
      assertEquals(-1, buffer.setBytes(i, in, 0));
    }

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      buffer.getBytes(i, out, BLOCK_SIZE);
    }

    assertTrue(Arrays.equals(expected, out.toByteArray()));
  }

  @Test(groups = "unit")
  public void testStreamTransfer2() throws Exception {
    byte[] expected = new byte[buffer.capacity()];
    random.nextBytes(expected);
    buffer.clear();

    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      ByteArrayInputStream in = new ByteArrayInputStream(expected, i, BLOCK_SIZE);
      assertEquals(i, buffer.writerIndex());
      buffer.writeBytes(in, BLOCK_SIZE);
      assertEquals(i + BLOCK_SIZE, buffer.writerIndex());
    }

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      assertEquals(i, buffer.readerIndex());
      buffer.readBytes(out, BLOCK_SIZE);
      assertEquals(i + BLOCK_SIZE, buffer.readerIndex());
    }

    assertTrue(Arrays.equals(expected, out.toByteArray()));
  }

  @Test(groups = "unit")
  public void testCopy() {
    for (int i = 0; i < buffer.capacity(); i++) {
      byte value = (byte) random.nextInt();
      buffer.setByte(i, value);
    }

    final int readerIndex = CAPACITY / 3;
    final int writerIndex = CAPACITY * 2 / 3;
    buffer.setIndex(readerIndex, writerIndex);

    // Make sure all properties are copied.
    ByteBuf copy = buffer.copy();
    assertEquals(0, copy.readerIndex());
    assertEquals(buffer.readableBytes(), copy.writerIndex());
    assertEquals(buffer.readableBytes(), copy.capacity());
    assertSame(buffer.order(), copy.order());
    for (int i = 0; i < copy.capacity(); i++) {
      assertEquals(buffer.getByte(i + readerIndex), copy.getByte(i));
    }

    // Make sure the buffer content is independent from each other.
    buffer.setByte(readerIndex, (byte) (buffer.getByte(readerIndex) + 1));
    assertTrue(buffer.getByte(readerIndex) != copy.getByte(0));
    copy.setByte(1, (byte) (copy.getByte(1) + 1));
    assertTrue(buffer.getByte(readerIndex + 1) != copy.getByte(1));
    copy.release();
  }

  @Test(groups = "unit")
  public void testDuplicate() {
    for (int i = 0; i < buffer.capacity(); i++) {
      byte value = (byte) random.nextInt();
      buffer.setByte(i, value);
    }

    final int readerIndex = CAPACITY / 3;
    final int writerIndex = CAPACITY * 2 / 3;
    buffer.setIndex(readerIndex, writerIndex);

    // Make sure all properties are copied.
    ByteBuf duplicate = buffer.duplicate();
    assertSame(buffer.order(), duplicate.order());
    assertEquals(buffer.readableBytes(), duplicate.readableBytes());
    assertEquals(0, buffer.compareTo(duplicate));

    // Make sure the buffer content is shared.
    buffer.setByte(readerIndex, (byte) (buffer.getByte(readerIndex) + 1));
    assertEquals(buffer.getByte(readerIndex), duplicate.getByte(duplicate.readerIndex()));
    duplicate.setByte(duplicate.readerIndex(), (byte) (duplicate.getByte(duplicate.readerIndex()) + 1));
    assertEquals(buffer.getByte(readerIndex), duplicate.getByte(duplicate.readerIndex()));
  }

  @Test(groups = "unit")
  public void testSliceEndianness() throws Exception {
    assertEquals(buffer.order(), buffer.slice(0, buffer.capacity()).order());
    assertEquals(buffer.order(), buffer.slice(0, buffer.capacity() - 1).order());
    assertEquals(buffer.order(), buffer.slice(1, buffer.capacity() - 1).order());
    assertEquals(buffer.order(), buffer.slice(1, buffer.capacity() - 2).order());
  }

  @Test(groups = "unit")
  public void testSliceIndex() throws Exception {
    assertEquals(0, buffer.slice(0, buffer.capacity()).readerIndex());
    assertEquals(0, buffer.slice(0, buffer.capacity() - 1).readerIndex());
    assertEquals(0, buffer.slice(1, buffer.capacity() - 1).readerIndex());
    assertEquals(0, buffer.slice(1, buffer.capacity() - 2).readerIndex());

    assertEquals(buffer.capacity(), buffer.slice(0, buffer.capacity()).writerIndex());
    assertEquals(buffer.capacity() - 1, buffer.slice(0, buffer.capacity() - 1).writerIndex());
    assertEquals(buffer.capacity() - 1, buffer.slice(1, buffer.capacity() - 1).writerIndex());
    assertEquals(buffer.capacity() - 2, buffer.slice(1, buffer.capacity() - 2).writerIndex());
  }

  @Test(groups = "unit")
  public void testRetainedSliceIndex() throws Exception {
    ByteBuf retainedSlice = buffer.retainedSlice(0, buffer.capacity());
    assertEquals(0, retainedSlice.readerIndex());
    retainedSlice.release();

    retainedSlice = buffer.retainedSlice(0, buffer.capacity() - 1);
    assertEquals(0, retainedSlice.readerIndex());
    retainedSlice.release();

    retainedSlice = buffer.retainedSlice(1, buffer.capacity() - 1);
    assertEquals(0, retainedSlice.readerIndex());
    retainedSlice.release();

    retainedSlice = buffer.retainedSlice(1, buffer.capacity() - 2);
    assertEquals(0, retainedSlice.readerIndex());
    retainedSlice.release();

    retainedSlice = buffer.retainedSlice(0, buffer.capacity());
    assertEquals(buffer.capacity(), retainedSlice.writerIndex());
    retainedSlice.release();

    retainedSlice = buffer.retainedSlice(0, buffer.capacity() - 1);
    assertEquals(buffer.capacity() - 1, retainedSlice.writerIndex());
    retainedSlice.release();

    retainedSlice = buffer.retainedSlice(1, buffer.capacity() - 1);
    assertEquals(buffer.capacity() - 1, retainedSlice.writerIndex());
    retainedSlice.release();

    retainedSlice = buffer.retainedSlice(1, buffer.capacity() - 2);
    assertEquals(buffer.capacity() - 2, retainedSlice.writerIndex());
    retainedSlice.release();
  }

  @Test(groups = "unit")
  @SuppressWarnings("ObjectEqualsNull")
  public void testEquals() {
    assertFalse(buffer.equals(null));
    assertFalse(buffer.equals(new Object()));

    byte[] value = new byte[32];
    buffer.setIndex(0, value.length);
    random.nextBytes(value);
    buffer.setBytes(0, value);

    assertEquals(buffer, wrappedBuffer(value));
    assertEquals(buffer, wrappedBuffer(value).order(LITTLE_ENDIAN));

    value[0]++;
    assertFalse(buffer.equals(wrappedBuffer(value)));
    assertFalse(buffer.equals(wrappedBuffer(value).order(LITTLE_ENDIAN)));
  }

  @Test(groups = "unit")
  public void testCompareTo() {
    try {
      buffer.compareTo(null);
      fail();
    } catch (NullPointerException e) {
      // Expected
    }

    // Fill the random stuff
    byte[] value = new byte[32];
    random.nextBytes(value);
    // Prevent overflow / underflow
    if (value[0] == 0) {
      value[0]++;
    } else if (value[0] == -1) {
      value[0]--;
    }

    buffer.setIndex(0, value.length);
    buffer.setBytes(0, value);

    assertEquals(0, buffer.compareTo(wrappedBuffer(value)));
    assertEquals(0, buffer.compareTo(wrappedBuffer(value).order(LITTLE_ENDIAN)));

    value[0]++;
    assertTrue(buffer.compareTo(wrappedBuffer(value)) < 0);
    assertTrue(buffer.compareTo(wrappedBuffer(value).order(LITTLE_ENDIAN)) < 0);
    value[0] -= 2;
    assertTrue(buffer.compareTo(wrappedBuffer(value)) > 0);
    assertTrue(buffer.compareTo(wrappedBuffer(value).order(LITTLE_ENDIAN)) > 0);
    value[0]++;

    assertTrue(buffer.compareTo(wrappedBuffer(value, 0, 31)) > 0);
    assertTrue(buffer.compareTo(wrappedBuffer(value, 0, 31).order(LITTLE_ENDIAN)) > 0);
    assertTrue(buffer.slice(0, 31).compareTo(wrappedBuffer(value)) < 0);
    assertTrue(buffer.slice(0, 31).compareTo(wrappedBuffer(value).order(LITTLE_ENDIAN)) < 0);

    ByteBuf retainedSlice = buffer.retainedSlice(0, 31);
    assertTrue(retainedSlice.compareTo(wrappedBuffer(value)) < 0);
    retainedSlice.release();

    retainedSlice = buffer.retainedSlice(0, 31);
    assertTrue(retainedSlice.compareTo(wrappedBuffer(value).order(LITTLE_ENDIAN)) < 0);
    retainedSlice.release();
  }

  @Test(groups = "unit")
  public void testCompareTo2() {
    byte[] bytes = { 1, 2, 3, 4 };
    byte[] bytesReversed = { 4, 3, 2, 1 };

    ByteBuf buf1 = newBuffer(4).clear().writeBytes(bytes).order(ByteOrder.LITTLE_ENDIAN);
    ByteBuf buf2 = newBuffer(4).clear().writeBytes(bytesReversed).order(ByteOrder.LITTLE_ENDIAN);
    ByteBuf buf3 = newBuffer(4).clear().writeBytes(bytes).order(ByteOrder.BIG_ENDIAN);
    ByteBuf buf4 = newBuffer(4).clear().writeBytes(bytesReversed).order(ByteOrder.BIG_ENDIAN);
    try {
      assertEquals(buf1.compareTo(buf2), buf3.compareTo(buf4));
      assertEquals(buf2.compareTo(buf1), buf4.compareTo(buf3));
      assertEquals(buf1.compareTo(buf3), buf2.compareTo(buf4));
      assertEquals(buf3.compareTo(buf1), buf4.compareTo(buf2));
    } finally {
      buf1.release();
      buf2.release();
      buf3.release();
      buf4.release();
    }
  }

  @Test(groups = "unit")
  public void testToString() {
    ByteBuf copied = copiedBuffer("Hello, World!", CharsetUtil.ISO_8859_1);
    buffer.clear();
    buffer.writeBytes(copied);
    assertEquals("Hello, World!", buffer.toString(CharsetUtil.ISO_8859_1));
    copied.release();
  }

  @Test(groups = "unit", timeOut = 10000)
  public void testToStringMultipleThreads() throws Throwable {
    buffer.clear();
    buffer.writeBytes("Hello, World!".getBytes(CharsetUtil.ISO_8859_1));

    final AtomicInteger counter = new AtomicInteger(30000);
    final AtomicReference<Throwable> errorRef = new AtomicReference<Throwable>();
    List<Thread> threads = new ArrayList<Thread>();
    for (int i = 0; i < 10; i++) {
      Thread thread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            while (errorRef.get() == null && counter.decrementAndGet() > 0) {
              assertEquals("Hello, World!", buffer.toString(CharsetUtil.ISO_8859_1));
            }
          } catch (Throwable cause) {
            errorRef.compareAndSet(null, cause);
          }
        }
      });
      threads.add(thread);
    }
    for (Thread thread: threads) {
      thread.start();
    }

    for (Thread thread: threads) {
      thread.join();
    }

    Throwable error = errorRef.get();
    if (error != null) {
      throw error;
    }
  }

  @Test(groups = "unit")
  public void testIndexOf() {
    buffer.clear();
    buffer.writeByte((byte) 1);
    buffer.writeByte((byte) 2);
    buffer.writeByte((byte) 3);
    buffer.writeByte((byte) 2);
    buffer.writeByte((byte) 1);

    assertEquals(-1, buffer.indexOf(1, 4, (byte) 1));
    assertEquals(-1, buffer.indexOf(4, 1, (byte) 1));
    assertEquals(1, buffer.indexOf(1, 4, (byte) 2));
    assertEquals(3, buffer.indexOf(4, 1, (byte) 2));
  }

  @Test(groups = "unit")
  public void testNioBuffer1() {
    assumeTrue(buffer.nioBufferCount() == 1);

    byte[] value = new byte[buffer.capacity()];
    random.nextBytes(value);
    buffer.clear();
    buffer.writeBytes(value);

    assertRemainingEquals(ByteBuffer.wrap(value), buffer.nioBuffer());
  }

  @Test(groups = "unit")
  public void testToByteBuffer2() {
    assumeTrue(buffer.nioBufferCount() == 1);

    byte[] value = new byte[buffer.capacity()];
    random.nextBytes(value);
    buffer.clear();
    buffer.writeBytes(value);

    for (int i = 0; i < buffer.capacity() - BLOCK_SIZE + 1; i += BLOCK_SIZE) {
      assertRemainingEquals(ByteBuffer.wrap(value, i, BLOCK_SIZE), buffer.nioBuffer(i, BLOCK_SIZE));
    }
  }

  private static void assertRemainingEquals(ByteBuffer expected, ByteBuffer actual) {
    int remaining = expected.remaining();
    int remaining2 = actual.remaining();

    assertEquals(remaining, remaining2);
    byte[] array1 = new byte[remaining];
    byte[] array2 = new byte[remaining2];
    expected.get(array1);
    actual.get(array2);
    assertEquals(array1, array2);
  }

  @Test(groups = "unit")
  public void testToByteBuffer3() {
    assumeTrue(buffer.nioBufferCount() == 1);

    assertEquals(buffer.order(), buffer.nioBuffer().order());
  }

  @Test(groups = "unit")
  public void testSkipBytes1() {
    buffer.setIndex(CAPACITY / 4, CAPACITY / 2);

    buffer.skipBytes(CAPACITY / 4);
    assertEquals(CAPACITY / 4 * 2, buffer.readerIndex());

    try {
      buffer.skipBytes(CAPACITY / 4 + 1);
      fail();
    } catch (IndexOutOfBoundsException e) {
      // Expected
    }

    // Should remain unchanged.
    assertEquals(CAPACITY / 4 * 2, buffer.readerIndex());
  }

  @Test(groups = "unit")
  public void testHashCode() {
    ByteBuf elemA = buffer(15);
    ByteBuf elemB = directBuffer(15);
    elemA.writeBytes(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5 });
    elemB.writeBytes(new byte[] { 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9 });

    Set<ByteBuf> set = new HashSet<ByteBuf>();
    set.add(elemA);
    set.add(elemB);

    assertEquals(2, set.size());
    ByteBuf elemACopy = elemA.copy();
    assertTrue(set.contains(elemACopy));

    ByteBuf elemBCopy = elemB.copy();
    assertTrue(set.contains(elemBCopy));

    buffer.clear();
    buffer.writeBytes(elemA.duplicate());

    assertTrue(set.remove(buffer));
    assertFalse(set.contains(elemA));
    assertEquals(1, set.size());

    buffer.clear();
    buffer.writeBytes(elemB.duplicate());
    assertTrue(set.remove(buffer));
    assertFalse(set.contains(elemB));
    assertEquals(0, set.size());
    elemA.release();
    elemB.release();
    elemACopy.release();
    elemBCopy.release();
  }

  // Test case for https://github.com/netty/netty/issues/325
  @Test(groups = "unit")
  public void testDiscardAllReadBytes() {
    buffer.writerIndex(buffer.capacity());
    buffer.readerIndex(buffer.writerIndex());
    buffer.discardReadBytes();
  }

  @Test(groups = "unit")
  public void testForEachByte() {
    buffer.clear();
    for (int i = 0; i < CAPACITY; i++) {
      buffer.writeByte(i + 1);
    }

    final AtomicInteger lastIndex = new AtomicInteger();
    buffer.setIndex(CAPACITY / 4, CAPACITY * 3 / 4);
    assertEquals(buffer.forEachByte(new ByteProcessor() {
      int i = CAPACITY / 4;

      @Override
      public boolean process(byte value) throws Exception {
        assertEquals(value, (byte) (i + 1));
        lastIndex.set(i);
        i++;
        return true;
      }
    }), -1);

    assertEquals(lastIndex.get(), CAPACITY * 3 / 4 - 1);
  }

  @Test(groups = "unit")
  public void testForEachByteAbort() {
    buffer.clear();
    for (int i = 0; i < CAPACITY; i++) {
      buffer.writeByte(i + 1);
    }

    final int stop = CAPACITY / 2;
    assertEquals(buffer.forEachByte(CAPACITY / 3, CAPACITY / 3, new ByteProcessor() {
      int i = CAPACITY / 3;

      @Override
      public boolean process(byte value) throws Exception {
        assertEquals(value, (byte) (i + 1));
        if (i == stop) {
          return false;
        }

        i++;
        return true;
      }
    }), stop);
  }

  @Test(groups = "unit")
  public void testForEachByteDesc() {
    buffer.clear();
    for (int i = 0; i < CAPACITY; i++) {
      buffer.writeByte(i + 1);
    }

    final AtomicInteger lastIndex = new AtomicInteger();
    assertEquals(buffer.forEachByteDesc(CAPACITY / 4, CAPACITY * 2 / 4, new ByteProcessor() {
      int i = CAPACITY * 3 / 4 - 1;

      @Override
      public boolean process(byte value) throws Exception {
        assertEquals(value, (byte) (i + 1));
        lastIndex.set(i);
        i--;
        return true;
      }
    }), -1);

    assertEquals(lastIndex.get(), CAPACITY / 4);
  }

  @Test(groups = "unit")
  public void testInternalNioBuffer() {
    testInternalNioBuffer(128);
    testInternalNioBuffer(1024);
    testInternalNioBuffer(4 * 1024);
    testInternalNioBuffer(64 * 1024);
    testInternalNioBuffer(32 * 1024 * 1024);
    testInternalNioBuffer(64 * 1024 * 1024);
  }

  private void testInternalNioBuffer(int a) {
    ByteBuf buffer = newBuffer(2);
    ByteBuffer buf = buffer.internalNioBuffer(buffer.readerIndex(), 1);
    assertEquals(1, buf.remaining());

    byte[] data = new byte[a];
    PlatformDependent.threadLocalRandom().nextBytes(data);
    buffer.writeBytes(data);

    buf = buffer.internalNioBuffer(buffer.readerIndex(), a);
    assertEquals(a, buf.remaining());

    for (int i = 0; i < a; i++) {
      assertEquals(data[i], buf.get());
    }
    assertFalse(buf.hasRemaining());
    buffer.release();
  }

  @Test(groups = "unit")
  public void testDuplicateReadGatheringByteChannelMultipleThreads() throws Exception {
    testReadGatheringByteChannelMultipleThreads(false);
  }

  @Test(groups = "unit")
  public void testSliceReadGatheringByteChannelMultipleThreads() throws Exception {
    testReadGatheringByteChannelMultipleThreads(true);
  }

  private void testReadGatheringByteChannelMultipleThreads(final boolean slice) throws Exception {
    final byte[] bytes = new byte[8];
    random.nextBytes(bytes);

    final ByteBuf buffer = newBuffer(8);
    buffer.writeBytes(bytes);
    final CountDownLatch latch = new CountDownLatch(60000);
    final CyclicBarrier barrier = new CyclicBarrier(11);
    for (int i = 0; i < 10; i++) {
      new Thread(new Runnable() {
        @Override
        public void run() {
          while (latch.getCount() > 0) {
            ByteBuf buf;
            if (slice) {
              buf = buffer.slice();
            } else {
              buf = buffer.duplicate();
            }
            TestGatheringByteChannel channel = new TestGatheringByteChannel();

            while (buf.isReadable()) {
              try {
                buf.readBytes(channel, buf.readableBytes());
              } catch (IOException e) {
                // Never happens
                return;
              }
            }
            assertEquals(bytes, channel.writtenBytes());
            latch.countDown();
          }
          try {
            barrier.await();
          } catch (Exception e) {
            // ignore
          }
        }
      }).start();
    }
    latch.await(10, TimeUnit.SECONDS);
    barrier.await(5, TimeUnit.SECONDS);
    buffer.release();
  }

  @Test(groups = "unit")
  public void testDuplicateReadOutputStreamMultipleThreads() throws Exception {
    testReadOutputStreamMultipleThreads(false);
  }

  @Test(groups = "unit")
  public void testSliceReadOutputStreamMultipleThreads() throws Exception {
    testReadOutputStreamMultipleThreads(true);
  }

  private void testReadOutputStreamMultipleThreads(final boolean slice) throws Exception {
    final byte[] bytes = new byte[8];
    random.nextBytes(bytes);

    final ByteBuf buffer = newBuffer(8);
    buffer.writeBytes(bytes);
    final CountDownLatch latch = new CountDownLatch(60000);
    final CyclicBarrier barrier = new CyclicBarrier(11);
    for (int i = 0; i < 10; i++) {
      new Thread(new Runnable() {
        @Override
        public void run() {
          while (latch.getCount() > 0) {
            ByteBuf buf;
            if (slice) {
              buf = buffer.slice();
            } else {
              buf = buffer.duplicate();
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();

            while (buf.isReadable()) {
              try {
                buf.readBytes(out, buf.readableBytes());
              } catch (IOException e) {
                // Never happens
                return;
              }
            }
            assertEquals(bytes, out.toByteArray());
            latch.countDown();
          }
          try {
            barrier.await();
          } catch (Exception e) {
            // ignore
          }
        }
      }).start();
    }
    latch.await(10, TimeUnit.SECONDS);
    barrier.await(5, TimeUnit.SECONDS);
    buffer.release();
  }

  @Test(groups = "unit")
  public void testDuplicateBytesInArrayMultipleThreads() throws Exception {
    testBytesInArrayMultipleThreads(false);
  }

  @Test(groups = "unit")
  public void testSliceBytesInArrayMultipleThreads() throws Exception {
    testBytesInArrayMultipleThreads(true);
  }

  private void testBytesInArrayMultipleThreads(final boolean slice) throws Exception {
    final byte[] bytes = new byte[8];
    random.nextBytes(bytes);

    final ByteBuf buffer = newBuffer(8);
    buffer.writeBytes(bytes);
    final AtomicReference<Throwable> cause = new AtomicReference<Throwable>();
    final CountDownLatch latch = new CountDownLatch(60000);
    final CyclicBarrier barrier = new CyclicBarrier(11);
    for (int i = 0; i < 10; i++) {
      new Thread(new Runnable() {
        @Override
        public void run() {
          while (cause.get() == null && latch.getCount() > 0) {
            ByteBuf buf;
            if (slice) {
              buf = buffer.slice();
            } else {
              buf = buffer.duplicate();
            }

            byte[] array = new byte[8];
            buf.readBytes(array);

            assertEquals(bytes, array);

            Arrays.fill(array, (byte) 0);
            buf.getBytes(0, array);
            assertEquals(bytes, array);

            latch.countDown();
          }
          try {
            barrier.await();
          } catch (Exception e) {
            // ignore
          }
        }
      }).start();
    }
    latch.await(10, TimeUnit.SECONDS);
    barrier.await(5, TimeUnit.SECONDS);
    assertNull(cause.get());
    buffer.release();
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void readByteThrowsIndexOutOfBoundsException() {
    final ByteBuf buffer = newBuffer(8);
    try {
      buffer.writeByte(0);
      assertEquals((byte) 0, buffer.readByte());
      buffer.readByte();
    } finally {
      buffer.release();
    }
  }

  @Test(groups = "unit")
  @SuppressWarnings("ForLoopThatDoesntUseLoopVariable")
  public void testNioBufferExposeOnlyRegion() {
    final ByteBuf buffer = newBuffer(8);
    byte[] data = new byte[8];
    random.nextBytes(data);
    buffer.writeBytes(data);

    ByteBuffer nioBuf = buffer.nioBuffer(1, data.length - 2);
    assertEquals(0, nioBuf.position());
    assertEquals(6, nioBuf.remaining());

    for (int i = 1; nioBuf.hasRemaining(); i++) {
      assertEquals(data[i], nioBuf.get());
    }
    buffer.release();
  }

  @Test(groups = "unit")
  public void ensureWritableWithForceDoesNotThrow() {
    ensureWritableDoesNotThrow(true);
  }

  @Test(groups = "unit")
  public void ensureWritableWithOutForceDoesNotThrow() {
    ensureWritableDoesNotThrow(false);
  }

  private void ensureWritableDoesNotThrow(boolean force) {
    final ByteBuf buffer = newBuffer(8);
    buffer.writerIndex(buffer.capacity());
    buffer.ensureWritable(8, force);
    buffer.release();
  }

  // See:
  // - https://github.com/netty/netty/issues/2587
  // - https://github.com/netty/netty/issues/2580
  @Test(groups = "unit")
  public void testLittleEndianWithExpand() {
    ByteBuf buffer = newBuffer(0).order(LITTLE_ENDIAN);
    buffer.writeInt(0x12345678);
    assertEquals("78563412", ByteBufUtil.hexDump(buffer));
    buffer.release();
  }

  private ByteBuf releasedBuffer() {
    ByteBuf buffer = newBuffer(8);

    // Clear the buffer so we are sure the reader and writer indices are 0.
    // This is important as we may return a slice from newBuffer(...).
    buffer.clear();
    assertTrue(buffer.release());
    return buffer;
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testDiscardReadBytesAfterRelease() {
    releasedBuffer().discardReadBytes();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testDiscardSomeReadBytesAfterRelease() {
    releasedBuffer().discardSomeReadBytes();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testEnsureWritableAfterRelease() {
    releasedBuffer().ensureWritable(16);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetBooleanAfterRelease() {
    releasedBuffer().getBoolean(0);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetByteAfterRelease() {
    releasedBuffer().getByte(0);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetUnsignedByteAfterRelease() {
    releasedBuffer().getUnsignedByte(0);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetShortAfterRelease() {
    releasedBuffer().getShort(0);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetShortLEAfterRelease() {
    releasedBuffer().getShortLE(0);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetUnsignedShortAfterRelease() {
    releasedBuffer().getUnsignedShort(0);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetUnsignedShortLEAfterRelease() {
    releasedBuffer().getUnsignedShortLE(0);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetMediumAfterRelease() {
    releasedBuffer().getMedium(0);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetMediumLEAfterRelease() {
    releasedBuffer().getMediumLE(0);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetUnsignedMediumAfterRelease() {
    releasedBuffer().getUnsignedMedium(0);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetIntAfterRelease() {
    releasedBuffer().getInt(0);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetIntLEAfterRelease() {
    releasedBuffer().getIntLE(0);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetUnsignedIntAfterRelease() {
    releasedBuffer().getUnsignedInt(0);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetUnsignedIntLEAfterRelease() {
    releasedBuffer().getUnsignedIntLE(0);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetLongAfterRelease() {
    releasedBuffer().getLong(0);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetLongLEAfterRelease() {
    releasedBuffer().getLongLE(0);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetCharAfterRelease() {
    releasedBuffer().getChar(0);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetFloatAfterRelease() {
    releasedBuffer().getFloat(0);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetFloatLEAfterRelease() {
    releasedBuffer().getFloatLE(0);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetDoubleAfterRelease() {
    releasedBuffer().getDouble(0);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetDoubleLEAfterRelease() {
    releasedBuffer().getDoubleLE(0);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetBytesAfterRelease() {
    ByteBuf buffer = buffer(8);
    try {
      releasedBuffer().getBytes(0, buffer);
    } finally {
      buffer.release();
    }
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetBytesAfterRelease2() {
    ByteBuf buffer = buffer();
    try {
      releasedBuffer().getBytes(0, buffer, 1);
    } finally {
      buffer.release();
    }
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetBytesAfterRelease3() {
    ByteBuf buffer = buffer();
    try {
      releasedBuffer().getBytes(0, buffer, 0, 1);
    } finally {
      buffer.release();
    }
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetBytesAfterRelease4() {
    releasedBuffer().getBytes(0, new byte[8]);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetBytesAfterRelease5() {
    releasedBuffer().getBytes(0, new byte[8], 0, 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetBytesAfterRelease6() {
    releasedBuffer().getBytes(0, ByteBuffer.allocate(8));
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetBytesAfterRelease7() throws IOException {
    releasedBuffer().getBytes(0, new ByteArrayOutputStream(), 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testGetBytesAfterRelease8() throws IOException {
    releasedBuffer().getBytes(0, new DevNullGatheringByteChannel(), 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSetBooleanAfterRelease() {
    releasedBuffer().setBoolean(0, true);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSetByteAfterRelease() {
    releasedBuffer().setByte(0, 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSetShortAfterRelease() {
    releasedBuffer().setShort(0, 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSetShortLEAfterRelease() {
    releasedBuffer().setShortLE(0, 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSetMediumAfterRelease() {
    releasedBuffer().setMedium(0, 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSetMediumLEAfterRelease() {
    releasedBuffer().setMediumLE(0, 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSetIntAfterRelease() {
    releasedBuffer().setInt(0, 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSetIntLEAfterRelease() {
    releasedBuffer().setIntLE(0, 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSetLongAfterRelease() {
    releasedBuffer().setLong(0, 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSetLongLEAfterRelease() {
    releasedBuffer().setLongLE(0, 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSetCharAfterRelease() {
    releasedBuffer().setChar(0, 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSetFloatAfterRelease() {
    releasedBuffer().setFloat(0, 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSetDoubleAfterRelease() {
    releasedBuffer().setDouble(0, 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSetBytesAfterRelease() {
    ByteBuf buffer = buffer();
    try {
      releasedBuffer().setBytes(0, buffer);
    } finally {
      buffer.release();
    }
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSetBytesAfterRelease2() {
    ByteBuf buffer = buffer();
    try {
      releasedBuffer().setBytes(0, buffer, 1);
    } finally {
      buffer.release();
    }
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSetBytesAfterRelease3() {
    ByteBuf buffer = buffer();
    try {
      releasedBuffer().setBytes(0, buffer, 0, 1);
    } finally {
      buffer.release();
    }
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSetUsAsciiCharSequenceAfterRelease() {
    testSetCharSequenceAfterRelease0(CharsetUtil.US_ASCII);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSetIso88591CharSequenceAfterRelease() {
    testSetCharSequenceAfterRelease0(CharsetUtil.ISO_8859_1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSetUtf8CharSequenceAfterRelease() {
    testSetCharSequenceAfterRelease0(CharsetUtil.UTF_8);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSetUtf16CharSequenceAfterRelease() {
    testSetCharSequenceAfterRelease0(CharsetUtil.UTF_16);
  }

  private void testSetCharSequenceAfterRelease0(Charset charset) {
    releasedBuffer().setCharSequence(0, "x", charset);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSetBytesAfterRelease4() {
    releasedBuffer().setBytes(0, new byte[8]);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSetBytesAfterRelease5() {
    releasedBuffer().setBytes(0, new byte[8], 0, 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSetBytesAfterRelease6() {
    releasedBuffer().setBytes(0, ByteBuffer.allocate(8));
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSetBytesAfterRelease7() throws IOException {
    releasedBuffer().setBytes(0, new ByteArrayInputStream(new byte[8]), 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSetBytesAfterRelease8() throws IOException {
    releasedBuffer().setBytes(0, new TestScatteringByteChannel(), 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSetZeroAfterRelease() {
    releasedBuffer().setZero(0, 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadBooleanAfterRelease() {
    releasedBuffer().readBoolean();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadByteAfterRelease() {
    releasedBuffer().readByte();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadUnsignedByteAfterRelease() {
    releasedBuffer().readUnsignedByte();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadShortAfterRelease() {
    releasedBuffer().readShort();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadShortLEAfterRelease() {
    releasedBuffer().readShortLE();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadUnsignedShortAfterRelease() {
    releasedBuffer().readUnsignedShort();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadUnsignedShortLEAfterRelease() {
    releasedBuffer().readUnsignedShortLE();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadMediumAfterRelease() {
    releasedBuffer().readMedium();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadMediumLEAfterRelease() {
    releasedBuffer().readMediumLE();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadUnsignedMediumAfterRelease() {
    releasedBuffer().readUnsignedMedium();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadUnsignedMediumLEAfterRelease() {
    releasedBuffer().readUnsignedMediumLE();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadIntAfterRelease() {
    releasedBuffer().readInt();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadIntLEAfterRelease() {
    releasedBuffer().readIntLE();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadUnsignedIntAfterRelease() {
    releasedBuffer().readUnsignedInt();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadUnsignedIntLEAfterRelease() {
    releasedBuffer().readUnsignedIntLE();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadLongAfterRelease() {
    releasedBuffer().readLong();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadLongLEAfterRelease() {
    releasedBuffer().readLongLE();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadCharAfterRelease() {
    releasedBuffer().readChar();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadFloatAfterRelease() {
    releasedBuffer().readFloat();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadFloatLEAfterRelease() {
    releasedBuffer().readFloatLE();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadDoubleAfterRelease() {
    releasedBuffer().readDouble();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadDoubleLEAfterRelease() {
    releasedBuffer().readDoubleLE();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadBytesAfterRelease() {
    releasedBuffer().readBytes(1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadBytesAfterRelease2() {
    ByteBuf buffer = buffer(8);
    try {
      releasedBuffer().readBytes(buffer);
    } finally {
      buffer.release();
    }
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadBytesAfterRelease3() {
    ByteBuf buffer = buffer(8);
    try {
      releasedBuffer().readBytes(buffer);
    } finally {
      buffer.release();
    }
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadBytesAfterRelease4() {
    ByteBuf buffer = buffer(8);
    try {
      releasedBuffer().readBytes(buffer, 0, 1);
    } finally {
      buffer.release();
    }
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadBytesAfterRelease5() {
    releasedBuffer().readBytes(new byte[8]);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadBytesAfterRelease6() {
    releasedBuffer().readBytes(new byte[8], 0, 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadBytesAfterRelease7() {
    releasedBuffer().readBytes(ByteBuffer.allocate(8));
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadBytesAfterRelease8() throws IOException {
    releasedBuffer().readBytes(new ByteArrayOutputStream(), 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadBytesAfterRelease9() throws IOException {
    releasedBuffer().readBytes(new ByteArrayOutputStream(), 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testReadBytesAfterRelease10() throws IOException {
    releasedBuffer().readBytes(new DevNullGatheringByteChannel(), 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteBooleanAfterRelease() {
    releasedBuffer().writeBoolean(true);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteByteAfterRelease() {
    releasedBuffer().writeByte(1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteShortAfterRelease() {
    releasedBuffer().writeShort(1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteShortLEAfterRelease() {
    releasedBuffer().writeShortLE(1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteMediumAfterRelease() {
    releasedBuffer().writeMedium(1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteMediumLEAfterRelease() {
    releasedBuffer().writeMediumLE(1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteIntAfterRelease() {
    releasedBuffer().writeInt(1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteIntLEAfterRelease() {
    releasedBuffer().writeIntLE(1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteLongAfterRelease() {
    releasedBuffer().writeLong(1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteLongLEAfterRelease() {
    releasedBuffer().writeLongLE(1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteCharAfterRelease() {
    releasedBuffer().writeChar(1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteFloatAfterRelease() {
    releasedBuffer().writeFloat(1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteFloatLEAfterRelease() {
    releasedBuffer().writeFloatLE(1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteDoubleAfterRelease() {
    releasedBuffer().writeDouble(1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteDoubleLEAfterRelease() {
    releasedBuffer().writeDoubleLE(1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteBytesAfterRelease() {
    ByteBuf buffer = buffer(8);
    try {
      releasedBuffer().writeBytes(buffer);
    } finally {
      buffer.release();
    }
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteBytesAfterRelease2() {
    ByteBuf buffer = copiedBuffer(new byte[8]);
    try {
      releasedBuffer().writeBytes(buffer, 1);
    } finally {
      buffer.release();
    }
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteBytesAfterRelease3() {
    ByteBuf buffer = buffer(8);
    try {
      releasedBuffer().writeBytes(buffer, 0, 1);
    } finally {
      buffer.release();
    }
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteBytesAfterRelease4() {
    releasedBuffer().writeBytes(new byte[8]);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteBytesAfterRelease5() {
    releasedBuffer().writeBytes(new byte[8], 0, 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteBytesAfterRelease6() {
    releasedBuffer().writeBytes(ByteBuffer.allocate(8));
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteBytesAfterRelease7() throws IOException {
    releasedBuffer().writeBytes(new ByteArrayInputStream(new byte[8]), 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteBytesAfterRelease8() throws IOException {
    releasedBuffer().writeBytes(new TestScatteringByteChannel(), 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteZeroAfterRelease() throws IOException {
    releasedBuffer().writeZero(1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteUsAsciiCharSequenceAfterRelease() {
    testWriteCharSequenceAfterRelease0(CharsetUtil.US_ASCII);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteIso88591CharSequenceAfterRelease() {
    testWriteCharSequenceAfterRelease0(CharsetUtil.ISO_8859_1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteUtf8CharSequenceAfterRelease() {
    testWriteCharSequenceAfterRelease0(CharsetUtil.UTF_8);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testWriteUtf16CharSequenceAfterRelease() {
    testWriteCharSequenceAfterRelease0(CharsetUtil.UTF_16);
  }

  private void testWriteCharSequenceAfterRelease0(Charset charset) {
    releasedBuffer().writeCharSequence("x", charset);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testForEachByteAfterRelease() {
    releasedBuffer().forEachByte(new TestByteProcessor());
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testForEachByteAfterRelease1() {
    releasedBuffer().forEachByte(0, 1, new TestByteProcessor());
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testForEachByteDescAfterRelease() {
    releasedBuffer().forEachByteDesc(new TestByteProcessor());
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testForEachByteDescAfterRelease1() {
    releasedBuffer().forEachByteDesc(0, 1, new TestByteProcessor());
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testCopyAfterRelease() {
    releasedBuffer().copy();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testCopyAfterRelease1() {
    releasedBuffer().copy();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testNioBufferAfterRelease() {
    releasedBuffer().nioBuffer();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testNioBufferAfterRelease1() {
    releasedBuffer().nioBuffer(0, 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testInternalNioBufferAfterRelease() {
    ByteBuf releasedBuffer = releasedBuffer();
    releasedBuffer.internalNioBuffer(releasedBuffer.readerIndex(), 1);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testNioBuffersAfterRelease() {
    releasedBuffer().nioBuffers();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testNioBuffersAfterRelease2() {
    releasedBuffer().nioBuffers(0, 1);
  }

  @Test(groups = "unit")
  public void testArrayAfterRelease() {
    ByteBuf buf = releasedBuffer();
    if (buf.hasArray()) {
      try {
        buf.array();
        fail();
      } catch (IllegalReferenceCountException e) {
        // expected
      }
    }
  }

  @Test(groups = "unit")
  public void testMemoryAddressAfterRelease() {
    ByteBuf buf = releasedBuffer();
    if (buf.hasMemoryAddress()) {
      try {
        buf.memoryAddress();
        fail();
      } catch (IllegalReferenceCountException e) {
        // expected
      }
    }
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSliceAfterRelease() {
    releasedBuffer().slice();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testSliceAfterRelease2() {
    releasedBuffer().slice(0, 1);
  }

  private static void assertSliceFailAfterRelease(ByteBuf... bufs) {
    for (ByteBuf buf: bufs) {
      if (buf.refCnt() > 0) {
        buf.release();
      }
    }
    for (ByteBuf buf: bufs) {
      try {
        assertEquals(0, buf.refCnt());
        buf.slice();
        fail();
      } catch (IllegalReferenceCountException ignored) {
        // as expected
      }
    }
  }

  @Test(groups = "unit")
  public void testSliceAfterReleaseRetainedSlice() {
    ByteBuf buf = newBuffer(1);
    ByteBuf buf2 = buf.retainedSlice(0, 1);
    assertSliceFailAfterRelease(buf, buf2);
  }

  @Test(groups = "unit")
  public void testSliceAfterReleaseRetainedSliceDuplicate() {
    ByteBuf buf = newBuffer(1);
    ByteBuf buf2 = buf.retainedSlice(0, 1);
    ByteBuf buf3 = buf2.duplicate();
    assertSliceFailAfterRelease(buf, buf2, buf3);
  }

  @Test(groups = "unit")
  public void testSliceAfterReleaseRetainedSliceRetainedDuplicate() {
    ByteBuf buf = newBuffer(1);
    ByteBuf buf2 = buf.retainedSlice(0, 1);
    ByteBuf buf3 = buf2.retainedDuplicate();
    assertSliceFailAfterRelease(buf, buf2, buf3);
  }

  @Test(groups = "unit")
  public void testSliceAfterReleaseRetainedDuplicate() {
    ByteBuf buf = newBuffer(1);
    ByteBuf buf2 = buf.retainedDuplicate();
    assertSliceFailAfterRelease(buf, buf2);
  }

  @Test(groups = "unit")
  public void testSliceAfterReleaseRetainedDuplicateSlice() {
    ByteBuf buf = newBuffer(1);
    ByteBuf buf2 = buf.retainedDuplicate();
    ByteBuf buf3 = buf2.slice(0, 1);
    assertSliceFailAfterRelease(buf, buf2, buf3);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testRetainedSliceAfterRelease() {
    releasedBuffer().retainedSlice();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testRetainedSliceAfterRelease2() {
    releasedBuffer().retainedSlice(0, 1);
  }

  private static void assertRetainedSliceFailAfterRelease(ByteBuf... bufs) {
    for (ByteBuf buf: bufs) {
      if (buf.refCnt() > 0) {
        buf.release();
      }
    }
    for (ByteBuf buf: bufs) {
      try {
        assertEquals(0, buf.refCnt());
        buf.retainedSlice();
        fail();
      } catch (IllegalReferenceCountException ignored) {
        // as expected
      }
    }
  }

  @Test(groups = "unit")
  public void testRetainedSliceAfterReleaseRetainedSlice() {
    ByteBuf buf = newBuffer(1);
    ByteBuf buf2 = buf.retainedSlice(0, 1);
    assertRetainedSliceFailAfterRelease(buf, buf2);
  }

  @Test(groups = "unit")
  public void testRetainedSliceAfterReleaseRetainedSliceDuplicate() {
    ByteBuf buf = newBuffer(1);
    ByteBuf buf2 = buf.retainedSlice(0, 1);
    ByteBuf buf3 = buf2.duplicate();
    assertRetainedSliceFailAfterRelease(buf, buf2, buf3);
  }

  @Test(groups = "unit")
  public void testRetainedSliceAfterReleaseRetainedSliceRetainedDuplicate() {
    ByteBuf buf = newBuffer(1);
    ByteBuf buf2 = buf.retainedSlice(0, 1);
    ByteBuf buf3 = buf2.retainedDuplicate();
    assertRetainedSliceFailAfterRelease(buf, buf2, buf3);
  }

  @Test(groups = "unit")
  public void testRetainedSliceAfterReleaseRetainedDuplicate() {
    ByteBuf buf = newBuffer(1);
    ByteBuf buf2 = buf.retainedDuplicate();
    assertRetainedSliceFailAfterRelease(buf, buf2);
  }

  @Test(groups = "unit")
  public void testRetainedSliceAfterReleaseRetainedDuplicateSlice() {
    ByteBuf buf = newBuffer(1);
    ByteBuf buf2 = buf.retainedDuplicate();
    ByteBuf buf3 = buf2.slice(0, 1);
    assertRetainedSliceFailAfterRelease(buf, buf2, buf3);
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testDuplicateAfterRelease() {
    releasedBuffer().duplicate();
  }

  @Test(groups = "unit", expectedExceptions = IllegalReferenceCountException.class)
  public void testRetainedDuplicateAfterRelease() {
    releasedBuffer().retainedDuplicate();
  }

  private static void assertDuplicateFailAfterRelease(ByteBuf... bufs) {
    for (ByteBuf buf: bufs) {
      if (buf.refCnt() > 0) {
        buf.release();
      }
    }
    for (ByteBuf buf: bufs) {
      try {
        assertEquals(0, buf.refCnt());
        buf.duplicate();
        fail();
      } catch (IllegalReferenceCountException ignored) {
        // as expected
      }
    }
  }

  @Test(groups = "unit")
  public void testDuplicateAfterReleaseRetainedSliceDuplicate() {
    ByteBuf buf = newBuffer(1);
    ByteBuf buf2 = buf.retainedSlice(0, 1);
    ByteBuf buf3 = buf2.duplicate();
    assertDuplicateFailAfterRelease(buf, buf2, buf3);
  }

  @Test(groups = "unit")
  public void testDuplicateAfterReleaseRetainedDuplicate() {
    ByteBuf buf = newBuffer(1);
    ByteBuf buf2 = buf.retainedDuplicate();
    assertDuplicateFailAfterRelease(buf, buf2);
  }

  @Test(groups = "unit")
  public void testDuplicateAfterReleaseRetainedDuplicateSlice() {
    ByteBuf buf = newBuffer(1);
    ByteBuf buf2 = buf.retainedDuplicate();
    ByteBuf buf3 = buf2.slice(0, 1);
    assertDuplicateFailAfterRelease(buf, buf2, buf3);
  }

  private static void assertRetainedDuplicateFailAfterRelease(ByteBuf... bufs) {
    for (ByteBuf buf: bufs) {
      if (buf.refCnt() > 0) {
        buf.release();
      }
    }
    for (ByteBuf buf: bufs) {
      try {
        assertEquals(0, buf.refCnt());
        buf.retainedDuplicate();
        fail();
      } catch (IllegalReferenceCountException ignored) {
        // as expected
      }
    }
  }

  @Test(groups = "unit")
  public void testRetainedDuplicateAfterReleaseRetainedDuplicate() {
    ByteBuf buf = newBuffer(1);
    ByteBuf buf2 = buf.retainedDuplicate();
    assertRetainedDuplicateFailAfterRelease(buf, buf2);
  }

  @Test(groups = "unit")
  public void testRetainedDuplicateAfterReleaseDuplicate() {
    ByteBuf buf = newBuffer(1);
    ByteBuf buf2 = buf.duplicate();
    assertRetainedDuplicateFailAfterRelease(buf, buf2);
  }

  @Test(groups = "unit")
  public void testRetainedDuplicateAfterReleaseRetainedSlice() {
    ByteBuf buf = newBuffer(1);
    ByteBuf buf2 = buf.retainedSlice(0, 1);
    assertRetainedDuplicateFailAfterRelease(buf, buf2);
  }

  @Test(groups = "unit")
  public void testSliceRelease() {
    ByteBuf buf = newBuffer(8);
    assertEquals(1, buf.refCnt());
    assertTrue(buf.slice().release());
    assertEquals(0, buf.refCnt());
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void testReadSliceOutOfBounds() {
    testReadSliceOutOfBounds(false);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void testReadRetainedSliceOutOfBounds() {
    testReadSliceOutOfBounds(true);
  }

  private void testReadSliceOutOfBounds(boolean retainedSlice) {
    ByteBuf buf = newBuffer(100);
    try {
      buf.writeZero(50);
      if (retainedSlice) {
        buf.readRetainedSlice(51);
      } else {
        buf.readSlice(51);
      }
      fail();
    } finally {
      buf.release();
    }
  }

  @Test(groups = "unit")
  public void testWriteUsAsciiCharSequenceExpand() {
    testWriteCharSequenceExpand(CharsetUtil.US_ASCII);
  }

  @Test(groups = "unit")
  public void testWriteUtf8CharSequenceExpand() {
    testWriteCharSequenceExpand(CharsetUtil.UTF_8);
  }

  @Test(groups = "unit")
  public void testWriteIso88591CharSequenceExpand() {
    testWriteCharSequenceExpand(CharsetUtil.ISO_8859_1);
  }

  @Test(groups = "unit")
  public void testWriteUtf16CharSequenceExpand() {
    testWriteCharSequenceExpand(CharsetUtil.UTF_16);
  }

  private void testWriteCharSequenceExpand(Charset charset) {
    ByteBuf buf = newBuffer(1);
    try {
      int writerIndex = buf.capacity() - 1;
      buf.writerIndex(writerIndex);
      int written = buf.writeCharSequence("AB", charset);
      assertEquals(writerIndex, buf.writerIndex() - written);
    } finally {
      buf.release();
    }
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void testSetUsAsciiCharSequenceNoExpand() {
    testSetCharSequenceNoExpand(CharsetUtil.US_ASCII);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void testSetUtf8CharSequenceNoExpand() {
    testSetCharSequenceNoExpand(CharsetUtil.UTF_8);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void testSetIso88591CharSequenceNoExpand() {
    testSetCharSequenceNoExpand(CharsetUtil.ISO_8859_1);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void testSetUtf16CharSequenceNoExpand() {
    testSetCharSequenceNoExpand(CharsetUtil.UTF_16);
  }

  private void testSetCharSequenceNoExpand(Charset charset) {
    ByteBuf buf = newBuffer(1);
    try {
      buf.setCharSequence(0, "AB", charset);
    } finally {
      buf.release();
    }
  }

  @Test(groups = "unit")
  public void testSetUsAsciiCharSequence() {
    testSetGetCharSequence(CharsetUtil.US_ASCII);
  }

  @Test(groups = "unit")
  public void testSetUtf8CharSequence() {
    testSetGetCharSequence(CharsetUtil.UTF_8);
  }

  @Test(groups = "unit")
  public void testSetIso88591CharSequence() {
    testSetGetCharSequence(CharsetUtil.ISO_8859_1);
  }

  @Test(groups = "unit")
  public void testSetUtf16CharSequence() {
    testSetGetCharSequence(CharsetUtil.UTF_16);
  }

  private void testSetGetCharSequence(Charset charset) {
    ByteBuf buf = newBuffer(16);
    String sequence = "AB";
    int bytes = buf.setCharSequence(1, sequence, charset);
    assertEquals(sequence, buf.getCharSequence(1, bytes, charset).toString());
    buf.release();
  }

  @Test(groups = "unit")
  public void testWriteReadUsAsciiCharSequence() {
    testWriteReadCharSequence(CharsetUtil.US_ASCII);
  }

  @Test(groups = "unit")
  public void testWriteReadUtf8CharSequence() {
    testWriteReadCharSequence(CharsetUtil.UTF_8);
  }

  @Test(groups = "unit")
  public void testWriteReadIso88591CharSequence() {
    testWriteReadCharSequence(CharsetUtil.ISO_8859_1);
  }

  @Test(groups = "unit")
  public void testWriteReadUtf16CharSequence() {
    testWriteReadCharSequence(CharsetUtil.UTF_16);
  }

  private void testWriteReadCharSequence(Charset charset) {
    ByteBuf buf = newBuffer(16);
    String sequence = "AB";
    buf.writerIndex(1);
    int bytes = buf.writeCharSequence(sequence, charset);
    buf.readerIndex(1);
    assertEquals(sequence, buf.readCharSequence(bytes, charset).toString());
    buf.release();
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void testRetainedSliceIndexOutOfBounds() {
    testSliceOutOfBounds(true, true, true);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void testRetainedSliceLengthOutOfBounds() {
    testSliceOutOfBounds(true, true, false);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void testMixedSliceAIndexOutOfBounds() {
    testSliceOutOfBounds(true, false, true);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void testMixedSliceALengthOutOfBounds() {
    testSliceOutOfBounds(true, false, false);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void testMixedSliceBIndexOutOfBounds() {
    testSliceOutOfBounds(false, true, true);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void testMixedSliceBLengthOutOfBounds() {
    testSliceOutOfBounds(false, true, false);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void testSliceIndexOutOfBounds() {
    testSliceOutOfBounds(false, false, true);
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void testSliceLengthOutOfBounds() {
    testSliceOutOfBounds(false, false, false);
  }

  @Test(groups = "unit")
  public void testRetainedSliceAndRetainedDuplicateContentIsExpected() {
    ByteBuf buf = newBuffer(8).resetWriterIndex();
    ByteBuf expected1 = newBuffer(6).resetWriterIndex();
    ByteBuf expected2 = newBuffer(5).resetWriterIndex();
    ByteBuf expected3 = newBuffer(4).resetWriterIndex();
    ByteBuf expected4 = newBuffer(3).resetWriterIndex();
    buf.writeBytes(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });
    expected1.writeBytes(new byte[] { 2, 3, 4, 5, 6, 7 });
    expected2.writeBytes(new byte[] { 3, 4, 5, 6, 7 });
    expected3.writeBytes(new byte[] { 4, 5, 6, 7 });
    expected4.writeBytes(new byte[] { 5, 6, 7 });

    ByteBuf slice1 = buf.retainedSlice(buf.readerIndex() + 1, 6);
    assertEquals(0, slice1.compareTo(expected1));
    assertEquals(0, slice1.compareTo(buf.slice(buf.readerIndex() + 1, 6)));
    // Simulate a handler that releases the original buffer, and propagates a slice.
    buf.release();

    // Advance the reader index on the slice.
    slice1.readByte();

    ByteBuf dup1 = slice1.retainedDuplicate();
    assertEquals(0, dup1.compareTo(expected2));
    assertEquals(0, dup1.compareTo(slice1.duplicate()));

    // Advance the reader index on dup1.
    dup1.readByte();

    ByteBuf dup2 = dup1.duplicate();
    assertEquals(0, dup2.compareTo(expected3));

    // Advance the reader index on dup2.
    dup2.readByte();

    ByteBuf slice2 = dup2.retainedSlice(dup2.readerIndex(), 3);
    assertEquals(0, slice2.compareTo(expected4));
    assertEquals(0, slice2.compareTo(dup2.slice(dup2.readerIndex(), 3)));

    // Cleanup the expected buffers used for testing.
    assertTrue(expected1.release());
    assertTrue(expected2.release());
    assertTrue(expected3.release());
    assertTrue(expected4.release());

    slice2.release();
    dup2.release();

    assertEquals(slice2.refCnt(), dup2.refCnt());
    assertEquals(dup2.refCnt(), dup1.refCnt());

    // The handler is now done with the original slice
    assertTrue(slice1.release());

    // Reference counting may be shared, or may be independently tracked, but at this point all buffers should
    // be deallocated and have a reference count of 0.
    assertEquals(0, buf.refCnt());
    assertEquals(0, slice1.refCnt());
    assertEquals(0, slice2.refCnt());
    assertEquals(0, dup1.refCnt());
    assertEquals(0, dup2.refCnt());
  }

  @Test(groups = "unit")
  public void testRetainedDuplicateAndRetainedSliceContentIsExpected() {
    ByteBuf buf = newBuffer(8).resetWriterIndex();
    ByteBuf expected1 = newBuffer(6).resetWriterIndex();
    ByteBuf expected2 = newBuffer(5).resetWriterIndex();
    ByteBuf expected3 = newBuffer(4).resetWriterIndex();
    buf.writeBytes(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });
    expected1.writeBytes(new byte[] { 2, 3, 4, 5, 6, 7 });
    expected2.writeBytes(new byte[] { 3, 4, 5, 6, 7 });
    expected3.writeBytes(new byte[] { 5, 6, 7 });

    ByteBuf dup1 = buf.retainedDuplicate();
    assertEquals(0, dup1.compareTo(buf));
    assertEquals(0, dup1.compareTo(buf.slice()));
    // Simulate a handler that releases the original buffer, and propagates a slice.
    buf.release();

    // Advance the reader index on the dup.
    dup1.readByte();

    ByteBuf slice1 = dup1.retainedSlice(dup1.readerIndex(), 6);
    assertEquals(0, slice1.compareTo(expected1));
    assertEquals(0, slice1.compareTo(slice1.duplicate()));

    // Advance the reader index on slice1.
    slice1.readByte();

    ByteBuf dup2 = slice1.duplicate();
    assertEquals(0, dup2.compareTo(slice1));

    // Advance the reader index on dup2.
    dup2.readByte();

    ByteBuf slice2 = dup2.retainedSlice(dup2.readerIndex() + 1, 3);
    assertEquals(0, slice2.compareTo(expected3));
    assertEquals(0, slice2.compareTo(dup2.slice(dup2.readerIndex() + 1, 3)));

    // Cleanup the expected buffers used for testing.
    assertTrue(expected1.release());
    assertTrue(expected2.release());
    assertTrue(expected3.release());

    slice2.release();
    slice1.release();

    assertEquals(slice2.refCnt(), dup2.refCnt());
    assertEquals(dup2.refCnt(), slice1.refCnt());

    // The handler is now done with the original slice
    assertTrue(dup1.release());

    // Reference counting may be shared, or may be independently tracked, but at this point all buffers should
    // be deallocated and have a reference count of 0.
    assertEquals(0, buf.refCnt());
    assertEquals(0, slice1.refCnt());
    assertEquals(0, slice2.refCnt());
    assertEquals(0, dup1.refCnt());
    assertEquals(0, dup2.refCnt());
  }

  @Test(groups = "unit")
  public void testRetainedSliceContents() {
    testSliceContents(true);
  }

  @Test(groups = "unit")
  public void testMultipleLevelRetainedSlice1() {
    testMultipleLevelRetainedSliceWithNonRetained(true, true);
  }

  @Test(groups = "unit")
  public void testMultipleLevelRetainedSlice2() {
    testMultipleLevelRetainedSliceWithNonRetained(true, false);
  }

  @Test(groups = "unit")
  public void testMultipleLevelRetainedSlice3() {
    testMultipleLevelRetainedSliceWithNonRetained(false, true);
  }

  @Test(groups = "unit")
  public void testMultipleLevelRetainedSlice4() {
    testMultipleLevelRetainedSliceWithNonRetained(false, false);
  }

  @Test(groups = "unit")
  public void testRetainedSliceReleaseOriginal1() {
    testSliceReleaseOriginal(true, true);
  }

  @Test(groups = "unit")
  public void testRetainedSliceReleaseOriginal2() {
    testSliceReleaseOriginal(true, false);
  }

  @Test(groups = "unit")
  public void testRetainedSliceReleaseOriginal3() {
    testSliceReleaseOriginal(false, true);
  }

  @Test(groups = "unit")
  public void testRetainedSliceReleaseOriginal4() {
    testSliceReleaseOriginal(false, false);
  }

  @Test(groups = "unit")
  public void testRetainedDuplicateReleaseOriginal1() {
    testDuplicateReleaseOriginal(true, true);
  }

  @Test(groups = "unit")
  public void testRetainedDuplicateReleaseOriginal2() {
    testDuplicateReleaseOriginal(true, false);
  }

  @Test(groups = "unit")
  public void testRetainedDuplicateReleaseOriginal3() {
    testDuplicateReleaseOriginal(false, true);
  }

  @Test(groups = "unit")
  public void testRetainedDuplicateReleaseOriginal4() {
    testDuplicateReleaseOriginal(false, false);
  }

  @Test(groups = "unit")
  public void testMultipleRetainedSliceReleaseOriginal1() {
    testMultipleRetainedSliceReleaseOriginal(true, true);
  }

  @Test(groups = "unit")
  public void testMultipleRetainedSliceReleaseOriginal2() {
    testMultipleRetainedSliceReleaseOriginal(true, false);
  }

  @Test(groups = "unit")
  public void testMultipleRetainedSliceReleaseOriginal3() {
    testMultipleRetainedSliceReleaseOriginal(false, true);
  }

  @Test(groups = "unit")
  public void testMultipleRetainedSliceReleaseOriginal4() {
    testMultipleRetainedSliceReleaseOriginal(false, false);
  }

  @Test(groups = "unit")
  public void testMultipleRetainedDuplicateReleaseOriginal1() {
    testMultipleRetainedDuplicateReleaseOriginal(true, true);
  }

  @Test(groups = "unit")
  public void testMultipleRetainedDuplicateReleaseOriginal2() {
    testMultipleRetainedDuplicateReleaseOriginal(true, false);
  }

  @Test(groups = "unit")
  public void testMultipleRetainedDuplicateReleaseOriginal3() {
    testMultipleRetainedDuplicateReleaseOriginal(false, true);
  }

  @Test(groups = "unit")
  public void testMultipleRetainedDuplicateReleaseOriginal4() {
    testMultipleRetainedDuplicateReleaseOriginal(false, false);
  }

  @Test(groups = "unit")
  public void testSliceContents() {
    testSliceContents(false);
  }

  @Test(groups = "unit")
  public void testRetainedDuplicateContents() {
    testDuplicateContents(true);
  }

  @Test(groups = "unit")
  public void testDuplicateContents() {
    testDuplicateContents(false);
  }

  @Test(groups = "unit", enabled = false)
  public void testDuplicateCapacityChange() {
    testDuplicateCapacityChange(false);
  }

  @Test(groups = "unit", enabled = false)
  public void testRetainedDuplicateCapacityChange() {
    testDuplicateCapacityChange(true);
  }

  @Test(groups = "unit", expectedExceptions = UnsupportedOperationException.class)
  public void testSliceCapacityChange() {
    testSliceCapacityChange(false);
  }

  @Test(groups = "unit", expectedExceptions = UnsupportedOperationException.class)
  public void testRetainedSliceCapacityChange() {
    testSliceCapacityChange(true);
  }

  @Test(groups = "unit")
  public void testRetainedSliceUnreleasable1() {
    testRetainedSliceUnreleasable(true, true);
  }

  @Test(groups = "unit")
  public void testRetainedSliceUnreleasable2() {
    testRetainedSliceUnreleasable(true, false);
  }

  @Test(groups = "unit")
  public void testRetainedSliceUnreleasable3() {
    testRetainedSliceUnreleasable(false, true);
  }

  @Test(groups = "unit")
  public void testRetainedSliceUnreleasable4() {
    testRetainedSliceUnreleasable(false, false);
  }

  @Test(groups = "unit")
  public void testReadRetainedSliceUnreleasable1() {
    testReadRetainedSliceUnreleasable(true, true);
  }

  @Test(groups = "unit")
  public void testReadRetainedSliceUnreleasable2() {
    testReadRetainedSliceUnreleasable(true, false);
  }

  @Test(groups = "unit")
  public void testReadRetainedSliceUnreleasable3() {
    testReadRetainedSliceUnreleasable(false, true);
  }

  @Test(groups = "unit")
  public void testReadRetainedSliceUnreleasable4() {
    testReadRetainedSliceUnreleasable(false, false);
  }

  @Test(groups = "unit")
  public void testRetainedDuplicateUnreleasable1() {
    testRetainedDuplicateUnreleasable(true, true);
  }

  @Test(groups = "unit")
  public void testRetainedDuplicateUnreleasable2() {
    testRetainedDuplicateUnreleasable(true, false);
  }

  @Test(groups = "unit")
  public void testRetainedDuplicateUnreleasable3() {
    testRetainedDuplicateUnreleasable(false, true);
  }

  @Test(groups = "unit")
  public void testRetainedDuplicateUnreleasable4() {
    testRetainedDuplicateUnreleasable(false, false);
  }

  private void testRetainedSliceUnreleasable(boolean initRetainedSlice, boolean finalRetainedSlice) {
    ByteBuf buf = newBuffer(8);
    ByteBuf buf1 = initRetainedSlice ? buf.retainedSlice() : buf.slice().retain();
    ByteBuf buf2 = unreleasableBuffer(buf1);
    ByteBuf buf3 = finalRetainedSlice ? buf2.retainedSlice() : buf2.slice().retain();
    assertFalse(buf3.release());
    assertFalse(buf2.release());
    buf1.release();
    assertTrue(buf.release());
    assertEquals(0, buf1.refCnt());
    assertEquals(0, buf.refCnt());
  }

  private void testReadRetainedSliceUnreleasable(boolean initRetainedSlice, boolean finalRetainedSlice) {
    ByteBuf buf = newBuffer(8);
    ByteBuf buf1 = initRetainedSlice ? buf.retainedSlice() : buf.slice().retain();
    ByteBuf buf2 = unreleasableBuffer(buf1);
    ByteBuf buf3 = finalRetainedSlice
        ? buf2.readRetainedSlice(buf2.readableBytes())
        : buf2.readSlice(buf2.readableBytes()).retain();
    assertFalse(buf3.release());
    assertFalse(buf2.release());
    buf1.release();
    assertTrue(buf.release());
    assertEquals(0, buf1.refCnt());
    assertEquals(0, buf.refCnt());
  }

  private void testRetainedDuplicateUnreleasable(boolean initRetainedDuplicate, boolean finalRetainedDuplicate) {
    ByteBuf buf = newBuffer(8);
    ByteBuf buf1 = initRetainedDuplicate ? buf.retainedDuplicate() : buf.duplicate().retain();
    ByteBuf buf2 = unreleasableBuffer(buf1);
    ByteBuf buf3 = finalRetainedDuplicate ? buf2.retainedDuplicate() : buf2.duplicate().retain();
    assertFalse(buf3.release());
    assertFalse(buf2.release());
    buf1.release();
    assertTrue(buf.release());
    assertEquals(0, buf1.refCnt());
    assertEquals(0, buf.refCnt());
  }

  private void testDuplicateCapacityChange(boolean retainedDuplicate) {
    ByteBuf buf = newBuffer(8);
    ByteBuf dup = retainedDuplicate ? buf.retainedDuplicate() : buf.duplicate();
    try {
      dup.capacity(10);
      assertEquals(buf.capacity(), dup.capacity());
      dup.capacity(5);
      assertEquals(buf.capacity(), dup.capacity());
    } finally {
      if (retainedDuplicate) {
        dup.release();
      }
      buf.release();
    }
  }

  private void testSliceCapacityChange(boolean retainedSlice) {
    ByteBuf buf = newBuffer(8);
    ByteBuf slice = retainedSlice ? buf.retainedSlice(buf.readerIndex() + 1, 3) : buf.slice(buf.readerIndex() + 1, 3);
    try {
      slice.capacity(10);
    } finally {
      if (retainedSlice) {
        slice.release();
      }
      buf.release();
    }
  }

  private void testSliceOutOfBounds(boolean initRetainedSlice, boolean finalRetainedSlice, boolean indexOutOfBounds) {
    ByteBuf buf = newBuffer(8);
    ByteBuf slice =
        initRetainedSlice ? buf.retainedSlice(buf.readerIndex() + 1, 2) : buf.slice(buf.readerIndex() + 1, 2);
    try {
      assertEquals(2, slice.capacity());
      assertEquals(2, slice.maxCapacity());
      final int index = indexOutOfBounds ? 3 : 0;
      final int length = indexOutOfBounds ? 0 : 3;
      if (finalRetainedSlice) {
        // This is expected to fail ... so no need to release.
        slice.retainedSlice(index, length);
      } else {
        slice.slice(index, length);
      }
    } finally {
      if (initRetainedSlice) {
        slice.release();
      }
      buf.release();
    }
  }

  private void testSliceContents(boolean retainedSlice) {
    ByteBuf buf = newBuffer(8).resetWriterIndex();
    ByteBuf expected = newBuffer(3).resetWriterIndex();
    buf.writeBytes(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });
    expected.writeBytes(new byte[] { 4, 5, 6 });
    ByteBuf slice = retainedSlice ? buf.retainedSlice(buf.readerIndex() + 3, 3) : buf.slice(buf.readerIndex() + 3, 3);
    try {
      assertEquals(0, slice.compareTo(expected));
      assertEquals(0, slice.compareTo(slice.duplicate()));
      ByteBuf b = slice.retainedDuplicate();
      assertEquals(0, slice.compareTo(b));
      b.release();
      assertEquals(0, slice.compareTo(slice.slice(0, slice.capacity())));
    } finally {
      if (retainedSlice) {
        slice.release();
      }
      buf.release();
      expected.release();
    }
  }

  private void testSliceReleaseOriginal(boolean retainedSlice1, boolean retainedSlice2) {
    ByteBuf buf = newBuffer(8).resetWriterIndex();
    ByteBuf expected1 = newBuffer(3).resetWriterIndex();
    ByteBuf expected2 = newBuffer(2).resetWriterIndex();
    buf.writeBytes(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });
    expected1.writeBytes(new byte[] { 6, 7, 8 });
    expected2.writeBytes(new byte[] { 7, 8 });
    ByteBuf slice1 =
        retainedSlice1 ? buf.retainedSlice(buf.readerIndex() + 5, 3) : buf.slice(buf.readerIndex() + 5, 3).retain();
    assertEquals(0, slice1.compareTo(expected1));
    // Simulate a handler that releases the original buffer, and propagates a slice.
    buf.release();

    ByteBuf slice2 = retainedSlice2
        ? slice1.retainedSlice(slice1.readerIndex() + 1, 2)
        : slice1.slice(slice1.readerIndex() + 1, 2).retain();
    assertEquals(0, slice2.compareTo(expected2));

    // Cleanup the expected buffers used for testing.
    assertTrue(expected1.release());
    assertTrue(expected2.release());

    // The handler created a slice of the slice and is now done with it.
    slice2.release();

    // The handler is now done with the original slice
    assertTrue(slice1.release());

    // Reference counting may be shared, or may be independently tracked, but at this point all buffers should
    // be deallocated and have a reference count of 0.
    assertEquals(0, buf.refCnt());
    assertEquals(0, slice1.refCnt());
    assertEquals(0, slice2.refCnt());
  }

  private void testMultipleLevelRetainedSliceWithNonRetained(boolean doSlice1, boolean doSlice2) {
    ByteBuf buf = newBuffer(8).resetWriterIndex();
    ByteBuf expected1 = newBuffer(6).resetWriterIndex();
    ByteBuf expected2 = newBuffer(4).resetWriterIndex();
    ByteBuf expected3 = newBuffer(2).resetWriterIndex();
    ByteBuf expected4SliceSlice = newBuffer(1).resetWriterIndex();
    ByteBuf expected4DupSlice = newBuffer(1).resetWriterIndex();
    buf.writeBytes(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });
    expected1.writeBytes(new byte[] { 2, 3, 4, 5, 6, 7 });
    expected2.writeBytes(new byte[] { 3, 4, 5, 6 });
    expected3.writeBytes(new byte[] { 4, 5 });
    expected4SliceSlice.writeBytes(new byte[] { 5 });
    expected4DupSlice.writeBytes(new byte[] { 4 });

    ByteBuf slice1 = buf.retainedSlice(buf.readerIndex() + 1, 6);
    assertEquals(0, slice1.compareTo(expected1));
    // Simulate a handler that releases the original buffer, and propagates a slice.
    buf.release();

    ByteBuf slice2 = slice1.retainedSlice(slice1.readerIndex() + 1, 4);
    assertEquals(0, slice2.compareTo(expected2));
    assertEquals(0, slice2.compareTo(slice2.duplicate()));
    assertEquals(0, slice2.compareTo(slice2.slice()));

    ByteBuf tmpBuf = slice2.retainedDuplicate();
    assertEquals(0, slice2.compareTo(tmpBuf));
    tmpBuf.release();
    tmpBuf = slice2.retainedSlice();
    assertEquals(0, slice2.compareTo(tmpBuf));
    tmpBuf.release();

    ByteBuf slice3 = doSlice1 ? slice2.slice(slice2.readerIndex() + 1, 2) : slice2.duplicate();
    if (doSlice1) {
      assertEquals(0, slice3.compareTo(expected3));
    } else {
      assertEquals(0, slice3.compareTo(expected2));
    }

    ByteBuf slice4 = doSlice2 ? slice3.slice(slice3.readerIndex() + 1, 1) : slice3.duplicate();
    if (doSlice1 && doSlice2) {
      assertEquals(0, slice4.compareTo(expected4SliceSlice));
    } else if (doSlice2) {
      assertEquals(0, slice4.compareTo(expected4DupSlice));
    } else {
      assertEquals(0, slice3.compareTo(slice4));
    }

    // Cleanup the expected buffers used for testing.
    assertTrue(expected1.release());
    assertTrue(expected2.release());
    assertTrue(expected3.release());
    assertTrue(expected4SliceSlice.release());
    assertTrue(expected4DupSlice.release());

    // Slice 4, 3, and 2 should effectively "share" a reference count.
    slice4.release();
    assertEquals(slice3.refCnt(), slice2.refCnt());
    assertEquals(slice3.refCnt(), slice4.refCnt());

    // Slice 1 should also release the original underlying buffer without throwing exceptions
    assertTrue(slice1.release());

    // Reference counting may be shared, or may be independently tracked, but at this point all buffers should
    // be deallocated and have a reference count of 0.
    assertEquals(0, buf.refCnt());
    assertEquals(0, slice1.refCnt());
    assertEquals(0, slice2.refCnt());
    assertEquals(0, slice3.refCnt());
  }

  private void testDuplicateReleaseOriginal(boolean retainedDuplicate1, boolean retainedDuplicate2) {
    ByteBuf buf = newBuffer(8).resetWriterIndex();
    ByteBuf expected = newBuffer(8).resetWriterIndex();
    buf.writeBytes(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });
    expected.writeBytes(buf, buf.readerIndex(), buf.readableBytes());
    ByteBuf dup1 = retainedDuplicate1 ? buf.retainedDuplicate() : buf.duplicate().retain();
    assertEquals(0, dup1.compareTo(expected));
    // Simulate a handler that releases the original buffer, and propagates a slice.
    buf.release();

    ByteBuf dup2 = retainedDuplicate2 ? dup1.retainedDuplicate() : dup1.duplicate().retain();
    assertEquals(0, dup2.compareTo(expected));

    // Cleanup the expected buffers used for testing.
    assertTrue(expected.release());

    // The handler created a slice of the slice and is now done with it.
    dup2.release();

    // The handler is now done with the original slice
    assertTrue(dup1.release());

    // Reference counting may be shared, or may be independently tracked, but at this point all buffers should
    // be deallocated and have a reference count of 0.
    assertEquals(0, buf.refCnt());
    assertEquals(0, dup1.refCnt());
    assertEquals(0, dup2.refCnt());
  }

  private void testMultipleRetainedSliceReleaseOriginal(boolean retainedSlice1, boolean retainedSlice2) {
    ByteBuf buf = newBuffer(8).resetWriterIndex();
    ByteBuf expected1 = newBuffer(3).resetWriterIndex();
    ByteBuf expected2 = newBuffer(2).resetWriterIndex();
    ByteBuf expected3 = newBuffer(2).resetWriterIndex();
    buf.writeBytes(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });
    expected1.writeBytes(new byte[] { 6, 7, 8 });
    expected2.writeBytes(new byte[] { 7, 8 });
    expected3.writeBytes(new byte[] { 6, 7 });
    ByteBuf slice1 =
        retainedSlice1 ? buf.retainedSlice(buf.readerIndex() + 5, 3) : buf.slice(buf.readerIndex() + 5, 3).retain();
    assertEquals(0, slice1.compareTo(expected1));
    // Simulate a handler that releases the original buffer, and propagates a slice.
    buf.release();

    ByteBuf slice2 = retainedSlice2
        ? slice1.retainedSlice(slice1.readerIndex() + 1, 2)
        : slice1.slice(slice1.readerIndex() + 1, 2).retain();
    assertEquals(0, slice2.compareTo(expected2));

    // The handler created a slice of the slice and is now done with it.
    slice2.release();

    ByteBuf slice3 = slice1.retainedSlice(slice1.readerIndex(), 2);
    assertEquals(0, slice3.compareTo(expected3));

    // The handler created another slice of the slice and is now done with it.
    slice3.release();

    // The handler is now done with the original slice
    assertTrue(slice1.release());

    // Cleanup the expected buffers used for testing.
    assertTrue(expected1.release());
    assertTrue(expected2.release());
    assertTrue(expected3.release());

    // Reference counting may be shared, or may be independently tracked, but at this point all buffers should
    // be deallocated and have a reference count of 0.
    assertEquals(0, buf.refCnt());
    assertEquals(0, slice1.refCnt());
    assertEquals(0, slice2.refCnt());
    assertEquals(0, slice3.refCnt());
  }

  private void testMultipleRetainedDuplicateReleaseOriginal(boolean retainedDuplicate1, boolean retainedDuplicate2) {
    ByteBuf buf = newBuffer(8).resetWriterIndex();
    ByteBuf expected = newBuffer(8).resetWriterIndex();
    buf.writeBytes(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });
    expected.writeBytes(buf, buf.readerIndex(), buf.readableBytes());
    ByteBuf dup1 = retainedDuplicate1 ? buf.retainedDuplicate() : buf.duplicate().retain();
    assertEquals(0, dup1.compareTo(expected));
    // Simulate a handler that releases the original buffer, and propagates a slice.
    buf.release();

    ByteBuf dup2 = retainedDuplicate2 ? dup1.retainedDuplicate() : dup1.duplicate().retain();
    assertEquals(0, dup2.compareTo(expected));
    assertEquals(0, dup2.compareTo(dup2.duplicate()));
    assertEquals(0, dup2.compareTo(dup2.slice()));

    ByteBuf tmpBuf = dup2.retainedDuplicate();
    assertEquals(0, dup2.compareTo(tmpBuf));
    tmpBuf.release();
    tmpBuf = dup2.retainedSlice();
    assertEquals(0, dup2.compareTo(tmpBuf));
    tmpBuf.release();

    // The handler created a slice of the slice and is now done with it.
    dup2.release();

    ByteBuf dup3 = dup1.retainedDuplicate();
    assertEquals(0, dup3.compareTo(expected));

    // The handler created another slice of the slice and is now done with it.
    dup3.release();

    // The handler is now done with the original slice
    assertTrue(dup1.release());

    // Cleanup the expected buffers used for testing.
    assertTrue(expected.release());

    // Reference counting may be shared, or may be independently tracked, but at this point all buffers should
    // be deallocated and have a reference count of 0.
    assertEquals(0, buf.refCnt());
    assertEquals(0, dup1.refCnt());
    assertEquals(0, dup2.refCnt());
    assertEquals(0, dup3.refCnt());
  }

  private void testDuplicateContents(boolean retainedDuplicate) {
    ByteBuf buf = newBuffer(8).resetWriterIndex();
    buf.writeBytes(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });
    ByteBuf dup = retainedDuplicate ? buf.retainedDuplicate() : buf.duplicate();
    try {
      assertEquals(0, dup.compareTo(buf));
      assertEquals(0, dup.compareTo(dup.duplicate()));
      ByteBuf b = dup.retainedDuplicate();
      assertEquals(0, dup.compareTo(b));
      b.release();
      assertEquals(0, dup.compareTo(dup.slice(dup.readerIndex(), dup.readableBytes())));
    } finally {
      if (retainedDuplicate) {
        dup.release();
      }
      buf.release();
    }
  }

  @Test(groups = "unit")
  public void testDuplicateRelease() {
    ByteBuf buf = newBuffer(8);
    assertEquals(1, buf.refCnt());
    assertTrue(buf.duplicate().release());
    assertEquals(0, buf.refCnt());
  }

  // Test-case trying to reproduce:
  // https://github.com/netty/netty/issues/2843
  @Test(groups = "unit")
  public void testRefCnt() throws Exception {
    testRefCnt0(false);
  }

  // Test-case trying to reproduce:
  // https://github.com/netty/netty/issues/2843
  @Test(groups = "unit")
  public void testRefCnt2() throws Exception {
    testRefCnt0(true);
  }

  @Test(groups = "unit")
  public void testEmptyNioBuffers() throws Exception {
    ByteBuf buffer = newBuffer(8);
    buffer.clear();
    assertFalse(buffer.isReadable());
    ByteBuffer[] nioBuffers = buffer.nioBuffers();
    assertEquals(1, nioBuffers.length);
    assertFalse(nioBuffers[0].hasRemaining());
    buffer.release();
  }

  @Test(groups = "unit")
  public void testGetReadOnlyDirectDst() {
    testGetReadOnlyDst(true);
  }

  @Test(groups = "unit")
  public void testGetReadOnlyHeapDst() {
    testGetReadOnlyDst(false);
  }

  private void testGetReadOnlyDst(boolean direct) {
    byte[] bytes = { 'a', 'b', 'c', 'd' };

    ByteBuf buffer = newBuffer(bytes.length);
    buffer.writeBytes(bytes);

    ByteBuffer dst = direct ? ByteBuffer.allocateDirect(bytes.length) : ByteBuffer.allocate(bytes.length);
    ByteBuffer readOnlyDst = dst.asReadOnlyBuffer();
    try {
      buffer.getBytes(0, readOnlyDst);
      fail();
    } catch (ReadOnlyBufferException e) {
      // expected
    }
    assertEquals(0, readOnlyDst.position());
    buffer.release();
  }

  @Test(groups = "unit")
  public void testReadBytesAndWriteBytesWithFileChannel() throws IOException {
    File file = File.createTempFile("file-channel", ".tmp");
    RandomAccessFile randomAccessFile = null;
    try {
      randomAccessFile = new RandomAccessFile(file, "rw");
      FileChannel channel = randomAccessFile.getChannel();
      // channelPosition should never be changed
      long channelPosition = channel.position();

      byte[] bytes = { 'a', 'b', 'c', 'd' };
      int len = bytes.length;
      ByteBuf buffer = newBuffer(len);
      buffer.resetReaderIndex();
      buffer.resetWriterIndex();
      buffer.writeBytes(bytes);

      int oldReaderIndex = buffer.readerIndex();
      assertEquals(len, buffer.readBytes(channel, 10, len));
      assertEquals(oldReaderIndex + len, buffer.readerIndex());
      assertEquals(channelPosition, channel.position());

      ByteBuf buffer2 = newBuffer(len);
      buffer2.resetReaderIndex();
      buffer2.resetWriterIndex();
      int oldWriterIndex = buffer2.writerIndex();
      assertEquals(len, buffer2.writeBytes(channel, 10, len));
      assertEquals(channelPosition, channel.position());
      assertEquals(oldWriterIndex + len, buffer2.writerIndex());
      assertEquals('a', buffer2.getByte(0));
      assertEquals('b', buffer2.getByte(1));
      assertEquals('c', buffer2.getByte(2));
      assertEquals('d', buffer2.getByte(3));
      buffer.release();
      buffer2.release();
    } finally {
      if (randomAccessFile != null) {
        randomAccessFile.close();
      }
      file.delete();
    }
  }

  @Test(groups = "unit")
  public void testGetBytesAndSetBytesWithFileChannel() throws IOException {
    File file = File.createTempFile("file-channel", ".tmp");
    RandomAccessFile randomAccessFile = null;
    try {
      randomAccessFile = new RandomAccessFile(file, "rw");
      FileChannel channel = randomAccessFile.getChannel();
      // channelPosition should never be changed
      long channelPosition = channel.position();

      byte[] bytes = { 'a', 'b', 'c', 'd' };
      int len = bytes.length;
      ByteBuf buffer = newBuffer(len);
      buffer.resetReaderIndex();
      buffer.resetWriterIndex();
      buffer.writeBytes(bytes);

      int oldReaderIndex = buffer.readerIndex();
      assertEquals(len, buffer.getBytes(oldReaderIndex, channel, 10, len));
      assertEquals(oldReaderIndex, buffer.readerIndex());
      assertEquals(channelPosition, channel.position());

      ByteBuf buffer2 = newBuffer(len);
      buffer2.resetReaderIndex();
      buffer2.resetWriterIndex();
      int oldWriterIndex = buffer2.writerIndex();
      assertEquals(buffer2.setBytes(oldWriterIndex, channel, 10, len), len);
      assertEquals(channelPosition, channel.position());

      assertEquals(oldWriterIndex, buffer2.writerIndex());
      assertEquals('a', buffer2.getByte(oldWriterIndex));
      assertEquals('b', buffer2.getByte(oldWriterIndex + 1));
      assertEquals('c', buffer2.getByte(oldWriterIndex + 2));
      assertEquals('d', buffer2.getByte(oldWriterIndex + 3));

      buffer.release();
      buffer2.release();
    } finally {
      if (randomAccessFile != null) {
        randomAccessFile.close();
      }
      file.delete();
    }
  }

  @Test(groups = "unit")
  public void testReadBytes() {
    ByteBuf buffer = newBuffer(8);
    byte[] bytes = new byte[8];
    buffer.writeBytes(bytes);

    ByteBuf buffer2 = buffer.readBytes(4);
    assertSame(buffer.alloc(), buffer2.alloc());
    assertEquals(4, buffer.readerIndex());
    assertTrue(buffer.release());
    assertEquals(0, buffer.refCnt());
    assertTrue(buffer2.release());
    assertEquals(0, buffer2.refCnt());
  }

  @Test(groups = "unit")
  public void testForEachByteDesc2() {
    byte[] expected = { 1, 2, 3, 4 };
    ByteBuf buf = newBuffer(expected.length);
    try {
      buf.writeBytes(expected);
      final byte[] bytes = new byte[expected.length];
      int i = buf.forEachByteDesc(new ByteProcessor() {
        private int index = bytes.length - 1;

        @Override
        public boolean process(byte value) throws Exception {
          bytes[index--] = value;
          return true;
        }
      });
      assertEquals(-1, i);
      assertEquals(expected, bytes);
    } finally {
      buf.release();
    }
  }

  @Test(groups = "unit")
  public void testForEachByte2() {
    byte[] expected = { 1, 2, 3, 4 };
    ByteBuf buf = newBuffer(expected.length);
    try {
      buf.writeBytes(expected);
      final byte[] bytes = new byte[expected.length];
      int i = buf.forEachByte(new ByteProcessor() {
        private int index;

        @Override
        public boolean process(byte value) throws Exception {
          bytes[index++] = value;
          return true;
        }
      });
      assertEquals(-1, i);
      assertEquals(expected, bytes);
    } finally {
      buf.release();
    }
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void testGetBytesByteBuffer() {
    byte[] bytes = { 'a', 'b', 'c', 'd', 'e', 'f', 'g' };
    // Ensure destination buffer is bigger then what is in the ByteBuf.
    ByteBuffer nioBuffer = ByteBuffer.allocate(bytes.length + 1);
    ByteBuf buffer = newBuffer(bytes.length);
    try {
      buffer.writeBytes(bytes);
      buffer.getBytes(buffer.readerIndex(), nioBuffer);
    } finally {
      buffer.release();
    }
  }

  private void testRefCnt0(final boolean parameter) throws Exception {
    for (int i = 0; i < 10; i++) {
      final CountDownLatch latch = new CountDownLatch(1);
      final CountDownLatch innerLatch = new CountDownLatch(1);

      final ByteBuf buffer = newBuffer(4);
      assertEquals(1, buffer.refCnt());
      final AtomicInteger cnt = new AtomicInteger(Integer.MAX_VALUE);
      Thread t1 = new Thread(new Runnable() {
        @Override
        public void run() {
          boolean released;
          if (parameter) {
            released = buffer.release(buffer.refCnt());
          } else {
            released = buffer.release();
          }
          assertTrue(released);
          Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
              cnt.set(buffer.refCnt());
              latch.countDown();
            }
          });
          t2.start();
          try {
            // Keep Thread alive a bit so the ThreadLocal caches are not freed
            innerLatch.await();
          } catch (InterruptedException ignore) {
            // ignore
          }
        }
      });
      t1.start();

      latch.await();
      assertEquals(0, cnt.get());
      innerLatch.countDown();
    }
  }

  static final class TestGatheringByteChannel implements GatheringByteChannel {
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final WritableByteChannel channel = Channels.newChannel(out);
    private final int limit;

    TestGatheringByteChannel(int limit) {
      this.limit = limit;
    }

    TestGatheringByteChannel() {
      this(Integer.MAX_VALUE);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
      long written = 0;
      for (; offset < length; offset++) {
        written += write(srcs[offset]);
        if (written >= limit) {
          break;
        }
      }
      return written;
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
      return write(srcs, 0, srcs.length);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
      int oldLimit = src.limit();
      if (limit < src.remaining()) {
        src.limit(src.position() + limit);
      }
      int w = channel.write(src);
      src.limit(oldLimit);
      return w;
    }

    @Override
    public boolean isOpen() {
      return channel.isOpen();
    }

    @Override
    public void close() throws IOException {
      channel.close();
    }

    public byte[] writtenBytes() {
      return out.toByteArray();
    }
  }

  private static final class DevNullGatheringByteChannel implements GatheringByteChannel {
    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long write(ByteBuffer[] srcs) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int write(ByteBuffer src) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isOpen() {
      return false;
    }

    @Override
    public void close() {
      throw new UnsupportedOperationException();
    }
  }

  private static final class TestScatteringByteChannel implements ScatteringByteChannel {
    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long read(ByteBuffer[] dsts) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int read(ByteBuffer dst) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isOpen() {
      return false;
    }

    @Override
    public void close() {
      throw new UnsupportedOperationException();
    }
  }

  private static final class TestByteProcessor implements ByteProcessor {
    @Override
    public boolean process(byte value) throws Exception {
      return true;
    }
  }

  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
  public void testCapacityEnforceMaxCapacity() {
    ByteBuf buffer = newBuffer(3, 13);
    assertEquals(13, buffer.maxCapacity());
    assertEquals(3, buffer.capacity());
    try {
      buffer.capacity(14);
    } finally {
      buffer.release();
    }
  }

  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
  public void testCapacityNegative() {
    ByteBuf buffer = newBuffer(3, 13);
    assertEquals(13, buffer.maxCapacity());
    assertEquals(3, buffer.capacity());
    try {
      buffer.capacity(-1);
    } finally {
      buffer.release();
    }
  }

  @Test(groups = "unit")
  public void testCapacityDecrease() {
    ByteBuf buffer = newBuffer(3, 13);
    assertEquals(13, buffer.maxCapacity());
    assertEquals(3, buffer.capacity());
    try {
      buffer.capacity(2);
      assertEquals(2, buffer.capacity());
      assertEquals(13, buffer.maxCapacity());
    } finally {
      buffer.release();
    }
  }

  @Test(groups = "unit")
  public void testCapacityIncrease() {
    ByteBuf buffer = newBuffer(3, 13);
    assertEquals(13, buffer.maxCapacity());
    assertEquals(3, buffer.capacity());
    try {
      buffer.capacity(4);
      assertEquals(4, buffer.capacity());
      assertEquals(13, buffer.maxCapacity());
    } finally {
      buffer.release();
    }
  }

  @Test(groups = "unit", expectedExceptions = IndexOutOfBoundsException.class)
  public void testReaderIndexLargerThanWriterIndex() {
    String content1 = "hello";
    String content2 = "world";
    int length = content1.length() + content2.length();
    ByteBuf buffer = newBuffer(length);
    buffer.setIndex(0, 0);
    buffer.writeCharSequence(content1, CharsetUtil.US_ASCII);
    buffer.markWriterIndex();
    buffer.skipBytes(content1.length());
    buffer.writeCharSequence(content2, CharsetUtil.US_ASCII);
    buffer.skipBytes(content2.length());
    assertTrue(buffer.readerIndex() <= buffer.writerIndex());

    try {
      buffer.resetWriterIndex();
    } finally {
      buffer.release();
    }
  }
}
