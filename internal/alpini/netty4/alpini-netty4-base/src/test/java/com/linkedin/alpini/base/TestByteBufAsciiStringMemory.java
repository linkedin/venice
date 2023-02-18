package com.linkedin.alpini.base;

import com.linkedin.alpini.base.misc.ByteBufAsciiString;
import io.netty.util.ByteProcessor;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestByteBufAsciiStringMemory {
  private byte[] a;
  private byte[] b;
  private int aOffset = 22;
  private int bOffset = 53;
  private int length = 100;
  private ByteBufAsciiString aAsciiString;
  private ByteBufAsciiString bAsciiString;
  private Random r = new Random();

  @BeforeMethod(groups = "unit")
  public void setUp() {
    a = new byte[128];
    b = new byte[256];
    r.nextBytes(a);
    r.nextBytes(b);
    aOffset = 22;
    bOffset = 53;
    length = 100;
    System.arraycopy(a, aOffset, b, bOffset, length);
    aAsciiString = new ByteBufAsciiString(a, aOffset, length, false);
    bAsciiString = new ByteBufAsciiString(b, bOffset, length, false);
  }

  @Test(groups = "unit")
  public void testSharedMemory() {
    ++a[aOffset];
    ByteBufAsciiString aAsciiString1 = new ByteBufAsciiString(a, aOffset, length, true);
    ByteBufAsciiString aAsciiString2 = new ByteBufAsciiString(a, aOffset, length, false);
    Assert.assertEquals(aAsciiString1, aAsciiString);
    Assert.assertEquals(aAsciiString2, aAsciiString);
    for (int i = aOffset; i < length; ++i) {
      Assert.assertEquals(a[i], aAsciiString.byteAt(i - aOffset));
    }
  }

  @Test(groups = "unit")
  public void testNotSharedMemory() {
    ByteBufAsciiString aAsciiString1 = new ByteBufAsciiString(a, aOffset, length, true);
    ++a[aOffset];
    Assert.assertNotEquals(aAsciiString1, aAsciiString);
    int i = aOffset;
    Assert.assertNotEquals(aAsciiString1.byteAt(i - aOffset), a[i]);
    ++i;
    for (; i < length; ++i) {
      Assert.assertEquals(aAsciiString1.byteAt(i - aOffset), a[i]);
    }
  }

  @Test(groups = "unit")
  public void forEachTest() {
    final AtomicReference<Integer> aCount = new AtomicReference<Integer>(0);
    final AtomicReference<Integer> bCount = new AtomicReference<Integer>(0);
    aAsciiString.forEachByte(new ByteProcessor() {
      int i;

      @Override
      public boolean process(byte value) {
        Assert.assertEquals(bAsciiString.byteAt(i++), value, "failed at index: " + i);
        aCount.set(aCount.get() + 1);
        return true;
      }
    });
    bAsciiString.forEachByte(new ByteProcessor() {
      int i;

      @Override
      public boolean process(byte value) {
        Assert.assertEquals(aAsciiString.byteAt(i++), value, "failed at index: " + i);
        bCount.set(bCount.get() + 1);
        return true;
      }
    });
    Assert.assertEquals(aCount.get().intValue(), aAsciiString.length());
    Assert.assertEquals(bCount.get().intValue(), bAsciiString.length());
  }

  @Test(groups = "unit")
  public void forEachWithIndexEndTest() {
    Assert.assertNotEquals(
        aAsciiString.forEachByte(
            aAsciiString.length() - 1,
            1,
            new ByteProcessor.IndexOfProcessor(aAsciiString.byteAt(aAsciiString.length() - 1))),
        -1);
  }

  @Test(groups = "unit")
  public void forEachWithIndexBeginTest() {
    Assert.assertNotEquals(
        aAsciiString.forEachByte(0, 1, new ByteProcessor.IndexOfProcessor(aAsciiString.byteAt(0))),
        -1);
  }

  @Test(groups = "unit")
  public void forEachDescTest() {
    final AtomicReference<Integer> aCount = new AtomicReference<Integer>(0);
    final AtomicReference<Integer> bCount = new AtomicReference<Integer>(0);
    aAsciiString.forEachByteDesc(new ByteProcessor() {
      int i = 1;

      @Override
      public boolean process(byte value) throws Exception {
        Assert.assertEquals(bAsciiString.byteAt(bAsciiString.length() - (i++)), value, "failed at index: " + i);
        aCount.set(aCount.get() + 1);
        return true;
      }
    });
    bAsciiString.forEachByteDesc(new ByteProcessor() {
      int i = 1;

      @Override
      public boolean process(byte value) throws Exception {
        Assert.assertEquals(aAsciiString.byteAt(aAsciiString.length() - (i++)), value, "failed at index: " + i);
        bCount.set(bCount.get() + 1);
        return true;
      }
    });
    Assert.assertEquals(aCount.get().intValue(), aAsciiString.length());
    Assert.assertEquals(bCount.get().intValue(), bAsciiString.length());
  }

  @Test(groups = "unit")
  public void forEachDescWithIndexEndTest() {
    Assert.assertNotEquals(
        bAsciiString.forEachByteDesc(
            bAsciiString.length() - 1,
            1,
            new ByteProcessor.IndexOfProcessor(bAsciiString.byteAt(bAsciiString.length() - 1))),
        -1);
  }

  @Test(groups = "unit")
  public void forEachDescWithIndexBeginTest() {
    Assert.assertNotEquals(
        bAsciiString.forEachByteDesc(0, 1, new ByteProcessor.IndexOfProcessor(bAsciiString.byteAt(0))),
        -1);
  }

  @Test(groups = "unit")
  public void subSequenceTest() {
    final int start = 12;
    final int end = aAsciiString.length();
    ByteBufAsciiString aSubSequence = aAsciiString.subSequence(start, end, false);
    ByteBufAsciiString bSubSequence = bAsciiString.subSequence(start, end, true);
    Assert.assertEquals(aSubSequence, bSubSequence);
    Assert.assertEquals(aSubSequence.hashCode(), bSubSequence.hashCode());
  }

  /*@Test(groups = "unit")
  public void copyTest() {
    byte[] aCopy = new byte[aAsciiString.length()];
    aAsciiString.copy(0, aCopy, 0, aCopy.length);
    AsciiString aAsciiStringCopy = new AsciiString(aCopy, false);
    assertEquals(aAsciiString, aAsciiStringCopy);
  }*/
}
