package com.linkedin.venice.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ByteUtilsTest {
  @Test
  public void testHumanReadableByteCountBin() {
    Assert.assertEquals(ByteUtils.generateHumanReadableByteCountString(10), "10 B");
    Assert.assertEquals(ByteUtils.generateHumanReadableByteCountString(100), "100 B");
    Assert.assertEquals(ByteUtils.generateHumanReadableByteCountString(1000), "1000 B");
    Assert.assertEquals(ByteUtils.generateHumanReadableByteCountString(-1000), "-1000 B");

    Assert.assertEquals(ByteUtils.generateHumanReadableByteCountString(1024), "1.0 KiB");
    Assert.assertEquals(ByteUtils.generateHumanReadableByteCountString(1500), "1.5 KiB");
    Assert.assertEquals(ByteUtils.generateHumanReadableByteCountString(-1500), "-1.5 KiB");

    Assert.assertEquals(ByteUtils.generateHumanReadableByteCountString((long) Math.pow(1024, 2)), "1.0 MiB");
    Assert.assertEquals(ByteUtils.generateHumanReadableByteCountString((long) Math.pow(1024, 2) * 100), "100.0 MiB");
    Assert.assertEquals(ByteUtils.generateHumanReadableByteCountString((long) -Math.pow(1024, 2) * 100), "-100.0 MiB");

    Assert.assertEquals(ByteUtils.generateHumanReadableByteCountString((long) Math.pow(1024, 3)), "1.0 GiB");
    Assert.assertEquals(ByteUtils.generateHumanReadableByteCountString((long) Math.pow(1024, 3) * 100), "100.0 GiB");
    Assert.assertEquals(ByteUtils.generateHumanReadableByteCountString((long) -Math.pow(1024, 3) * 100), "-100.0 GiB");

    Assert.assertEquals(ByteUtils.generateHumanReadableByteCountString((long) Math.pow(1024, 4)), "1.0 TiB");
    Assert.assertEquals(ByteUtils.generateHumanReadableByteCountString((long) Math.pow(1024, 4) * 100), "100.0 TiB");
    Assert.assertEquals(ByteUtils.generateHumanReadableByteCountString((long) -Math.pow(1024, 4) * 100), "-100.0 TiB");

    Assert.assertEquals(ByteUtils.generateHumanReadableByteCountString((long) Math.pow(1024, 5)), "1.0 PiB");
    Assert.assertEquals(ByteUtils.generateHumanReadableByteCountString((long) Math.pow(1024, 5) * 100), "100.0 PiB");
    Assert.assertEquals(ByteUtils.generateHumanReadableByteCountString((long) -Math.pow(1024, 5) * 100), "-100.0 PiB");

    Assert.assertEquals(ByteUtils.generateHumanReadableByteCountString((long) Math.pow(1024, 6)), "1.0 EiB");
    Assert.assertEquals(ByteUtils.generateHumanReadableByteCountString(Long.MAX_VALUE), "8.0 EiB");
    Assert.assertEquals(ByteUtils.generateHumanReadableByteCountString(Long.MIN_VALUE), "-8.0 EiB");
  }

  @Test
  public void testCopyByteArray() {
    ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[0]);

    Assert.assertEquals(ByteUtils.copyByteArray(EMPTY_BYTE_BUFFER).length, 0);

    byte[] data = "111222333".getBytes();
    ByteBuffer nonEmptyByteBuffer = ByteBuffer.wrap(data, 3, 3);
    Assert.assertTrue(Arrays.equals(ByteUtils.copyByteArray(nonEmptyByteBuffer), "222".getBytes()));

    ByteBuffer directBuffer = ByteBuffer.allocateDirect(data.length);
    directBuffer.put(data);
    directBuffer.flip();
    Assert.assertTrue(Arrays.equals(ByteUtils.copyByteArray(directBuffer), data));
  }
}
