package com.linkedin.venice.utils;

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
}
