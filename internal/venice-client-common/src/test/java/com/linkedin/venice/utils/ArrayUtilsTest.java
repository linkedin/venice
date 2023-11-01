package com.linkedin.venice.utils;

import org.testng.Assert;
import org.testng.annotations.Test;


public class ArrayUtilsTest {
  @Test
  public void testCompareUnsignedSameArrays() {
    byte[] a = new byte[] { 1, 2, 3 };
    Assert.assertEquals(ArrayUtils.compareUnsigned(a, a), 0);
  }

  @Test
  public void testCompareUnsignedIdenticalArrays() {
    byte[] a = new byte[] { 1, 2, 3 };
    byte[] b = new byte[] { 1, 2, 3 };
    Assert.assertEquals(ArrayUtils.compareUnsigned(a, b), 0);
  }

  @Test
  public void testCompareUnsignedFirstLarger() {
    byte[] a = new byte[] { 1, 2, 3 };
    byte[] b = new byte[] { 1, 2 };
    Assert.assertEquals(ArrayUtils.compareUnsigned(a, b), 1);

    byte[] c = new byte[] { 1, 4 };
    byte[] d = new byte[] { 1, 2, 3 };
    Assert.assertEquals(ArrayUtils.compareUnsigned(c, d), 2);
  }

  @Test
  public void testCompareUnsignedHonorsUnsigned() {
    byte[] a = new byte[] { 1, -100 }; // For unsigned comparison, this is 156. For signed comparison, this is -100.
    byte[] b = new byte[] { 1, 2 };
    Assert.assertEquals(ArrayUtils.compareUnsigned(a, b), 154);
  }

  @Test
  public void testCompareUnsignedSecondLarger() {
    byte[] a = new byte[] { 1, 2 };
    byte[] b = new byte[] { 1, 2, 3 };
    Assert.assertEquals(ArrayUtils.compareUnsigned(a, b), -1);

    byte[] c = new byte[] { 1, 2, 3 };
    byte[] d = new byte[] { 1, 4 };
    Assert.assertEquals(ArrayUtils.compareUnsigned(c, d), -2);
  }

  @Test
  public void testCompareUnsignedNullArrays() {
    byte[] a = new byte[] { 1, 2, 3 };

    Assert.assertEquals(ArrayUtils.compareUnsigned(a, null), 1);

    Assert.assertEquals(ArrayUtils.compareUnsigned(null, a), -1);

    Assert.assertEquals(ArrayUtils.compareUnsigned(null, null), 0);
  }
}
