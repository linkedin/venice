package com.linkedin.venice.io;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import org.testng.annotations.Test;


public class ZeroCopyByteArrayOutputStreamTest {
  @Test
  public void testBackingArrayRemainsUnchanged() {
    ZeroCopyByteArrayOutputStream zos = new ZeroCopyByteArrayOutputStream(10);

    // since this is a zero copy stream, the size should be 10, i.e., the same as that of the backing array
    assertEquals(zos.toByteArray().length, 10);
    assertEquals(zos.size(), 0);
    assertSame(zos.toByteBuffer().array(), zos.toByteArray());

    zos.write('A');
    assertEquals(zos.toByteArray().length, 10);
    assertEquals(zos.size(), 1);
    assertSame(zos.toByteBuffer().array(), zos.toByteArray());

    zos.write(98);
    assertEquals(zos.toByteArray().length, 10);
    assertEquals(zos.size(), 2);
    assertSame(zos.toByteBuffer().array(), zos.toByteArray());
  }
}
