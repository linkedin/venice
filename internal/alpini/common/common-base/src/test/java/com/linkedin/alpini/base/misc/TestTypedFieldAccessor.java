package com.linkedin.alpini.base.misc;

import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = "unit")
public class TestTypedFieldAccessor {
  static class Simple {
    private byte[] _bytes;
  }

  public void simpleTest() {
    TypedFieldAccessor<Simple, byte[]> accessor = TypedFieldAccessor.forField(Simple.class, "_bytes");

    Simple s = new Simple();
    s._bytes = new byte[5];

    Assert.assertSame(accessor.apply(s), s._bytes);

    byte[] n = { 1, 2 };
    accessor.accept(s, n);

    Assert.assertSame(s._bytes, n);
  }

}
