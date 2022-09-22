package com.linkedin.alpini.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 1/24/18.
 */
public class TestZeroInputStream {
  @Test(groups = "unit")
  public void testBasic() throws IOException {
    ZeroInputStream is = new ZeroInputStream(11235);
    ByteArrayOutputStream os = new ByteArrayOutputStream();

    Assert.assertEquals(is.getBytesRemaining(), 11235L);
    Assert.assertEquals(is.available(), 11235);

    Assert.assertEquals(is.read(), 0);

    IOUtils.copy(is, os);
    byte[] bytes = os.toByteArray();

    Assert.assertEquals(bytes.length, 11234);
    Assert.assertEquals(is.getBytesRemaining(), 0L);
    Assert.assertEquals(is.available(), 0);
    Assert.assertEquals(is.read(), -1);

    for (byte b: bytes) {
      Assert.assertEquals(b, 0);
    }
  }

  @Test(groups = "unit")
  public void testSkip() throws IOException {
    long big = Integer.MAX_VALUE + 11235L;

    ZeroInputStream is = new ZeroInputStream(big);

    Assert.assertEquals(is.getBytesRemaining(), big);
    Assert.assertEquals(is.available(), Integer.MAX_VALUE);

    Assert.assertEquals(is.skip(Integer.MAX_VALUE / 2), Integer.MAX_VALUE / 2);

    Assert.assertEquals(is.getBytesRemaining(), big - Integer.MAX_VALUE / 2);
    Assert.assertEquals(is.available(), Integer.MAX_VALUE / 2 + 11235 + 1);

    Assert.assertEquals(is.skip(Integer.MAX_VALUE), Integer.MAX_VALUE / 2 + 11235 + 1);

    Assert.assertEquals(is.getBytesRemaining(), 0L);
    Assert.assertEquals(is.available(), 0);
  }

  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
  public void testBadSkip() throws IOException {

    ZeroInputStream is = new ZeroInputStream(1);

    is.skip(-1);
  }

  @Test(groups = "unit", expectedExceptions = ArrayIndexOutOfBoundsException.class)
  public void testBadRead1() throws IOException {

    ZeroInputStream is = new ZeroInputStream(1);

    is.read(new byte[1], -1, 0);
  }

  @Test(groups = "unit", expectedExceptions = ArrayIndexOutOfBoundsException.class)
  public void testBadRead2() throws IOException {

    ZeroInputStream is = new ZeroInputStream(1);

    is.read(new byte[1], 0, -1);
  }

}
