package com.linkedin.alpini.io;

import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 1/24/18.
 */
public class TestNullOutputStream {
  @Test(groups = "unit")
  public void testBasic() throws IOException {
    NullOutputStream ns = new NullOutputStream();
    MeteredOutputStream ms = new MeteredOutputStream(ns);

    ms.write(1);
    Assert.assertEquals(ns.getBytesWritten(), 1);
    Assert.assertEquals(ms.getBytesWritten(), 1);

    ms.write(new byte[50]);
    Assert.assertEquals(ns.getBytesWritten(), 51);
    Assert.assertEquals(ms.getBytesWritten(), 51);

    ms.write(new byte[100], 0, 42);
    Assert.assertEquals(ns.getBytesWritten(), 93);
    Assert.assertEquals(ms.getBytesWritten(), 93);
  }
}
