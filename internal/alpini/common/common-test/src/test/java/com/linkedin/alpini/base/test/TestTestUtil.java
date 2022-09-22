package com.linkedin.alpini.base.test;

import com.linkedin.alpini.base.misc.Time;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestTestUtil {
  @Test
  public void basicTest() {
    long time = Time.nanoTime();
    TestUtil.waitFor(() -> false, 1, TimeUnit.SECONDS);
    Assert.assertEquals((Time.nanoTime() - time) / 1000000000.0, 1, 0.1);
    time = Time.nanoTime();
    TestUtil.waitFor(() -> true, 1, TimeUnit.SECONDS);
    Assert.assertEquals((Time.nanoTime() - time) / 1000000000.0, 0, 0.1);
  }
}
