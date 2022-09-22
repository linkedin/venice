package com.linkedin.alpini.consts;

import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 12/6/17.
 */
public class TestQOS {
  @Test(groups = "unit")
  public void testQOS() {
    Assert.assertEquals(Arrays.asList(QOS.values()), Arrays.asList(QOS.LOW, QOS.NORMAL, QOS.HIGH));

    Assert.assertSame(QOS.valueOf("LOW"), QOS.LOW);
  }
}
