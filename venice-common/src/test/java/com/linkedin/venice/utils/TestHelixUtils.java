package com.linkedin.venice.utils;

import com.linkedin.venice.meta.Instance;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestHelixUtils {
  @Test
  public void parsesHostnameFromInstanceName() {
    Instance instance1 = HelixUtils.getInstanceFromHelixInstanceName("host_1234");
    Assert.assertEquals(instance1.getHost(), "host");
    Assert.assertEquals(instance1.getPort(), 1234);

    Instance instance2 = HelixUtils.getInstanceFromHelixInstanceName("host_name_5678");
    Assert.assertEquals(instance2.getHost(), "host_name");
    Assert.assertEquals(instance2.getPort(), 5678);
  }
}
