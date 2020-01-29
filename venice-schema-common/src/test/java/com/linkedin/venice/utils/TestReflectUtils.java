package com.linkedin.venice.utils;

import com.google.common.collect.Sets;
import com.linkedin.venice.security.DefaultSSLFactory;
import com.linkedin.venice.security.SSLFactory;
import java.util.HashSet;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestReflectUtils {
  @Test
  public void getSubclassNamesTest() {
    Assert.assertEquals(
        new HashSet<>(ReflectUtils.getSubclassNames(SSLFactory.class)),
        Sets.newHashSet(DefaultSSLFactory.class.getName())
    );
  }
}
