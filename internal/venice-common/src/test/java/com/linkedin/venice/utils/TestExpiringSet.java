package com.linkedin.venice.utils;

import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by mwise on 4/12/16.
 *
 * TODO: This test is flaky. We need to make it deterministic using the {@link Time} interface.
 */
public class TestExpiringSet {
  @Test
  public void expiringSetExpires() throws InterruptedException {
    long ttl = 5; /* milliseconds */
    String key = "myKey";
    ExpiringSet<String> mySet = new ExpiringSet<>(ttl, TimeUnit.MILLISECONDS);

    Assert.assertFalse(mySet.contains(key), "ExpiringSet should not contain a key that has not been added");
    mySet.add(key);
    Assert.assertTrue(mySet.contains(key), "ExpiringSet should contain a key that was just added");
    Assert.assertFalse(mySet.contains(key + "123"), "ExpiringSet should not contain a key that has not been added");
    Thread.sleep(ttl + 1);
    Assert.assertFalse(mySet.contains(key), "ExpiringSet should not contain an expired item");
    // Asserting twice because .contains operation will null out expired elements
    Assert.assertFalse(mySet.contains(key), "ExpiringSet should not contain an expired item");
    Assert.assertFalse(mySet.contains(key + "123"), "ExpiringSet should not contain a key that has not been added");
  }
}
