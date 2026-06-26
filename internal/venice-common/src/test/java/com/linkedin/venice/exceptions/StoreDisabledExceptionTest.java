package com.linkedin.venice.exceptions;

import org.apache.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.Test;


public class StoreDisabledExceptionTest {
  @Test
  public void testException() {
    StoreDisabledException e = new StoreDisabledException("test_store", "push");
    Assert.assertEquals(
        e.getHttpStatusCode(),
        HttpStatus.SC_FORBIDDEN,
        "Operating on an administratively disabled store must surface as HTTP 403, not 500");

    StoreDisabledException withVersion = new StoreDisabledException("test_store", "push", 3);
    Assert.assertEquals(withVersion.getHttpStatusCode(), HttpStatus.SC_FORBIDDEN);
  }
}
