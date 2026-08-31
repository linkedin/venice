package com.linkedin.venice.exceptions;

import org.apache.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.Test;


public class VeniceStoreAclExceptionTest {
  @Test
  public void testException() {
    String message = "Missing write ACLs for the store";
    VeniceStoreAclException e = new VeniceStoreAclException(message);

    Assert.assertEquals(e.getMessage(), message);
    Assert.assertEquals(e.getErrorType(), ErrorType.ACL_ERROR);
    Assert.assertEquals(
        e.getHttpStatusCode(),
        HttpStatus.SC_FORBIDDEN,
        "An ACL/authorization failure must surface as HTTP 403, not 500");
  }
}
