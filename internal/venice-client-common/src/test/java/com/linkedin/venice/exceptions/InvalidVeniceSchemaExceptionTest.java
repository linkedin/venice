package com.linkedin.venice.exceptions;

import org.apache.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.Test;


public class InvalidVeniceSchemaExceptionTest {
  @Test
  public void testException() {
    InvalidVeniceSchemaException e = new InvalidVeniceSchemaException("Invalid schema supplied");

    Assert.assertEquals(e.getErrorType(), ErrorType.INVALID_SCHEMA);
    Assert.assertEquals(
        e.getHttpStatusCode(),
        HttpStatus.SC_BAD_REQUEST,
        "An invalid client-supplied schema must surface as HTTP 400, not 500");

    InvalidVeniceSchemaException notFound = new InvalidVeniceSchemaException("test_store", "5");
    Assert.assertEquals(notFound.getErrorType(), ErrorType.INVALID_SCHEMA);
    Assert.assertEquals(notFound.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
  }
}
