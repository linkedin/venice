package com.linkedin.venice.exceptions;

import org.apache.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ConcurrentBatchPushExceptionTest {
  @Test
  public void testException() {
    String message = "Unable to start the push, there is already a future version for the store";
    ConcurrentBatchPushException e = new ConcurrentBatchPushException(message);

    Assert.assertEquals(e.getMessage(), message);
    Assert.assertEquals(e.getErrorType(), ErrorType.CONCURRENT_BATCH_PUSH);
    Assert.assertEquals(
        e.getErrorType().getExceptionType(),
        ExceptionType.BAD_REQUEST,
        "CONCURRENT_BATCH_PUSH should map to a BAD_REQUEST exception type");
    Assert.assertEquals(
        e.getHttpStatusCode(),
        HttpStatus.SC_BAD_REQUEST,
        "Concurrent batch push is a client-side conflict and must surface as HTTP 400, not 500");
  }
}
