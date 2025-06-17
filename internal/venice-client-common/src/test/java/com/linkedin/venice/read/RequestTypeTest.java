package com.linkedin.venice.read;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;


public class RequestTypeTest {
  @Test
  public void testBooleanFuncInRequestType() {
    // Test the boolean functions in RequestType enum.
    for (RequestType requestType: RequestType.values()) {
      if (requestType == RequestType.SINGLE_GET) {
        assertTrue(RequestType.isSingleGet(requestType), "SINGLE_GET should be recognized as a single GET");
      } else {
        assertFalse(RequestType.isSingleGet(requestType), requestType + " should not be recognized as a single GET");
      }

      if (requestType == RequestType.COMPUTE) {
        assertTrue(RequestType.isCompute(requestType), "COMPUTE should be recognized as a compute request");
      } else {
        assertFalse(RequestType.isCompute(requestType), requestType + " should not be recognized as a compute request");
      }

      if (requestType == RequestType.MULTI_GET_STREAMING || requestType == RequestType.COMPUTE_STREAMING) {
        assertTrue(RequestType.isStreaming(requestType), requestType + " should be recognized as a streaming request");
      } else {
        assertFalse(
            RequestType.isStreaming(requestType),
            requestType + " should not be recognized as a streaming request");
      }
    }
  }
}
