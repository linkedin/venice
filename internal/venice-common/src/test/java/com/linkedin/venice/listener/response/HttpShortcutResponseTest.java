package com.linkedin.venice.listener.response;

import static org.testng.Assert.assertEquals;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.testng.annotations.Test;


public class HttpShortcutResponseTest {
  @Test
  public void testHttpShortcutResponse() {
    // Case 1: Test constructor with both message and status
    HttpShortcutResponse responseWithMessage = new HttpShortcutResponse("Success", HttpResponseStatus.OK);
    assertEquals(responseWithMessage.getMessage(), "Success", "Message should be 'Success'");
    assertEquals(responseWithMessage.getStatus(), HttpResponseStatus.OK, "Status should be OK");

    // Case 2: Test constructor with empty message and status
    HttpShortcutResponse responseWithEmptyMessage = new HttpShortcutResponse("", HttpResponseStatus.BAD_REQUEST);
    assertEquals(responseWithEmptyMessage.getMessage(), "", "Message should be empty");
    assertEquals(responseWithEmptyMessage.getStatus(), HttpResponseStatus.BAD_REQUEST, "Status should be BAD_REQUEST");

    // Case 3: Test constructor with only status (default message)
    HttpShortcutResponse responseWithoutMessage = new HttpShortcutResponse(HttpResponseStatus.NOT_FOUND);
    assertEquals(responseWithoutMessage.getMessage(), "", "Message should be empty by default");
    assertEquals(responseWithoutMessage.getStatus(), HttpResponseStatus.NOT_FOUND, "Status should be NOT_FOUND");
  }
}
