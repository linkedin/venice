package com.linkedin.venice.response;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.testng.annotations.Test;


public class VeniceReadResponseStatusTest {
  @Test
  public void testEnumValues() {
    assertEquals(VeniceReadResponseStatus.KEY_NOT_FOUND.getHttpResponseStatus(), HttpResponseStatus.NOT_FOUND);
    assertEquals(VeniceReadResponseStatus.OK.getHttpResponseStatus(), HttpResponseStatus.OK);
    assertEquals(VeniceReadResponseStatus.BAD_REQUEST.getHttpResponseStatus(), HttpResponseStatus.BAD_REQUEST);
    assertEquals(VeniceReadResponseStatus.FORBIDDEN.getHttpResponseStatus(), HttpResponseStatus.FORBIDDEN);
    assertEquals(
        VeniceReadResponseStatus.METHOD_NOT_ALLOWED.getHttpResponseStatus(),
        HttpResponseStatus.METHOD_NOT_ALLOWED);
    assertEquals(VeniceReadResponseStatus.REQUEST_TIMEOUT.getHttpResponseStatus(), HttpResponseStatus.REQUEST_TIMEOUT);
    assertEquals(
        VeniceReadResponseStatus.TOO_MANY_REQUESTS.getHttpResponseStatus(),
        HttpResponseStatus.TOO_MANY_REQUESTS);
    assertEquals(
        VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getHttpResponseStatus(),
        HttpResponseStatus.INTERNAL_SERVER_ERROR);
    assertEquals(
        VeniceReadResponseStatus.SERVICE_UNAVAILABLE.getHttpResponseStatus(),
        HttpResponseStatus.SERVICE_UNAVAILABLE);
    assertEquals(VeniceReadResponseStatus.MISROUTED_STORE_VERSION.getHttpResponseStatus().code(), 570);
  }

  @Test
  public void testGetCode() {
    assertEquals(VeniceReadResponseStatus.KEY_NOT_FOUND.getCode(), HttpResponseStatus.NOT_FOUND.code());
    assertEquals(VeniceReadResponseStatus.OK.getCode(), HttpResponseStatus.OK.code());
    assertEquals(VeniceReadResponseStatus.BAD_REQUEST.getCode(), HttpResponseStatus.BAD_REQUEST.code());
    assertEquals(VeniceReadResponseStatus.FORBIDDEN.getCode(), HttpResponseStatus.FORBIDDEN.code());
    assertEquals(VeniceReadResponseStatus.METHOD_NOT_ALLOWED.getCode(), HttpResponseStatus.METHOD_NOT_ALLOWED.code());
    assertEquals(VeniceReadResponseStatus.REQUEST_TIMEOUT.getCode(), HttpResponseStatus.REQUEST_TIMEOUT.code());
    assertEquals(VeniceReadResponseStatus.TOO_MANY_REQUESTS.getCode(), HttpResponseStatus.TOO_MANY_REQUESTS.code());
    assertEquals(
        VeniceReadResponseStatus.INTERNAL_SERVER_ERROR.getCode(),
        HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
    assertEquals(VeniceReadResponseStatus.SERVICE_UNAVAILABLE.getCode(), HttpResponseStatus.SERVICE_UNAVAILABLE.code());
    assertEquals(VeniceReadResponseStatus.MISROUTED_STORE_VERSION.getCode(), 570);
  }

  @Test
  public void testFromCode() {
    assertEquals(
        VeniceReadResponseStatus.fromCode(HttpResponseStatus.NOT_FOUND.code()),
        VeniceReadResponseStatus.KEY_NOT_FOUND);
    assertEquals(VeniceReadResponseStatus.fromCode(HttpResponseStatus.OK.code()), VeniceReadResponseStatus.OK);
    assertEquals(
        VeniceReadResponseStatus.fromCode(HttpResponseStatus.BAD_REQUEST.code()),
        VeniceReadResponseStatus.BAD_REQUEST);
    assertEquals(
        VeniceReadResponseStatus.fromCode(HttpResponseStatus.FORBIDDEN.code()),
        VeniceReadResponseStatus.FORBIDDEN);
    assertEquals(
        VeniceReadResponseStatus.fromCode(HttpResponseStatus.METHOD_NOT_ALLOWED.code()),
        VeniceReadResponseStatus.METHOD_NOT_ALLOWED);
    assertEquals(
        VeniceReadResponseStatus.fromCode(HttpResponseStatus.REQUEST_TIMEOUT.code()),
        VeniceReadResponseStatus.REQUEST_TIMEOUT);
    assertEquals(
        VeniceReadResponseStatus.fromCode(HttpResponseStatus.TOO_MANY_REQUESTS.code()),
        VeniceReadResponseStatus.TOO_MANY_REQUESTS);
    assertEquals(
        VeniceReadResponseStatus.fromCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()),
        VeniceReadResponseStatus.INTERNAL_SERVER_ERROR);
    assertEquals(
        VeniceReadResponseStatus.fromCode(HttpResponseStatus.SERVICE_UNAVAILABLE.code()),
        VeniceReadResponseStatus.SERVICE_UNAVAILABLE);
    assertEquals(VeniceReadResponseStatus.fromCode(570), VeniceReadResponseStatus.MISROUTED_STORE_VERSION);
  }

  @Test
  public void testFromCodeForInvalidCode() {
    Exception exception = expectThrows(IllegalArgumentException.class, () -> VeniceReadResponseStatus.fromCode(999));
    assertEquals(exception.getMessage(), "Unknown status venice read response status code: 999");
  }
}
