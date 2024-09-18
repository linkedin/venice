package com.linkedin.venice.utils;

import static com.linkedin.venice.HttpConstants.VENICE_RETRY;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import org.testng.annotations.Test;


public class NettyUtilsTest {
  @Test
  public void testContainRetryHeader() {
    // 1. Header is present
    HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test");
    httpRequest.headers().set(VENICE_RETRY, "1");
    assertTrue(NettyUtils.containRetryHeader(httpRequest), "Request should contain the retry header");

    // 2. Header is present with value "true"
    httpRequest.headers().clear();
    httpRequest.headers().set(VENICE_RETRY, "true");
    assertTrue(NettyUtils.containRetryHeader(httpRequest), "Request should contain the retry header");

    // 3. Header is not present
    httpRequest.headers().clear();
    assertFalse(NettyUtils.containRetryHeader(httpRequest), "Request should not contain the retry header");
  }
}
