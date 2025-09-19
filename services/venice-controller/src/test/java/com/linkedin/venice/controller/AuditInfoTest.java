package com.linkedin.venice.controller;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import spark.Request;


public class AuditInfoTest {
  private static final String TEST_URL = "http://localhost/test";
  private static final String METHOD_GET = "GET";
  private static final String CLIENT_IP = "127.0.0.1";
  private static final int CLIENT_PORT = 8080;
  private static final String PARAM_1 = "param1";
  private static final String PARAM_2 = "param2";
  private static final String VALUE_1 = "value1";
  private static final String VALUE_2 = "value2";
  private static final String ERROR_MESSAGE = "Some error";

  private Request request;
  private AuditInfo auditInfo;
  private HttpServletRequest httpServletRequest;

  @BeforeMethod
  public void setUp() {
    request = mock(Request.class);
    when(request.url()).thenReturn(TEST_URL);
    when(request.requestMethod()).thenReturn(METHOD_GET);
    when(request.ip()).thenReturn(CLIENT_IP);

    Set<String> queryParams = new HashSet<>();
    queryParams.add(PARAM_1);
    queryParams.add(PARAM_2);

    when(request.queryParams()).thenReturn(queryParams);
    when(request.queryParams(PARAM_1)).thenReturn(VALUE_1);
    when(request.queryParams(PARAM_2)).thenReturn(VALUE_2);

    httpServletRequest = mock(HttpServletRequest.class);
    when(httpServletRequest.getRemotePort()).thenReturn(CLIENT_PORT);
    when(request.raw()).thenReturn(httpServletRequest);

    auditInfo = new AuditInfo(request);
  }

  @Test
  public void testToStringReturnsExpectedFormat() {
    String result = auditInfo.toString();
    String expected = "[AUDIT] GET http://localhost/test {param1=value1, param2=value2} ClientIP: 127.0.0.1:8080";
    assertEquals(result, expected);
  }

  @Test
  public void testSuccessStringWithLatencyReturnsExpectedFormat() {
    long latency = 10000;
    String result = auditInfo.successString(latency);
    String expected =
        "[AUDIT] SUCCESS GET http://localhost/test {param1=value1, param2=value2} ClientIP: 127.0.0.1:8080 Latency: 10000 ms";
    assertEquals(result, expected);
  }

  @Test
  public void testFailureWithLatencyStringReturnsExpectedFormat() {
    long latency = 10000;
    String result = auditInfo.failureString(ERROR_MESSAGE, latency);
    String expected =
        "[AUDIT] FAILURE: Some error GET http://localhost/test {param1=value1, param2=value2} ClientIP: 127.0.0.1:8080 Latency: 10000 ms";
    assertEquals(result, expected);
  }
}
