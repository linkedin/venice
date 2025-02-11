package com.linkedin.venice.controller;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

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
  private static final String AUDIT_PREFIX = "[AUDIT]";
  private static final String SUCCESS = "SUCCESS";
  private static final String FAILURE = "FAILURE";
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
    assertTrue(result.contains(AUDIT_PREFIX));
    assertTrue(result.contains(METHOD_GET));
    assertTrue(result.contains(TEST_URL));
    assertTrue(result.contains(PARAM_1 + "=" + VALUE_1));
    assertTrue(result.contains(PARAM_2 + "=" + VALUE_2));
    assertTrue(result.contains("ClientIP: " + CLIENT_IP + ":" + CLIENT_PORT));
  }

  @Test
  public void testSuccessStringReturnsExpectedFormat() {
    String result = auditInfo.successString();
    assertTrue(result.contains(AUDIT_PREFIX));
    assertTrue(result.contains(SUCCESS));
    assertTrue(result.contains(METHOD_GET));
    assertTrue(result.contains(TEST_URL));
    assertTrue(result.contains("ClientIP: " + CLIENT_IP));
  }

  @Test
  public void testFailureStringReturnsExpectedFormat() {
    String result = auditInfo.failureString(ERROR_MESSAGE);
    assertTrue(result.contains(AUDIT_PREFIX));
    assertTrue(result.contains(FAILURE));
    assertTrue(result.contains(ERROR_MESSAGE));
    assertTrue(result.contains(METHOD_GET));
    assertTrue(result.contains(TEST_URL));
    assertTrue(result.contains("ClientIP: " + CLIENT_IP));
  }

  @Test
  public void testFailureStringHandlesNullErrorMessage() {
    String result = auditInfo.failureString(null);
    assertTrue(result.contains(AUDIT_PREFIX));
    assertFalse(result.contains("null"));
  }
}
