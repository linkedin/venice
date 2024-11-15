package com.linkedin.venice.controller;

import static com.linkedin.venice.controllerapi.ControllerRoute.AGGREGATED_HEALTH_STATUS;
import static com.linkedin.venice.controllerapi.ControllerRoute.LEADER_CONTROLLER;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.HttpMethod;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;
import spark.Request;


public class TestAuditInfo {
  @Test
  public void testAuditInfo() {
    Request request = mock(Request.class);
    String leaderControllerUrl = "http://localhost:8080" + LEADER_CONTROLLER.getPath();

    when(request.url()).thenReturn(leaderControllerUrl);

    Map<String, String> queryParams = new HashMap<>();
    queryParams.put("key1", "value1");
    queryParams.put("key2", "value2");

    when(request.queryParams()).thenReturn(queryParams.keySet());
    doAnswer((invocation) -> {
      String key = invocation.getArgument(0);
      return queryParams.get(key);
    }).when(request).queryParams(anyString());

    when(request.requestMethod()).thenReturn(HttpMethod.GET.name());
    when(request.uri()).thenReturn(LEADER_CONTROLLER.getPath());
    when(request.body()).thenReturn("body");

    AuditInfo auditInfo = new AuditInfo(request);
    assertEquals(auditInfo.getUrl(), leaderControllerUrl);
    assertEquals(queryParams, auditInfo.getParams());
    assertEquals(auditInfo.getMethod(), "GET");
    assertNull(auditInfo.getBodyContent());
    assertEquals(
        auditInfo.toString(),
        "[AUDIT] GET http://localhost:8080/leader_controller {key1=value1, key2=value2}");
    assertEquals(
        auditInfo.successString(),
        "[AUDIT] SUCCESS GET http://localhost:8080/leader_controller {key1=value1, key2=value2}");
    assertEquals(
        auditInfo.failureString(null),
        "[AUDIT] FAILURE GET http://localhost:8080/leader_controller {key1=value1, key2=value2}");
    assertEquals(
        auditInfo.failureString("ERROR"),
        "[AUDIT] FAILURE ERROR GET http://localhost:8080/leader_controller {key1=value1, key2=value2}");
  }

  @Test
  public void testAuditInfoForUndefinedEndpoint() {
    Request request = mock(Request.class);
    String undefinedEndpointUrl = "http://localhost:8080/undefined_endpoint";

    when(request.url()).thenReturn(undefinedEndpointUrl);

    Map<String, String> queryParams = new HashMap<>();
    queryParams.put("key1", "value1");
    queryParams.put("key2", "value2");

    when(request.queryParams()).thenReturn(queryParams.keySet());
    doAnswer((invocation) -> {
      String key = invocation.getArgument(0);
      return queryParams.get(key);
    }).when(request).queryParams(anyString());

    when(request.requestMethod()).thenReturn(HttpMethod.GET.name());
    when(request.uri()).thenReturn("/undefined_endpoint");
    when(request.body()).thenReturn("body");

    AuditInfo auditInfo = new AuditInfo(request);
    assertEquals(auditInfo.getUrl(), undefinedEndpointUrl);
    assertEquals(queryParams, auditInfo.getParams());
    assertEquals(auditInfo.getMethod(), "GET");
    assertNull(auditInfo.getBodyContent());
    assertEquals(
        auditInfo.toString(),
        "[AUDIT] GET http://localhost:8080/undefined_endpoint {key1=value1, key2=value2}");
    assertEquals(
        auditInfo.successString(),
        "[AUDIT] SUCCESS GET http://localhost:8080/undefined_endpoint {key1=value1, key2=value2}");
    assertEquals(
        auditInfo.failureString(null),
        "[AUDIT] FAILURE GET http://localhost:8080/undefined_endpoint {key1=value1, key2=value2}");
    assertEquals(
        auditInfo.failureString("ERROR"),
        "[AUDIT] FAILURE ERROR GET http://localhost:8080/undefined_endpoint {key1=value1, key2=value2}");
  }

  @Test
  public void testAuditInfoWithoutSensitiveContent() {
    Request request = mock(Request.class);
    String aggregatedHealthStatusUrl = "http://localhost:8080" + AGGREGATED_HEALTH_STATUS.getPath();

    when(request.url()).thenReturn(aggregatedHealthStatusUrl);

    Map<String, String> queryParams = new HashMap<>();
    queryParams.put("key1", "value1");
    queryParams.put("key2", "value2");

    when(request.queryParams()).thenReturn(queryParams.keySet());
    doAnswer((invocation) -> {
      String key = invocation.getArgument(0);
      return queryParams.get(key);
    }).when(request).queryParams(anyString());

    when(request.requestMethod()).thenReturn(HttpMethod.GET.name());
    when(request.uri()).thenReturn(AGGREGATED_HEALTH_STATUS.getPath());
    when(request.body()).thenReturn("body");

    AuditInfo auditInfo = new AuditInfo(request);
    assertEquals(auditInfo.getUrl(), aggregatedHealthStatusUrl);
    assertEquals(queryParams, auditInfo.getParams());
    assertEquals(auditInfo.getMethod(), HttpMethod.GET.name());
    assertEquals(auditInfo.getBodyContent(), "body");
    assertEquals(
        auditInfo.toString(),
        "[AUDIT] GET http://localhost:8080/aggregatedHealthStatus {key1=value1, key2=value2} body");
    assertEquals(
        auditInfo.successString(),
        "[AUDIT] SUCCESS GET http://localhost:8080/aggregatedHealthStatus {key1=value1, key2=value2} body");
    assertEquals(
        auditInfo.failureString(null),
        "[AUDIT] FAILURE GET http://localhost:8080/aggregatedHealthStatus {key1=value1, key2=value2} body");
    assertEquals(
        auditInfo.failureString("ERROR"),
        "[AUDIT] FAILURE ERROR GET http://localhost:8080/aggregatedHealthStatus {key1=value1, key2=value2} body");
  }
}
