package com.linkedin.venice.controller;

import static com.linkedin.venice.controller.server.AdminSparkServer.REQUEST_PRINCIPAL_ATTRIBUTE_NAME;

import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import spark.Request;


public class AuditInfo {
  private String url;
  private Map<String, String> params;
  private String method;
  private Object principal;

  public AuditInfo(Request request) {
    this.url = request.url();
    this.params = new HashMap<>();
    for (String param: request.queryParams()) {
      this.params.put(param, request.queryParams(param));
    }
    this.method = request.requestMethod();
    this.principal = request.attribute(REQUEST_PRINCIPAL_ATTRIBUTE_NAME);
  }

  /**
   * @return a string representation of {@link AuditInfo} object.
   */
  @Override
  public String toString() {
    StringJoiner joiner = new StringJoiner(" ");
    joiner.add("[AUDIT]");
    if (principal != null) {
      joiner.add(principal.toString());
    }
    joiner.add(method);
    joiner.add(url);
    joiner.add(params.toString());
    return joiner.toString();
  }

  /**
   * @return a audit-successful string.
   */
  public String successString() {
    return toString(true, null);
  }

  /**
   * @return a audit-failure string.
   */
  public String failureString(String errMsg) {
    return toString(false, errMsg);
  }

  private String toString(boolean success, String errMsg) {
    StringJoiner joiner = new StringJoiner(" ");
    joiner.add("[AUDIT]");
    if (principal != null) {
      joiner.add(principal.toString());
    }
    if (success) {
      joiner.add("SUCCESS");
    } else {
      joiner.add("FAILURE: ");
      if (errMsg != null) {
        joiner.add(errMsg);
      }
    }
    joiner.add(method);
    joiner.add(url);
    joiner.add(params.toString());
    return joiner.toString();
  }
}
