package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.ControllerRoute;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import spark.Request;


public class AuditInfo {
  private final String url;
  private final Map<String, String> params;
  private final String method;
  private final String bodyContent;

  public AuditInfo(Request request) {
    this.url = request.url();
    this.params = new HashMap<>();
    for (String param: request.queryParams()) {
      this.params.put(param, request.queryParams(param));
    }
    this.method = request.requestMethod();

    ControllerRoute route = ControllerRoute.valueOfPath(request.uri());
    if (route != null && !route.doesBodyHaveSensitiveContent()) {
      this.bodyContent = request.body();
    } else {
      this.bodyContent = null;
    }
  }

  // Visible for testing
  String getUrl() {
    return url;
  }

  // Visible for testing
  Map<String, String> getParams() {
    return params;
  }

  // Visible for testing
  String getMethod() {
    return method;
  }

  // Visible for testing
  String getBodyContent() {
    return bodyContent;
  }

  /**
   * @return a string representation of {@link AuditInfo} object.
   */
  @Override
  public String toString() {
    StringJoiner joiner = new StringJoiner(" ");
    joiner.add("[AUDIT]");
    joiner.add(method);
    joiner.add(url);
    joiner.add(params.toString());

    if (bodyContent != null) {
      joiner.add(bodyContent);
    }

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
    if (success) {
      joiner.add("SUCCESS");
    } else {
      joiner.add("FAILURE");
      if (errMsg != null) {
        joiner.add(errMsg);
      }
    }
    joiner.add(method);
    joiner.add(url);
    joiner.add(params.toString());

    if (bodyContent != null) {
      joiner.add(bodyContent);
    }

    return joiner.toString();
  }
}
