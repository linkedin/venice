package com.linkedin.venice.controller;

import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import spark.Request;


public class AuditInfo {
  private String url;
  private Map<String, String> params;
  private String method;
  private String clientIp;

  public AuditInfo(Request request) {
    this.url = request.url();
    this.params = new HashMap<>();
    for (String param: request.queryParams()) {
      this.params.put(param, request.queryParams(param));
    }
    this.method = request.requestMethod();
    this.clientIp = request.ip() + ":" + request.raw().getRemotePort();
  }

  @Override
  public String toString() {
    return formatAuditMessage("[AUDIT]", null);
  }

  public String successString() {
    return formatAuditMessage("[AUDIT]", "SUCCESS");
  }

  public String failureString(String errMsg) {
    return formatAuditMessage("[AUDIT]", "FAILURE: " + (errMsg != null ? errMsg : ""));
  }

  private String formatAuditMessage(String prefix, String status) {
    StringJoiner joiner = new StringJoiner(" ").add(prefix);

    if (status != null) {
      joiner.add(status);
    }

    joiner.add(method).add(url).add(params.toString()).add("ClientIP: " + clientIp);

    return joiner.toString();
  }
}
