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
    return formatAuditMessage("[AUDIT]", null, null);
  }

  public String successString(long latency) {
    return formatAuditMessage("[AUDIT]", "SUCCESS", latency);
  }

  public String failureString(String errMsg, long latency) {
    return formatAuditMessage("[AUDIT]", "FAILURE: " + (errMsg != null ? errMsg : ""), latency);
  }

  private String formatAuditMessage(String prefix, String status, Long latency) {
    StringJoiner joiner = new StringJoiner(" ").add(prefix);

    if (status != null) {
      joiner.add(status);
    }

    joiner.add(method).add(url).add(params.toString()).add("ClientIP: " + clientIp);

    if (latency != null) {
      joiner.add("Latency: " + latency + " ms");
    }

    return joiner.toString();
  }
}
