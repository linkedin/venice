package com.linkedin.venice.controller;

import static com.linkedin.venice.VeniceConstants.CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME;

import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import javax.servlet.http.HttpServletRequest;
import spark.Request;


public class AuditInfo {
  private String url;
  private Map<String, String> params;
  private String method;
  private String clientIp;
  private String servicePrincipal;

  public AuditInfo(Request request) {
    this.url = request.url();
    this.params = new HashMap<>();
    for (String param: request.queryParams()) {
      this.params.put(param, request.queryParams(param));
    }
    this.method = request.requestMethod();
    this.clientIp = request.ip() + ":" + request.raw().getRemotePort();
    this.servicePrincipal = extractServicePrincipal(request);
  }

  private String extractServicePrincipal(Request request) {
    try {
      HttpServletRequest rawRequest = request.raw();
      Object certificateObject = rawRequest.getAttribute(CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME);
      if (certificateObject instanceof X509Certificate[]) {
        X509Certificate[] certs = (X509Certificate[]) certificateObject;
        if (certs.length > 0 && certs[0] != null) {
          return certs[0].getSubjectX500Principal().getName();
        }
      }
    } catch (Exception e) {
      // Silently ignore exceptions during principal extraction to avoid cluttering controller logs.
      // Principal extraction is a nice-to-have audit feature and not critical for operation.
    }
    return "N/A";
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

    joiner.add(method)
        .add(url)
        .add(params.toString())
        .add("ClientIP: " + clientIp)
        .add("Principal: " + servicePrincipal);

    if (latency != null) {
      joiner.add("Latency: " + latency + " ms");
    }

    return joiner.toString();
  }
}
