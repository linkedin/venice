package com.linkedin.venice.router.httpclient;

import com.linkedin.venice.meta.Instance;


public class VeniceMetaDataRequest {
  private static final int NO_REQUEST_TIMEOUT = -1;
  private final String method;
  private final Instance host;
  private final String query;
  private final boolean isSSL;
  private int timeout = NO_REQUEST_TIMEOUT;

  public VeniceMetaDataRequest(Instance host, String query, String method, boolean isSSL) {
    this.method = method;
    this.host = host;
    this.query = query;
    this.isSSL = isSSL;
  }

  public void setTimeout(int timeout) {
    this.timeout = timeout;
  }

  public boolean hasTimeout() {
    return timeout != NO_REQUEST_TIMEOUT;
  }

  public int getTimeout() {
    return timeout;
  }

  public String getNodeId() {
    return host.getNodeId();
  }

  public String getUrl() {
    return host.getHostUrl(isSSL);
  }

  public String getMethod() {
    return method;
  }

  public String getQuery() {
    return query;
  }
}
