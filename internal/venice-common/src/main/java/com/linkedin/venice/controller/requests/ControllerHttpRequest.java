package com.linkedin.venice.controller.requests;

import com.linkedin.venice.controllerapi.QueryParams;
import com.linkedin.venice.utils.Time;


/**
 * A general class to represent Http specific transport request that embodies all the Http requests
 * send to the controller as part of controller APIs. This is a container class defined to help with the
 * refactoring of ControllerClient to become transport protocol agnostic.
 */
public class ControllerHttpRequest {
  private static final int DEFAULT_REQUEST_TIMEOUT_MS = 600 * Time.MS_PER_SECOND;
  private static final int DEFAULT_MAX_ATTEMPTS = 10;
  private final QueryParams params;
  private final byte[] data;

  private final int timeoutMs;

  private final int maxRetries;

  public ControllerHttpRequest(QueryParams params, byte[] data, int timeoutMs, int maxRetries) {
    this.params = params;
    this.data = data;
    this.timeoutMs = timeoutMs;
    this.maxRetries = maxRetries;
  }

  public QueryParams getParams() {
    return params;
  }

  public byte[] getData() {
    return data;
  }

  public int getTimeoutMs() {
    return timeoutMs;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private QueryParams params;
    private byte[] data;
    private int timeoutMs = DEFAULT_REQUEST_TIMEOUT_MS;

    private int maxRetries = DEFAULT_MAX_ATTEMPTS;

    public Builder setParam(QueryParams params) {
      this.params = params;
      return this;
    }

    public Builder setData(byte[] data) {
      this.data = data;
      return this;
    }

    public Builder setTimeoutMs(int timeoutMs) {
      this.timeoutMs = timeoutMs;
      return this;
    }

    public Builder setMaxRetries(int retries) {
      this.maxRetries = retries;
      return this;
    }

    public ControllerHttpRequest build() {
      return new ControllerHttpRequest(params, data, timeoutMs, maxRetries);
    }
  }
}
