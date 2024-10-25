package com.linkedin.venice.controllerapi.transport;

import static com.linkedin.venice.controllerapi.ControllerClient.DEFAULT_MAX_ATTEMPTS;
import static com.linkedin.venice.controllerapi.ControllerClient.DEFAULT_REQUEST_TIMEOUT_MS;
import static com.linkedin.venice.controllerapi.ControllerClient.QUERY_JOB_STATUS_TIMEOUT;

import com.linkedin.venice.security.SSLFactory;
import java.util.List;


public class ControllerTransportAdapterConfigs {
  private final List<String> controllerDiscoveryUrls;
  private final SSLFactory sslFactory;
  private final int maxAttempts;
  private final int queryJobStatusTimeout;
  private final int requestTimeout;
  private final String clusterName;
  private final String controllerGrpcUrl;

  private ControllerTransportAdapterConfigs(Builder builder) {
    this.controllerDiscoveryUrls = builder.controllerDiscoveryUrls;
    this.sslFactory = builder.sslFactory;
    this.maxAttempts = builder.maxAttempts;
    this.queryJobStatusTimeout = builder.queryJobStatusTimeout;
    this.requestTimeout = builder.requestTimeout;
    this.clusterName = builder.clusterName;
    this.controllerGrpcUrl = builder.controllerGrpcUrl;
  }

  public String getControllerGrpcUrl() {
    return controllerGrpcUrl;
  }

  public String getClusterName() {
    return clusterName;
  }

  public SSLFactory getSslFactory() {
    return sslFactory;
  }

  public int getMaxAttempts() {
    return maxAttempts;
  }

  public int getQueryJobStatusTimeout() {
    return queryJobStatusTimeout;
  }

  public int getRequestTimeout() {
    return requestTimeout;
  }

  public List<String> getControllerDiscoveryUrls() {
    return controllerDiscoveryUrls;
  }

  public static class Builder {
    private List<String> controllerDiscoveryUrls;
    private SSLFactory sslFactory;
    private String clusterName;
    private int maxAttempts = -1;
    private int queryJobStatusTimeout = -1;
    private int requestTimeout = -1;
    private String controllerGrpcUrl;

    public Builder() {
    }

    public Builder setControllerGrpcUrl(String controllerGrpcUrl) {
      this.controllerGrpcUrl = controllerGrpcUrl;
      return this;
    }

    public Builder setClusterName(String clusterName) {
      this.clusterName = clusterName;
      return this;
    }

    public Builder setControllerDiscoveryUrls(List<String> controllerDiscoveryUrls) {
      this.controllerDiscoveryUrls = controllerDiscoveryUrls;
      return this;
    }

    public Builder setMaxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    public Builder setQueryJobStatusTimeout(int queryJobStatusTimeout) {
      this.queryJobStatusTimeout = queryJobStatusTimeout;
      return this;
    }

    public Builder setRequestTimeout(int requestTimeout) {
      this.requestTimeout = requestTimeout;
      return this;
    }

    public Builder setSslFactory(SSLFactory sslFactory) {
      this.sslFactory = sslFactory;
      return this;
    }

    private void verifyAndAddDefaults() {
      if (maxAttempts == -1) {
        maxAttempts = DEFAULT_MAX_ATTEMPTS;
      }
      if (queryJobStatusTimeout == -1) {
        queryJobStatusTimeout = QUERY_JOB_STATUS_TIMEOUT;
      }
      if (requestTimeout == -1) {
        requestTimeout = DEFAULT_REQUEST_TIMEOUT_MS;
      }
    }

    public ControllerTransportAdapterConfigs build() {
      verifyAndAddDefaults();
      return new ControllerTransportAdapterConfigs(this);
    }
  }
}
