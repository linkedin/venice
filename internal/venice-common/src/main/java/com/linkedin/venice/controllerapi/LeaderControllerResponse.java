package com.linkedin.venice.controllerapi;

public class LeaderControllerResponse
    extends ControllerResponse { /* Uses Json Reflective Serializer, get without set may break things */
  private String cluster;
  private String url;
  private String secureUrl = null;
  private String grpcUrl = null;
  private String secureGrpcUrl = null;

  public String getCluster() {
    return cluster;
  }

  public void setCluster(String cluster) {
    this.cluster = cluster;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getSecureUrl() {
    return secureUrl;
  }

  public void setSecureUrl(String url) {
    this.secureUrl = url;
  }

  public void setGrpcUrl(String url) {
    this.grpcUrl = url;
  }

  public String getGrpcUrl() {
    return grpcUrl;
  }

  public void setSecureGrpcUrl(String url) {
    this.secureGrpcUrl = url;
  }

  public String getSecureGrpcUrl() {
    return secureGrpcUrl;
  }
}
