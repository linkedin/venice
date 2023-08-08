package com.linkedin.venice.fastclient;

import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.security.SSLFactory;
import java.util.Map;


public class GrpcClientConfig {
  private Client r2Client;
  private Map<String, String> nettyServerToGrpcAddressMap;
  private SSLFactory sslFactory;

  public GrpcClientConfig(Builder builder) {
    this.r2Client = builder.r2Client;
    this.nettyServerToGrpcAddressMap = builder.nettyServerToGrpcAddressMap;
    this.sslFactory = builder.sslFactory;
  }

  public Client getR2Client() {
    return r2Client;
  }

  public Map<String, String> getNettyServerToGrpcAddressMap() {
    return nettyServerToGrpcAddressMap;
  }

  public SSLFactory getSslFactory() {
    return sslFactory;
  }

  public static class Builder {
    private Client r2Client = null;
    private Map<String, String> nettyServerToGrpcAddressMap = null;
    private SSLFactory sslFactory = null;

    public Builder setR2Client(Client r2Client) {
      this.r2Client = r2Client;
      return this;
    }

    public Builder setNettyServerToGrpcAddressMap(Map<String, String> nettyServerToGrpcAddressMap) {
      this.nettyServerToGrpcAddressMap = nettyServerToGrpcAddressMap;
      return this;
    }

    public Builder setSSLFactory(SSLFactory sslFactory) {
      this.sslFactory = sslFactory;
      return this;
    }

    public GrpcClientConfig build() {
      verify();
      return new GrpcClientConfig(this);
    }

    private void verify() {
      if (r2Client == null) {
        throw new IllegalArgumentException("R2 client must be set when enabling gRPC on FC");
      }
      if (nettyServerToGrpcAddressMap == null) {
        throw new IllegalArgumentException("Netty server to grpc address map must be set");
      }
      if (nettyServerToGrpcAddressMap.size() == 0) {
        throw new IllegalArgumentException("Netty server to grpc address map must not be empty");
      }
    }
  }
}
