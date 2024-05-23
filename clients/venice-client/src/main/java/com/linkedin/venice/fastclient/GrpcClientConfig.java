package com.linkedin.venice.fastclient;

import com.google.common.base.Preconditions;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.security.SSLFactory;
import java.util.Map;


public class GrpcClientConfig {
  // Use r2Client for non-storage related requests (not implemented in gRPC yet)
  private final Client r2Client;

  private final int port;
  // require a map from netty server to grpc address due to lack of gRPC service discovery
  private final Map<String, String> nettyServerToGrpcAddress;
  // SSL Factory required if using SSL
  private final SSLFactory sslFactory;

  GrpcClientConfig(Builder builder) {
    this.r2Client = builder.r2Client;
    this.port = builder.port;
    this.nettyServerToGrpcAddress = builder.nettyServerToGrpcAddress;
    this.sslFactory = builder.sslFactory;
  }

  public Client getR2Client() {
    return r2Client;
  }

  public int getPort() {
    return this.port;
  }

  public Map<String, String> getNettyServerToGrpcAddress() {
    return nettyServerToGrpcAddress;
  }

  public SSLFactory getSslFactory() {
    return sslFactory;
  }

  public static class Builder {
    private Client r2Client = null;

    private int port;
    private Map<String, String> nettyServerToGrpcAddress = null;
    private SSLFactory sslFactory = null;

    public Builder setR2Client(Client r2Client) {
      this.r2Client = r2Client;
      return this;
    }

    public Builder setPort(int port) {
      this.port = port;
      return this;
    }

    public Builder setNettyServerToGrpcAddress(Map<String, String> nettyServerToGrpcAddress) {
      this.nettyServerToGrpcAddress = nettyServerToGrpcAddress;
      return this;
    }

    public Builder setSSLFactory(SSLFactory sslFactory) {
      this.sslFactory = sslFactory;
      return this;
    }

    public GrpcClientConfig build() {
      Preconditions.checkNotNull(r2Client);
      Preconditions.checkNotNull(nettyServerToGrpcAddress);
      Preconditions.checkState(port != 0 || !nettyServerToGrpcAddress.isEmpty());
      return new GrpcClientConfig(this);
    }
  }
}
