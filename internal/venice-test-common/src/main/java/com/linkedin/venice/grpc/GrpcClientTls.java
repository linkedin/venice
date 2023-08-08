package com.linkedin.venice.grpc;

import com.linkedin.venice.security.SSLConfig;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;


public class GrpcClientTls {
  public static void main(String[] args) {
    // Server server = null;
    // SSLFactory sslFactory = SslUtils.getVeniceLocalSslFactory();
    //
    // SSLConfig sslConfig = sslFactory.getSSLConfig();
    // SSLContext sslContext = sslFactory.getSSLContext();
    // SSLParameters sslParameters = sslFactory.getSSLParameters();

  }

  public static void startServer(int port) throws Exception {
    // ServerBuilder serverBuilder = ServerBuilder.forPort(8080)
    // .addService(new VeniceReadServiceImpl())
    // .use
  }

  public static void startClient() throws Exception {
    SSLFactory sslFactory = SslUtils.getVeniceLocalSslFactory();
    SSLConfig sslConfig = sslFactory.getSSLConfig();

  }
}
