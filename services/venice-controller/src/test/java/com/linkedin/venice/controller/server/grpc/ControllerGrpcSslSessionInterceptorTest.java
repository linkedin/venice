package com.linkedin.venice.controller.server.grpc;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.Attributes;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import java.net.SocketAddress;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ControllerGrpcSslSessionInterceptorTest {
  private ControllerGrpcSslSessionInterceptor interceptor;
  private ServerCall<Object, Object> serverCall;
  private Metadata metadata;
  private ServerCallHandler<Object, Object> serverCallHandler;

  @BeforeMethod
  public void setUp() {
    interceptor = new ControllerGrpcSslSessionInterceptor();
    serverCall = mock(ServerCall.class);
    metadata = new Metadata();
    serverCallHandler = mock(ServerCallHandler.class);
  }

  @Test
  public void testSslSessionNotEnabled() {
    Attributes attributes = Attributes.newBuilder().build();
    when(serverCall.getAttributes()).thenReturn(attributes);
    interceptor.interceptCall(serverCall, metadata, serverCallHandler);
    verify(serverCall, times(1)).close(
        eq(ControllerGrpcSslSessionInterceptor.NON_SSL_CONNECTION_ERROR.getStatus()),
        eq(ControllerGrpcSslSessionInterceptor.NON_SSL_CONNECTION_ERROR.getTrailers()));
    verify(serverCallHandler, never()).startCall(any(), any());
  }

  @Test
  public void testSslSessionEnabledWithValidCertificate() throws SSLPeerUnverifiedException {
    SSLSession sslSession = mock(SSLSession.class);
    SocketAddress remoteAddress = mock(SocketAddress.class);
    X509Certificate clientCertificate = mock(X509Certificate.class);

    Attributes attributes = Attributes.newBuilder()
        .set(Grpc.TRANSPORT_ATTR_SSL_SESSION, sslSession)
        .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, remoteAddress)
        .build();

    when(serverCall.getAttributes()).thenReturn(attributes);
    when(remoteAddress.toString()).thenReturn("remote-address");
    Certificate[] peerCertificates = new Certificate[] { clientCertificate };
    when(sslSession.getPeerCertificates()).thenReturn(peerCertificates);

    interceptor.interceptCall(serverCall, metadata, serverCallHandler);

    verify(serverCallHandler, times(1)).startCall(serverCall, metadata);
  }

  @Test
  public void testCertificateExtractionFails() throws SSLPeerUnverifiedException {
    SSLSession sslSession = mock(SSLSession.class);
    SocketAddress remoteAddress = mock(SocketAddress.class);

    Attributes attributes = Attributes.newBuilder()
        .set(Grpc.TRANSPORT_ATTR_SSL_SESSION, sslSession)
        .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, remoteAddress)
        .build();

    when(serverCall.getAttributes()).thenReturn(attributes);
    doThrow(new RuntimeException("Failed to extract certificate")).when(sslSession).getPeerCertificates();

    interceptor.interceptCall(serverCall, metadata, serverCallHandler);

    verify(serverCall, times(1)).close(
        eq(ControllerGrpcSslSessionInterceptor.NON_SSL_CONNECTION_ERROR.getStatus()),
        eq(ControllerGrpcSslSessionInterceptor.NON_SSL_CONNECTION_ERROR.getTrailers()));
    verify(serverCallHandler, never()).startCall(any(), any());
  }

  @Test
  public void testRemoteAddressUnknown() throws SSLPeerUnverifiedException {
    SSLSession sslSession = mock(SSLSession.class);
    X509Certificate clientCertificate = mock(X509Certificate.class);

    Attributes attributes = Attributes.newBuilder().set(Grpc.TRANSPORT_ATTR_SSL_SESSION, sslSession).build();

    when(serverCall.getAttributes()).thenReturn(attributes);
    Certificate[] peerCertificates = new Certificate[] { clientCertificate };
    when(sslSession.getPeerCertificates()).thenReturn(peerCertificates);

    interceptor.interceptCall(serverCall, metadata, serverCallHandler);

    verify(serverCallHandler, times(1)).startCall(serverCall, metadata);
  }
}
