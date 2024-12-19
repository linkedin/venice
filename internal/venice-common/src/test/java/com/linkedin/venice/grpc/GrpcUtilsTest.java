package com.linkedin.venice.grpc;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.acl.handler.AccessResult;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import io.grpc.Attributes;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ServerCall;
import io.grpc.Status;
import io.grpc.TlsChannelCredentials;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class GrpcUtilsTest {
  private static SSLFactory sslFactory;

  @BeforeTest
  public static void setup() {
    sslFactory = SslUtils.getVeniceLocalSslFactory();
  }

  @Test
  public void testGetTrustManagers() throws Exception {
    TrustManager[] trustManagers = GrpcUtils.getTrustManagers(sslFactory);

    assertNotNull(trustManagers);
    assertTrue(trustManagers.length > 0);
  }

  @Test
  public void testGetKeyManagers() throws Exception {
    KeyManager[] keyManagers = GrpcUtils.getKeyManagers(sslFactory);

    assertNotNull(keyManagers);
    assertTrue(keyManagers.length > 0);
  }

  @Test
  public void testHttpResponseStatusToGrpcStatus() {
    Status grpcStatus = GrpcUtils.accessResultToGrpcStatus(AccessResult.GRANTED);
    assertEquals(
        grpcStatus.getCode(),
        Status.OK.getCode(),
        "Mismatch in GRPC status for the http response status permission denied");
    assertEquals(
        AccessResult.GRANTED.getMessage(),
        grpcStatus.getDescription(),
        "Mismatch in error description for the mapped grpc status");

    grpcStatus = GrpcUtils.accessResultToGrpcStatus(AccessResult.FORBIDDEN);
    assertEquals(
        grpcStatus.getCode(),
        Status.PERMISSION_DENIED.getCode(),
        "Mismatch in GRPC status for the http response status permission denied");
    assertEquals(
        AccessResult.FORBIDDEN.getMessage(),
        grpcStatus.getDescription(),
        "Mismatch in error description for the mapped grpc status");

    grpcStatus = GrpcUtils.accessResultToGrpcStatus(AccessResult.UNAUTHORIZED);
    assertEquals(
        grpcStatus.getCode(),
        Status.PERMISSION_DENIED.getCode(),
        "Mismatch in GRPC status for the http response status unauthorized");
    assertEquals(
        AccessResult.UNAUTHORIZED.getMessage(),
        grpcStatus.getDescription(),
        "Mismatch in error description for the mapped grpc status");
  }

  @Test
  public void testBuildChannelCredentials() {
    // Case 1: sslFactory is null, expect InsecureChannelCredentials
    ChannelCredentials credentials = GrpcUtils.buildChannelCredentials(null);
    assertTrue(
        credentials instanceof InsecureChannelCredentials,
        "Expected InsecureChannelCredentials when sslFactory is null");

    // Case 2: Valid sslFactory, expect TlsChannelCredentials
    SSLFactory validSslFactory = SslUtils.getVeniceLocalSslFactory();
    credentials = GrpcUtils.buildChannelCredentials(validSslFactory);
    assertTrue(
        credentials instanceof TlsChannelCredentials,
        "Expected TlsChannelCredentials when sslFactory is provided");

    // Case 3: SSLFactory throws an exception when initializing credentials
    SSLFactory faultySslFactory = mock(SSLFactory.class);
    Exception exception =
        expectThrows(VeniceClientException.class, () -> GrpcUtils.buildChannelCredentials(faultySslFactory));
    assertEquals(
        exception.getMessage(),
        "Failed to initialize SSL channel credentials for Venice gRPC Transport Client");
  }

  @Test
  public void testExtractGrpcClientCertWithValidCertificate() throws SSLPeerUnverifiedException {
    // Mock SSLSession and Certificate
    SSLSession sslSession = mock(SSLSession.class);
    X509Certificate x509Certificate = mock(X509Certificate.class);

    // Mock the ServerCall and its attributes
    Attributes attributes = Attributes.newBuilder().set(Grpc.TRANSPORT_ATTR_SSL_SESSION, sslSession).build();
    ServerCall<?, ?> call = mock(ServerCall.class);
    when(call.getAttributes()).thenReturn(attributes);
    when(sslSession.getPeerCertificates()).thenReturn(new Certificate[] { x509Certificate });

    // Extract the certificate
    X509Certificate extractedCertificate = GrpcUtils.extractGrpcClientCert(call);

    // Verify the returned certificate
    assertEquals(extractedCertificate, x509Certificate);
  }

  @Test
  public void testExtractGrpcClientCertWithNullSslSession() {
    // Mock the ServerCall with null SSLSession
    Attributes attributes = Attributes.newBuilder().set(Grpc.TRANSPORT_ATTR_SSL_SESSION, null).build();
    ServerCall<?, ?> call = mock(ServerCall.class);
    when(call.getAttributes()).thenReturn(attributes);
    when(call.getAuthority()).thenReturn("test-authority");

    // Expect a VeniceException
    VeniceException thrownException = expectThrows(VeniceException.class, () -> {
      GrpcUtils.extractGrpcClientCert(call);
    });

    // Verify the exception message
    assertEquals(thrownException.getMessage(), "Failed to obtain SSL session");
  }

  @Test
  public void testExtractGrpcClientCertWithPeerCertificateNotX509() throws SSLPeerUnverifiedException {
    // Mock SSLSession and Certificate
    SSLSession sslSession = mock(SSLSession.class);
    Certificate nonX509Certificate = mock(Certificate.class);

    // Mock the ServerCall and its attributes
    Attributes attributes = Attributes.newBuilder().set(Grpc.TRANSPORT_ATTR_SSL_SESSION, sslSession).build();
    ServerCall<?, ?> call = mock(ServerCall.class);
    when(call.getAttributes()).thenReturn(attributes);
    when(sslSession.getPeerCertificates()).thenReturn(new Certificate[] { nonX509Certificate });

    // Expect IllegalArgumentException
    IllegalArgumentException thrownException =
        expectThrows(IllegalArgumentException.class, () -> GrpcUtils.extractGrpcClientCert(call));

    // Verify the exception message
    assertTrue(
        thrownException.getMessage()
            .contains("Only certificates of type java.security.cert.X509Certificate are supported"));
  }

  @Test
  public void testExtractGrpcClientCertWithNullPeerCertificates() throws SSLPeerUnverifiedException {
    // Mock SSLSession with null peer certificates
    SSLSession sslSession = mock(SSLSession.class);
    when(sslSession.getPeerCertificates()).thenReturn(null);

    // Mock the ServerCall and its attributes
    Attributes attributes = Attributes.newBuilder().set(Grpc.TRANSPORT_ATTR_SSL_SESSION, sslSession).build();
    ServerCall<?, ?> call = mock(ServerCall.class);
    when(call.getAttributes()).thenReturn(attributes);

    // Expect NullPointerException or VeniceException
    NullPointerException thrownException =
        expectThrows(NullPointerException.class, () -> GrpcUtils.extractGrpcClientCert(call));

    // Verify the exception is thrown
    assertNotNull(thrownException);
  }

  @Test
  public void testExtractGrpcClientCertWithEmptyPeerCertificates() throws SSLPeerUnverifiedException {
    // Mock SSLSession with empty peer certificates
    SSLSession sslSession = mock(SSLSession.class);
    when(sslSession.getPeerCertificates()).thenReturn(new Certificate[] {});

    // Mock the ServerCall and its attributes
    Attributes attributes = Attributes.newBuilder().set(Grpc.TRANSPORT_ATTR_SSL_SESSION, sslSession).build();
    ServerCall<?, ?> call = mock(ServerCall.class);
    when(call.getAttributes()).thenReturn(attributes);

    // Expect IndexOutOfBoundsException
    IndexOutOfBoundsException thrownException =
        expectThrows(IndexOutOfBoundsException.class, () -> GrpcUtils.extractGrpcClientCert(call));

    // Verify the exception is thrown
    assertNotNull(thrownException);
  }
}
