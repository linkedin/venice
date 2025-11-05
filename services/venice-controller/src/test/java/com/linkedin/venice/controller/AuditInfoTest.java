package com.linkedin.venice.controller;

import static com.linkedin.venice.VeniceConstants.CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.security.cert.X509Certificate;
import java.util.HashSet;
import java.util.Set;
import javax.security.auth.x500.X500Principal;
import javax.servlet.http.HttpServletRequest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import spark.Request;


public class AuditInfoTest {
  private static final String TEST_URL = "http://localhost/test";
  private static final String METHOD_GET = "GET";
  private static final String CLIENT_IP = "127.0.0.1";
  private static final int CLIENT_PORT = 8080;
  private static final String PARAM_1 = "param1";
  private static final String PARAM_2 = "param2";
  private static final String VALUE_1 = "value1";
  private static final String VALUE_2 = "value2";
  private static final String ERROR_MESSAGE = "Some error";
  private static final String TEST_PRINCIPAL = "CN=test-service,OU=Engineering,O=LinkedIn,L=Sunnyvale,ST=CA,C=US";

  private Request request;
  private AuditInfo auditInfo;
  private HttpServletRequest httpServletRequest;

  @BeforeMethod
  public void setUp() {
    request = mock(Request.class);
    when(request.url()).thenReturn(TEST_URL);
    when(request.requestMethod()).thenReturn(METHOD_GET);
    when(request.ip()).thenReturn(CLIENT_IP);

    Set<String> queryParams = new HashSet<>();
    queryParams.add(PARAM_1);
    queryParams.add(PARAM_2);

    when(request.queryParams()).thenReturn(queryParams);
    when(request.queryParams(PARAM_1)).thenReturn(VALUE_1);
    when(request.queryParams(PARAM_2)).thenReturn(VALUE_2);

    httpServletRequest = mock(HttpServletRequest.class);
    when(httpServletRequest.getRemotePort()).thenReturn(CLIENT_PORT);
    when(request.raw()).thenReturn(httpServletRequest);

    auditInfo = new AuditInfo(request);
  }

  @Test
  public void testToStringReturnsExpectedFormat() {
    String result = auditInfo.toString();
    String expected =
        "[AUDIT] GET http://localhost/test {param1=value1, param2=value2} ClientIP: 127.0.0.1:8080 Principal: N/A";
    assertEquals(result, expected);
  }

  @Test
  public void testSuccessStringWithLatencyReturnsExpectedFormat() {
    long latency = 10000;
    String result = auditInfo.successString(latency);
    String expected =
        "[AUDIT] SUCCESS GET http://localhost/test {param1=value1, param2=value2} ClientIP: 127.0.0.1:8080 Principal: N/A Latency: 10000 ms";
    assertEquals(result, expected);
  }

  @Test
  public void testFailureWithLatencyStringReturnsExpectedFormat() {
    long latency = 10000;
    String result = auditInfo.failureString(ERROR_MESSAGE, latency);
    String expected =
        "[AUDIT] FAILURE: Some error GET http://localhost/test {param1=value1, param2=value2} ClientIP: 127.0.0.1:8080 Principal: N/A Latency: 10000 ms";
    assertEquals(result, expected);
  }

  @Test
  public void testExtractServicePrincipalWithValidCertificate() {
    // Case 1: Valid X509 certificate with principal
    Request requestWithCert = mock(Request.class);
    when(requestWithCert.url()).thenReturn(TEST_URL);
    when(requestWithCert.requestMethod()).thenReturn(METHOD_GET);
    when(requestWithCert.ip()).thenReturn(CLIENT_IP);
    when(requestWithCert.queryParams()).thenReturn(new HashSet<>());

    HttpServletRequest httpServletRequestWithCert = mock(HttpServletRequest.class);
    when(httpServletRequestWithCert.getRemotePort()).thenReturn(CLIENT_PORT);

    X509Certificate mockCertificate = mock(X509Certificate.class);
    X500Principal mockPrincipal = new X500Principal(TEST_PRINCIPAL);
    when(mockCertificate.getSubjectX500Principal()).thenReturn(mockPrincipal);

    X509Certificate[] certArray = new X509Certificate[] { mockCertificate };
    when(httpServletRequestWithCert.getAttribute(CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME)).thenReturn(certArray);
    when(requestWithCert.raw()).thenReturn(httpServletRequestWithCert);

    AuditInfo auditInfoWithCert = new AuditInfo(requestWithCert);
    String result = auditInfoWithCert.toString();

    // Verify principal is included in the output
    assertEquals(result, "[AUDIT] GET http://localhost/test {} ClientIP: 127.0.0.1:8080 Principal: " + TEST_PRINCIPAL);
  }

  @Test
  public void testExtractServicePrincipalWithNoCertificate() {
    // Case 1: No certificate attribute present
    Request requestNoCert = mock(Request.class);
    when(requestNoCert.url()).thenReturn(TEST_URL);
    when(requestNoCert.requestMethod()).thenReturn(METHOD_GET);
    when(requestNoCert.ip()).thenReturn(CLIENT_IP);
    when(requestNoCert.queryParams()).thenReturn(new HashSet<>());

    HttpServletRequest httpServletRequestNoCert = mock(HttpServletRequest.class);
    when(httpServletRequestNoCert.getRemotePort()).thenReturn(CLIENT_PORT);
    when(httpServletRequestNoCert.getAttribute(CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME)).thenReturn(null);
    when(requestNoCert.raw()).thenReturn(httpServletRequestNoCert);

    AuditInfo auditInfoNoCert = new AuditInfo(requestNoCert);
    String result = auditInfoNoCert.toString();

    // Verify N/A is used when no certificate
    assertEquals(result, "[AUDIT] GET http://localhost/test {} ClientIP: 127.0.0.1:8080 Principal: N/A");
  }

  @Test
  public void testExtractServicePrincipalWithInvalidCertificate() {
    // Case 1: Certificate attribute is not an array
    Request requestInvalidCert = mock(Request.class);
    when(requestInvalidCert.url()).thenReturn(TEST_URL);
    when(requestInvalidCert.requestMethod()).thenReturn(METHOD_GET);
    when(requestInvalidCert.ip()).thenReturn(CLIENT_IP);
    when(requestInvalidCert.queryParams()).thenReturn(new HashSet<>());

    HttpServletRequest httpServletRequestInvalidCert = mock(HttpServletRequest.class);
    when(httpServletRequestInvalidCert.getRemotePort()).thenReturn(CLIENT_PORT);
    when(httpServletRequestInvalidCert.getAttribute(CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME))
        .thenReturn("not-a-certificate-array");
    when(requestInvalidCert.raw()).thenReturn(httpServletRequestInvalidCert);

    AuditInfo auditInfoInvalidCert = new AuditInfo(requestInvalidCert);
    String result = auditInfoInvalidCert.toString();

    // Verify N/A is used when certificate is invalid
    assertEquals(result, "[AUDIT] GET http://localhost/test {} ClientIP: 127.0.0.1:8080 Principal: N/A");

    // Case 2: Empty certificate array
    Request requestEmptyCert = mock(Request.class);
    when(requestEmptyCert.url()).thenReturn(TEST_URL);
    when(requestEmptyCert.requestMethod()).thenReturn(METHOD_GET);
    when(requestEmptyCert.ip()).thenReturn(CLIENT_IP);
    when(requestEmptyCert.queryParams()).thenReturn(new HashSet<>());

    HttpServletRequest httpServletRequestEmptyCert = mock(HttpServletRequest.class);
    when(httpServletRequestEmptyCert.getRemotePort()).thenReturn(CLIENT_PORT);
    when(httpServletRequestEmptyCert.getAttribute(CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME))
        .thenReturn(new X509Certificate[0]);
    when(requestEmptyCert.raw()).thenReturn(httpServletRequestEmptyCert);

    AuditInfo auditInfoEmptyCert = new AuditInfo(requestEmptyCert);
    String resultEmpty = auditInfoEmptyCert.toString();

    // Verify N/A is used when certificate array is empty
    assertEquals(resultEmpty, "[AUDIT] GET http://localhost/test {} ClientIP: 127.0.0.1:8080 Principal: N/A");
  }
}
