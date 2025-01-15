package com.linkedin.venice.controller.server;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.acl.AclException;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.acl.NoOpDynamicAccessController;
import com.linkedin.venice.authorization.Method;
import java.security.cert.X509Certificate;
import javax.security.auth.x500.X500Principal;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceControllerAccessManagerTest {
  private DynamicAccessController mockedAccessController;
  private VeniceControllerAccessManager controllerAccessManager;

  @BeforeMethod
  public void setUp() {
    mockedAccessController = mock(DynamicAccessController.class);
    controllerAccessManager = new VeniceControllerAccessManager(mockedAccessController);
  }

  @Test
  public void testHasWriteAccessToPubSubTopicGranted() throws AclException {
    String resourceName = "test-topic";
    X509Certificate mockCert = mock(X509Certificate.class);

    when(mockedAccessController.hasAccess(mockCert, resourceName, Method.Write.name())).thenReturn(true);

    assertTrue(controllerAccessManager.hasWriteAccessToPubSubTopic(resourceName, mockCert, "host", "127.0.0.1"));
    verify(mockedAccessController, times(1)).hasAccess(mockCert, resourceName, Method.Write.name());
  }

  @Test
  public void testHasWriteAccessToPubSubTopicDenied() throws AclException {
    String resourceName = "test-topic";
    X509Certificate mockCert = mock(X509Certificate.class);

    when(mockedAccessController.hasAccess(mockCert, resourceName, Method.Write.name())).thenReturn(false);

    assertFalse(controllerAccessManager.hasWriteAccessToPubSubTopic(resourceName, mockCert, "host", "127.0.0.1"));
    verify(mockedAccessController, times(1)).hasAccess(mockCert, resourceName, Method.Write.name());
  }

  @Test
  public void testHasAccessHandlesNullCertificate() throws AclException {
    String resourceName = "test-topic";

    when(mockedAccessController.hasAccess(null, resourceName, Method.Read.name())).thenReturn(true);

    assertTrue(controllerAccessManager.hasReadAccessToPubSubTopic(resourceName, null, "host", "127.0.0.1"));
    verify(mockedAccessController, times(1)).hasAccess(null, resourceName, Method.Read.name());
  }

  @Test
  public void testHasAccessHandlesNullResource() {
    X509Certificate mockCert = mock(X509Certificate.class);

    Exception e = expectThrows(
        NullPointerException.class,
        () -> controllerAccessManager.hasAccess(null, mockCert, Method.Write, "host", ""));
    assertTrue(e.getMessage().contains("Resource name is required to enforce ACL"));
  }

  @Test
  public void testHasAccessHandlesNullMethod() {
    String resourceName = "test-topic";
    X509Certificate mockCert = mock(X509Certificate.class);

    Exception e = expectThrows(
        NullPointerException.class,
        () -> controllerAccessManager.hasAccess(resourceName, mockCert, null, "host", "127.0.0.1"));
    assertTrue(e.getMessage().contains("Access method is required to enforce ACL"));
  }

  @Test
  public void testHasAccessWhenAccessControllerThrowsException() throws AclException {
    String resourceName = "test-topic";
    X509Certificate mockCert = mock(X509Certificate.class);

    when(mockedAccessController.hasAccess(mockCert, resourceName, Method.Write.name()))
        .thenThrow(new AclException("Test exception"));

    assertFalse(controllerAccessManager.hasAccess(resourceName, mockCert, Method.Write, "host", ""));
    verify(mockedAccessController, times(1)).hasAccess(mockCert, resourceName, Method.Write.name());
  }

  @Test
  public void testHasAccessWhenAccessControllerIsNull() {
    Exception e = expectThrows(NullPointerException.class, () -> new VeniceControllerAccessManager(null));
    assertTrue(e.getMessage().contains("is required to enforce ACL"));
  }

  @Test
  public void testAccessManagerWithNoOpDynamicAccessController() {
    String resourceName = "test-topic";
    String hostname = "host";
    String remoteAddr = "127.0.0.1";

    // Case 1: Non-null certificate
    X509Certificate mockCert = mock(X509Certificate.class);
    X500Principal principal = new X500Principal("CN=Test User");
    doReturn(principal).when(mockCert).getSubjectX500Principal();

    VeniceControllerAccessManager accessManager =
        new VeniceControllerAccessManager(NoOpDynamicAccessController.INSTANCE);
    assertTrue(accessManager.hasAccessToStore(resourceName, mockCert, hostname, remoteAddr));
    assertTrue(accessManager.hasReadAccessToPubSubTopic(resourceName, mockCert, hostname, remoteAddr));
    assertTrue(accessManager.hasWriteAccessToPubSubTopic(resourceName, mockCert, hostname, remoteAddr));
    assertTrue(accessManager.isAllowListUser(resourceName, mockCert));
    assertFalse(accessManager.isAclEnabled());
    assertEquals(accessManager.getPrincipalId(mockCert), "CN=Test User");

    // Case 2: Null certificate
    assertTrue(accessManager.hasAccessToStore(resourceName, null, hostname, remoteAddr));
    assertTrue(accessManager.hasReadAccessToPubSubTopic(resourceName, null, hostname, remoteAddr));
    assertTrue(accessManager.hasWriteAccessToPubSubTopic(resourceName, null, hostname, remoteAddr));
    assertTrue(accessManager.isAllowListUser(resourceName, null));
    assertFalse(accessManager.isAclEnabled());
    assertEquals(accessManager.getPrincipalId(null), VeniceControllerAccessManager.UNKNOWN_USER);
  }
}
