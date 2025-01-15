package com.linkedin.venice.acl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Set;
import javax.security.auth.x500.X500Principal;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class NoOpDynamicAccessControllerTest {
  private NoOpDynamicAccessController accessController;

  @BeforeMethod
  public void setUp() {
    accessController = NoOpDynamicAccessController.INSTANCE;
  }

  @Test
  public void testAlwaysTrueMethods() throws AclException {
    // Test all methods that always return true
    assertTrue(accessController.hasAccess(null, "resource", "GET"), "Expected hasAccess to return true.");
    assertTrue(accessController.hasAccessToTopic(null, "topic", "READ"), "Expected hasAccessToTopic to return true.");
    assertTrue(
        accessController.hasAccessToAdminOperation(null, "operation"),
        "Expected hasAccessToAdminOperation to return true.");
    assertTrue(accessController.isAllowlistUsers(null, "resource", "GET"), "Expected isAllowlistUsers to return true.");
    assertTrue(accessController.hasAcl("resource"), "Expected hasAcl to return true.");
  }

  @Test
  public void testInitReturnsSameInstance() {
    DynamicAccessController initializedController = accessController.init(Collections.emptyList());
    assertSame(initializedController, accessController, "Expected the same instance after init.");
  }

  @Test
  public void testGetPrincipalId() {
    String expectedId = "CN=Test User,OU=Engineering,O=LinkedIn,C=US";
    X509Certificate mockCertificate = mock(X509Certificate.class);
    when(mockCertificate.getSubjectX500Principal()).thenReturn(new X500Principal(expectedId));

    assertEquals(accessController.getPrincipalId(mockCertificate), expectedId, "Expected the correct principal ID.");
    assertEquals(
        accessController.getPrincipalId(null),
        NoOpDynamicAccessController.USER_UNKNOWN,
        "Expected USER_UNKNOWN for null certificate.");
  }

  @Test
  public void testGetAccessControlledResources() {
    Set<String> resources = accessController.getAccessControlledResources();
    assertTrue(resources.isEmpty(), "Expected access-controlled resources to be empty.");
  }

  @Test
  public void testIsFailOpen() {
    assertFalse(accessController.isFailOpen(), "Expected isFailOpen to return false.");
  }
}
