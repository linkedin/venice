package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.authorization.AceEntry;
import com.linkedin.venice.authorization.AclBinding;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.authorization.Method;
import com.linkedin.venice.authorization.Permission;
import com.linkedin.venice.authorization.Principal;
import com.linkedin.venice.authorization.Resource;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.SystemStoreAttributes;
import com.linkedin.venice.meta.SystemStoreAttributesImpl;
import com.linkedin.venice.utils.TestUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestVeniceHelixAdminWithAcl {
  private static final String STORE_NAME = "test-store";
  private static final String CLUSTER_NAME = "test-cluster";

  private MockVeniceAuthorizer authorizerService;
  private Store store;
  private VeniceHelixAdmin veniceAdminMock;

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    // Setup authorizer with some default permissions
    authorizerService = new MockVeniceAuthorizer();

    // Add some default ACLs for testing
    AclBinding defaultAcl = new AclBinding(new Resource(STORE_NAME));
    defaultAcl.addAceEntry(new AceEntry(new Principal("admin"), Method.Read, Permission.ALLOW));
    defaultAcl.addAceEntry(new AceEntry(new Principal("admin"), Method.Write, Permission.ALLOW));
    authorizerService.setAcls(defaultAcl);

    // Setup store with proper configuration
    store = TestUtils.createTestStore(STORE_NAME, "owner", System.currentTimeMillis());
    store.setMigrating(false);
    store.setSystemStores(new HashMap<>());

    // Create a mock VeniceHelixAdmin
    veniceAdminMock = mock(VeniceHelixAdmin.class);

    // Set up the mock to use our test authorizer service
    when(veniceAdminMock.getAuthorizerService()).thenReturn(Optional.of(authorizerService));

    // Set up the real method call for cleanupAclsForStore
    doCallRealMethod().when(veniceAdminMock).cleanupAclsForStore(any(Store.class), anyString(), anyString());

    // Reset counters before each test
    authorizerService.setAclsCounter = 0;
    authorizerService.clearAclCounter = 0;
  }

  @Test
  public void testCleanupAclsForStore() {
    // Setup test data with multiple ACEs
    AclBinding aclBinding = new AclBinding(new Resource(STORE_NAME));
    aclBinding.addAceEntry(new AceEntry(new Principal("user:test"), Method.Read, Permission.ALLOW));
    aclBinding.addAceEntry(new AceEntry(new Principal("group:admins"), Method.Write, Permission.ALLOW));
    authorizerService.setAcls(aclBinding);

    // Verify initial state
    AclBinding initialAcl = authorizerService.describeAcls(new Resource(STORE_NAME));
    assertEquals(initialAcl.countAceEntries(), 2, "Should have 2 ACEs initially");

    veniceAdminMock.cleanupAclsForStore(store, STORE_NAME, CLUSTER_NAME);

    // Verify the authorizer service was called to clear ACLs for the main store
    assertEquals(authorizerService.clearAclCounter, 1, "clearAcls should be called once");

    // Verify the ACLs were actually cleared
    AclBinding clearedAcl = authorizerService.describeAcls(new Resource(STORE_NAME));
    assertEquals(clearedAcl.countAceEntries(), 0, "ACLs should be cleared for the main store");
  }

  @Test
  public void testCleanupAclsForStoreWhenStoreIsNull() {
    // Reset counter
    authorizerService.clearAclCounter = 0;

    // Call with null store - should log a warning but not throw
    veniceAdminMock.cleanupAclsForStore(null, STORE_NAME, CLUSTER_NAME);

    // Verify no interactions with authorizer service
    assertEquals(authorizerService.clearAclCounter, 0, "clearAcls should not be called for null store");
  }

  @Test
  public void testCleanupAclsForStoreWhenStoreIsMigrating() {
    // Mark store as migrating
    store.setMigrating(true);

    // Reset counter
    authorizerService.clearAclCounter = 0;

    // Call the method under test
    veniceAdminMock.cleanupAclsForStore(store, STORE_NAME, CLUSTER_NAME);

    // Verify no interactions with authorizer service when store is migrating
    assertEquals(authorizerService.clearAclCounter, 0, "clearAcls should not be called for migrating store");
  }

  @Test
  public void testCleanupAclsForStoreWhenAuthorizerServiceNotPresent() {
    // Create a new mock VeniceHelixAdmin without authorizer service
    VeniceHelixAdmin mockAdmin = mock(VeniceHelixAdmin.class);

    // Set up the mock to return empty authorizer service
    when(mockAdmin.getAuthorizerService()).thenReturn(Optional.empty());

    // Set up the real method call for cleanupAclsForStore
    doCallRealMethod().when(mockAdmin).cleanupAclsForStore(any(Store.class), anyString(), anyString());

    // Reset counter
    authorizerService.clearAclCounter = 0;

    mockAdmin.cleanupAclsForStore(store, STORE_NAME, CLUSTER_NAME);

    // Verify no interactions with our test authorizer service (since the mock has no authorizer)
    assertEquals(authorizerService.clearAclCounter, 0, "clearAcls should not be called when authorizer is not present");
  }

  @Test
  public void testCleanupAclsForStoreWhenAuthorizerThrowsException() {
    AclBinding aclBinding = new AclBinding(new Resource(STORE_NAME));
    aclBinding.addAceEntry(new AceEntry(new Principal("admin"), Method.Read, Permission.ALLOW));
    aclBinding.addAceEntry(new AceEntry(new Principal("admin"), Method.Write, Permission.ALLOW));
    authorizerService.setAcls(aclBinding);

    // Create a mock authorizer that throws exceptions
    AuthorizerService mockAuthorizerService = mock(AuthorizerService.class);
    doThrow(new RuntimeException("Test exception")).when(mockAuthorizerService).clearAcls(any(Resource.class));

    // Create a new mock admin with the throwing authorizer
    VeniceHelixAdmin mockAdmin = mock(VeniceHelixAdmin.class);
    when(mockAdmin.getAuthorizerService()).thenReturn(Optional.of(mockAuthorizerService));
    doCallRealMethod().when(mockAdmin).cleanupAclsForStore(any(Store.class), anyString(), anyString());

    // Verify that VeniceException is thrown
    expectThrows(VeniceException.class, () -> mockAdmin.cleanupAclsForStore(store, STORE_NAME, CLUSTER_NAME));

    // Verify the exception was thrown after the clearAcls call
    verify(mockAuthorizerService, times(1)).clearAcls(any(Resource.class));
  }

  @Test
  public void testCleanupAclsForStoreWithMultipleSystemStores() {
    // Setup test data with multiple ACEs
    AclBinding aclBinding = new AclBinding(new Resource(STORE_NAME));
    aclBinding.addAceEntry(new AceEntry(new Principal("admin"), Method.Read, Permission.ALLOW));
    aclBinding.addAceEntry(new AceEntry(new Principal("admin"), Method.Write, Permission.ALLOW));
    authorizerService.setAcls(aclBinding);

    // Enable system stores on the Store object
    // The implementation only cleans up ACLs for "enabled" system stores
    store.setDaVinciPushStatusStoreEnabled(true);
    store.setStoreMetaSystemStoreEnabled(true);

    // Setup system store attributes
    Map<String, SystemStoreAttributes> systemStores = new HashMap<>();
    SystemStoreAttributesImpl davinciAttrs = new SystemStoreAttributesImpl();
    davinciAttrs.setCurrentVersion(1);
    systemStores.put(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getPrefix(), davinciAttrs);

    SystemStoreAttributesImpl metaAttrs = new SystemStoreAttributesImpl();
    metaAttrs.setCurrentVersion(1);
    systemStores.put(VeniceSystemStoreType.META_STORE.getPrefix(), metaAttrs);

    store.setSystemStores(systemStores);

    // Add ACLs for enabled system stores
    int expectedSystemStoreCount = 2; // DaVinci Push Status + Meta Store
    for (VeniceSystemStoreType systemStoreType: new VeniceSystemStoreType[] {
        VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE, VeniceSystemStoreType.META_STORE }) {
      String systemStoreName = systemStoreType.getSystemStoreName(STORE_NAME);
      AclBinding systemAcl = new AclBinding(new Resource(systemStoreName));
      systemAcl.addAceEntry(new AceEntry(new Principal("admin"), Method.Read, Permission.ALLOW));
      systemAcl.addAceEntry(new AceEntry(new Principal("admin"), Method.Write, Permission.ALLOW));
      authorizerService.setAcls(systemAcl);
    }

    // Reset counter
    authorizerService.clearAclCounter = 0;

    // Call the method under test
    veniceAdminMock.cleanupAclsForStore(store, STORE_NAME, CLUSTER_NAME);

    // Verify the authorizer service was called to clear ACLs
    // Should be called once for main store + once for each system store
    int expectedClearCalls = 1 + expectedSystemStoreCount;
    assertEquals(
        authorizerService.clearAclCounter,
        expectedClearCalls,
        "clearAcls should be called " + expectedClearCalls + " times (1 main store + " + expectedSystemStoreCount
            + " system stores)");
  }
}
