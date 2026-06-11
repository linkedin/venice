package com.linkedin.venice.controller;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreGraveyard;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.TestUtils;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link ParentVersionOrchestrator}, exercised through the public version API of
 * {@link VeniceParentHelixAdmin} which delegates to the orchestrator. These tests were extracted from
 * {@code TestVeniceParentHelixAdmin} when the parent-controller version metadata orchestration moved into
 * {@link ParentVersionOrchestrator}.
 */
public class TestParentVersionOrchestrator extends AbstractTestVeniceParentHelixAdmin {
  @BeforeMethod
  public void setupTestCase() {
    setupInternalMocks();
    initializeParentAdmin(Optional.empty(), Optional.empty());
  }

  @AfterMethod
  public void cleanupTestCase() {
    super.cleanupTestCase();
  }

  @Test
  public void testGetCurrentVersionInRegionReturnsTheRegionCurrentVersion() {
    String storeName = "current-version-store";
    StoreResponse response = new StoreResponse();
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    store.setCurrentVersion(5);
    response.setStore(StoreInfo.fromStore(store));
    ControllerClient regionClient = mock(ControllerClient.class);
    doReturn(response).when(regionClient).getStore(storeName);
    controllerClients.put("region-with-version", regionClient);

    assertEquals(parentAdmin.getCurrentVersionInRegion(clusterName, storeName, "region-with-version"), 5);
  }

  @Test
  public void testGetCurrentVersionInRegionReturnsMinusOneWhenRegionClientIsMissing() {
    // No controller client registered for the requested region -> the orchestrator returns -1 instead of NPEing.
    assertEquals(parentAdmin.getCurrentVersionInRegion(clusterName, "any-store", "unknown-region"), -1);
  }

  @Test
  public void testGetCurrentVersionInRegionReturnsMinusOneWhenRegionQueryErrors() {
    String storeName = "errored-store";
    StoreResponse errorResponse = new StoreResponse();
    errorResponse.setError("Error querying store for testing.");
    ControllerClient regionClient = mock(ControllerClient.class);
    doReturn(errorResponse).when(regionClient).getStore(storeName);
    controllerClients.put("region-error", regionClient);

    assertEquals(parentAdmin.getCurrentVersionInRegion(clusterName, storeName, "region-error"), -1);
  }

  @Test
  public void testGetLargestUsedVersionAggregatesMaxAcrossParentAndChildRegions() {
    String storeName = "largest-used-version-store";
    // Parent's own largest-used version is 3; the aggregate must climb to the highest child value.
    doReturn(3).when(internalAdmin).getLargestUsedVersion(clusterName, storeName);
    controllerClients.clear();
    controllerClients.put("region-high", childWithLargestUsedVersion(storeName, 7)); // 7 > 3 -> picked up
    controllerClients.put("region-low", childWithLargestUsedVersion(storeName, 2)); // 2 < 7 -> ignored

    assertEquals(parentAdmin.getLargestUsedVersion(clusterName, storeName), 7);
  }

  @Test
  public void testGetLargestUsedVersionFromStoreGraveyardAggregatesMaxAcrossGraveyardAndChildRegions() {
    String storeName = "graveyard-version-store";
    StoreGraveyard graveyard = mock(StoreGraveyard.class);
    doReturn(graveyard).when(internalAdmin).getStoreGraveyard();
    // The graveyard's largest-used version is 3; the aggregate must climb to the highest child value.
    doReturn(3).when(graveyard).getLargestUsedVersionNumber(storeName);
    controllerClients.clear();
    controllerClients.put("region-high", childWithLargestUsedVersion(storeName, 9)); // 9 > 3 -> picked up
    controllerClients.put("region-low", childWithLargestUsedVersion(storeName, 1)); // 1 < 9 -> ignored

    assertEquals(parentAdmin.getLargestUsedVersionFromStoreGraveyard(clusterName, storeName), 9);
  }

  @Test
  public void testGetCurrentVersionIsUnsupportedOnParent() {
    Assert.expectThrows(
        VeniceUnsupportedOperationException.class,
        () -> parentAdmin.getCurrentVersion(clusterName, "any-store"));
  }

  @Test
  public void testFutureAndBackupVersionAreNonExistingOnParent() {
    assertEquals(parentAdmin.getFutureVersion(clusterName, "any-store"), Store.NON_EXISTING_VERSION);
    assertEquals(parentAdmin.getBackupVersion(clusterName, "any-store"), Store.NON_EXISTING_VERSION);
  }

  @Test
  public void testSetStoreCurrentVersionIsUnsupportedOnParent() {
    Assert.expectThrows(
        VeniceUnsupportedOperationException.class,
        () -> parentAdmin.setStoreCurrentVersion(clusterName, "any-store", 1));
  }

  @Test
  public void testSetStoreLargestUsedVersionIsUnsupportedOnParent() {
    Assert.expectThrows(
        VeniceUnsupportedOperationException.class,
        () -> parentAdmin.setStoreLargestUsedVersion(clusterName, "any-store", 1));
  }

  @Test
  public void testSetStoreLargestUsedRTVersionIsUnsupportedOnParent() {
    Assert.expectThrows(
        VeniceUnsupportedOperationException.class,
        () -> parentAdmin.setStoreLargestUsedRTVersion(clusterName, "any-store", 1));
  }

  @Test
  public void testVersionsForStoreDelegatesToInternalAdmin() {
    String storeName = "versions-store";
    List<Version> versions = Collections.emptyList();
    doReturn(versions).when(internalAdmin).versionsForStore(clusterName, storeName);

    assertEquals(parentAdmin.versionsForStore(clusterName, storeName), versions);
  }

  private ControllerClient childWithLargestUsedVersion(String storeName, int version) {
    ControllerClient client = mock(ControllerClient.class);
    VersionResponse versionResponse = new VersionResponse();
    versionResponse.setVersion(version);
    doReturn(versionResponse).when(client).getStoreLargestUsedVersion(clusterName, storeName);
    return client;
  }
}
