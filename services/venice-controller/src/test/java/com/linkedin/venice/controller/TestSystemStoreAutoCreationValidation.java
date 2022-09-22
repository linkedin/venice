package com.linkedin.venice.controller;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.SystemStoreAttributes;
import com.linkedin.venice.meta.SystemStoreAttributesImpl;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.meta.ZKStore;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestSystemStoreAutoCreationValidation {
  @Test
  public void testThrowExceptionWhenAutoCreationFailed() {
    // Setup a regular Venice store
    String testStoreName = "test_store";
    Store veniceStore = new ZKStore(
        testStoreName,
        "test_customer",
        0L,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        0,
        1000L,
        1000L,
        null,
        null,
        1);
    ReadWriteStoreRepository mockRepository = mock(ReadWriteStoreRepository.class);
    when(mockRepository.getStoreOrThrow(testStoreName)).thenReturn(veniceStore);
    HelixVeniceClusterResources mockResources = mock(HelixVeniceClusterResources.class);
    when(mockResources.getStoreMetadataRepository()).thenReturn(mockRepository);
    VeniceHelixAdmin mockHelixAdmin = mock(VeniceHelixAdmin.class);
    when(mockHelixAdmin.getHelixVeniceClusterResources(any())).thenReturn(mockResources);
    Version mockNewVersion = mock(Version.class);
    when(mockNewVersion.getNumber()).thenReturn(2);
    when(mockHelixAdmin.incrementVersionIdempotent(anyString(), anyString(), anyString(), anyInt(), anyInt()))
        .thenReturn(mockNewVersion);
    doCallRealMethod().when(mockHelixAdmin)
        .validateAndMaybeRetrySystemStoreAutoCreation(anyString(), anyString(), any());

    mockHelixAdmin
        .validateAndMaybeRetrySystemStoreAutoCreation("testCluster", testStoreName, VeniceSystemStoreType.META_STORE);

    // Add STARTED system store version and verify that method will throw exception.
    veniceStore.setStoreMetaSystemStoreEnabled(true);
    String systemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(testStoreName);
    Version version = new VersionImpl(systemStoreName, 1, "dummy_id");
    version.setStatus(VersionStatus.STARTED);
    SystemStoreAttributes systemStoreAttributes = new SystemStoreAttributesImpl();
    systemStoreAttributes.setCurrentVersion(0);
    systemStoreAttributes.setLargestUsedVersionNumber(1);
    systemStoreAttributes.setVersions(Collections.singletonList(version));
    veniceStore
        .setSystemStores(Collections.singletonMap(VeniceSystemStoreType.META_STORE.getPrefix(), systemStoreAttributes));
    String expectedExceptionMessage =
        "System store:" + systemStoreName + " push is still ongoing, will check it again. This is not an error.";
    assertThrowsWithExceptionMessage(
        () -> mockHelixAdmin.validateAndMaybeRetrySystemStoreAutoCreation(
            "testCluster",
            testStoreName,
            VeniceSystemStoreType.META_STORE),
        expectedExceptionMessage);

    // Set added system store version to ERROR state to simulate and verify that method will do empty push and throw
    // exception.
    version.setStatus(VersionStatus.ERROR);
    expectedExceptionMessage =
        "System store: " + systemStoreName + " pushed failed. Issuing a new empty push to create version: 2";
    assertThrowsWithExceptionMessage(
        () -> mockHelixAdmin.validateAndMaybeRetrySystemStoreAutoCreation(
            "testCluster",
            testStoreName,
            VeniceSystemStoreType.META_STORE),
        expectedExceptionMessage);

    // Set version list to empty and verify that method will do empty push and throw exception.
    systemStoreAttributes.setVersions(Collections.emptyList());
    expectedExceptionMessage =
        "System store: " + systemStoreName + " pushed failed. Issuing a new empty push to create version: 2";
    assertThrowsWithExceptionMessage(
        () -> mockHelixAdmin.validateAndMaybeRetrySystemStoreAutoCreation(
            "testCluster",
            testStoreName,
            VeniceSystemStoreType.META_STORE),
        expectedExceptionMessage);

  }

  private void assertThrowsWithExceptionMessage(Runnable runnable, String exceptionMessage) {
    try {
      runnable.run();
    } catch (VeniceException e) {
      Assert.assertEquals(exceptionMessage, e.getMessage());
    }
  }
}
