package com.linkedin.venice.controller;

import static com.linkedin.venice.common.VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE;
import static com.linkedin.venice.common.VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE;
import static com.linkedin.venice.common.VeniceSystemStoreType.META_STORE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.pushmonitor.PushMonitorDelegator;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.utils.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestUserSystemStoreLifeCycleHelper {
  private static final String CLUSTER_NAME = "test-cluster";
  private static final String STORE_NAME = "test-store";
  private static final String META_STORE_NAME = META_STORE.getSystemStoreName(STORE_NAME);
  private static final String DAVINCI_STORE_NAME = DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(STORE_NAME);
  private static final String HEARTBEAT_STORE_NAME = BATCH_JOB_HEARTBEAT_STORE.getSystemStoreName(STORE_NAME);
  private static final String UNKNOWN_STORE_NAME = "unknown_system_store";
  private static final Logger LOGGER = LogManager.getLogger(TestUserSystemStoreLifeCycleHelper.class);

  private VeniceHelixAdmin mockAdmin;
  private ReadWriteStoreRepository mockStoreRepository;
  private PushMonitorDelegator mockPushMonitor;
  private MetaStoreWriter mockMetaStoreWriter;
  private Store mockStore;
  private PushStatusStoreWriter mockPushStatusStoreWriter;

  @BeforeMethod
  public void setUp() {
    mockAdmin = mock(VeniceHelixAdmin.class);
    mockStoreRepository = mock(ReadWriteStoreRepository.class);
    mockPushMonitor = mock(PushMonitorDelegator.class);
    mockMetaStoreWriter = mock(MetaStoreWriter.class);
    mockStore = mock(Store.class);
    mockPushStatusStoreWriter = mock(PushStatusStoreWriter.class);

    when(mockAdmin.getPushStatusStoreWriter()).thenReturn(mockPushStatusStoreWriter);
    when(mockAdmin.isParent()).thenReturn(false);

    // Mock store repository to return our mock store
    when(mockStoreRepository.getStore(anyString())).thenReturn(mockStore);

    // Mock push monitor cleanup
    doNothing().when(mockPushMonitor).cleanupStoreStatus(anyString());

    // Mock version cleanup
    doNothing().when(mockAdmin).truncateOldTopics(anyString(), any(Store.class), anyBoolean());
  }

  @Test
  public void testDeleteMetaSystemStore() {
    // Test when running in child fabric - should truncate RT topic
    when(mockAdmin.isParent()).thenReturn(false);
    // Test deleting a META_STORE system store
    UserSystemStoreLifeCycleHelper.deleteSystemStore(
        mockAdmin,
        mockStoreRepository,
        mockPushMonitor,
        CLUSTER_NAME,
        META_STORE_NAME,
        false, // isStoreMigrating
        mockMetaStoreWriter,
        LOGGER);

    // Verify meta store writer cleanup is called
    verify(mockMetaStoreWriter).removeMetaStoreWriter(META_STORE_NAME);
    verify(mockAdmin).deleteAllVersionsInStore(CLUSTER_NAME, META_STORE_NAME);
    verify(mockPushMonitor).cleanupStoreStatus(META_STORE_NAME);
    verify(mockAdmin).truncateKafkaTopic(Utils.composeRealTimeTopic(META_STORE_NAME));
    verify(mockAdmin).truncateOldTopics(CLUSTER_NAME, mockStore, true);
  }

  @Test
  public void testDeleteDaVinciPushStatusSystemStore() {
    // Test when running in child fabric - should truncate RT topic
    when(mockAdmin.isParent()).thenReturn(false);
    // Test deleting a DAVINCI_PUSH_STATUS_STORE system store
    UserSystemStoreLifeCycleHelper.deleteSystemStore(
        mockAdmin,
        mockStoreRepository,
        mockPushMonitor,
        CLUSTER_NAME,
        DAVINCI_STORE_NAME,
        false, // isStoreMigrating
        mockMetaStoreWriter,
        LOGGER);

    // Verify push status store writer cleanup is called
    verify(mockPushStatusStoreWriter).removePushStatusStoreVeniceWriter(STORE_NAME);
    verify(mockAdmin).deleteAllVersionsInStore(CLUSTER_NAME, DAVINCI_STORE_NAME);
    verify(mockPushMonitor).cleanupStoreStatus(DAVINCI_STORE_NAME);
    verify(mockAdmin).truncateKafkaTopic(Utils.composeRealTimeTopic(DAVINCI_STORE_NAME));
    verify(mockAdmin).truncateOldTopics(CLUSTER_NAME, mockStore, true);
  }

  @Test
  public void testDeleteBatchJobHeartbeatSystemStore() {
    // Test when running in child fabric - should truncate RT topic
    when(mockAdmin.isParent()).thenReturn(false);
    // Test deleting a BATCH_JOB_HEARTBEAT_STORE system store
    // This should log an error but not throw an exception
    UserSystemStoreLifeCycleHelper.deleteSystemStore(
        mockAdmin,
        mockStoreRepository,
        mockPushMonitor,
        CLUSTER_NAME,
        HEARTBEAT_STORE_NAME,
        false, // isStoreMigrating
        mockMetaStoreWriter,
        LOGGER);

    // Verify basic cleanup is still performed
    verify(mockAdmin).deleteAllVersionsInStore(CLUSTER_NAME, HEARTBEAT_STORE_NAME);
    verify(mockPushMonitor).cleanupStoreStatus(HEARTBEAT_STORE_NAME);
    verify(mockAdmin).truncateKafkaTopic(Utils.composeRealTimeTopic(HEARTBEAT_STORE_NAME));
    verify(mockAdmin).truncateOldTopics(CLUSTER_NAME, mockStore, true);
  }

  @Test
  public void testDeleteUnknownSystemStore() {
    // Test deleting an unknown system store type - should throw VeniceException
    VeniceException exception = expectThrows(VeniceException.class, () -> {
      UserSystemStoreLifeCycleHelper.deleteSystemStore(
          mockAdmin,
          mockStoreRepository,
          mockPushMonitor,
          CLUSTER_NAME,
          UNKNOWN_STORE_NAME,
          false, // isStoreMigrating
          mockMetaStoreWriter,
          LOGGER);
    });

    // Verify the exception message
    assertTrue(exception.getMessage().contains("Unknown system store type: " + UNKNOWN_STORE_NAME));
  }

  @Test
  public void testDeleteSystemStoreWhenMigrating() {
    // Test deleting a system store when the store is migrating
    UserSystemStoreLifeCycleHelper.deleteSystemStore(
        mockAdmin,
        mockStoreRepository,
        mockPushMonitor,
        CLUSTER_NAME,
        META_STORE_NAME,
        true, // isStoreMigrating
        mockMetaStoreWriter,
        LOGGER);

    // Verify meta store writer cleanup is still called
    verify(mockAdmin).deleteAllVersionsInStore(CLUSTER_NAME, META_STORE_NAME);
    verify(mockPushMonitor).cleanupStoreStatus(META_STORE_NAME);

    verify(mockMetaStoreWriter, never()).removeMetaStoreWriter(META_STORE_NAME);
    verify(mockAdmin, never()).truncateKafkaTopic(Utils.composeRealTimeTopic(META_STORE_NAME));
  }

  @Test
  public void testDeleteSystemStoreWhenStoreNotInRepository() {
    // Test when the store doesn't exist in the repository
    when(mockStoreRepository.getStore(anyString())).thenReturn(null);

    UserSystemStoreLifeCycleHelper.deleteSystemStore(
        mockAdmin,
        mockStoreRepository,
        mockPushMonitor,
        CLUSTER_NAME,
        META_STORE_NAME,
        false, // isStoreMigrating
        mockMetaStoreWriter,
        LOGGER);

    // Verify the rest of the cleanup still happens
    verify(mockMetaStoreWriter).removeMetaStoreWriter(META_STORE_NAME);
    verify(mockAdmin).deleteAllVersionsInStore(CLUSTER_NAME, META_STORE_NAME);
    verify(mockPushMonitor).cleanupStoreStatus(META_STORE_NAME);

    // Verify truncateOldTopics is not called when store is not in repository
    verify(mockAdmin, never()).truncateOldTopics(anyString(), any(Store.class), anyBoolean());
  }

  @Test
  public void testDeleteSystemStoreInParentFabric() {
    // Test when running in parent fabric - should not truncate RT topic
    when(mockAdmin.isParent()).thenReturn(true);

    UserSystemStoreLifeCycleHelper.deleteSystemStore(
        mockAdmin,
        mockStoreRepository,
        mockPushMonitor,
        CLUSTER_NAME,
        META_STORE_NAME,
        false, // isStoreMigrating
        mockMetaStoreWriter,
        LOGGER);

    // Verify meta store writer cleanup is called
    verify(mockMetaStoreWriter).removeMetaStoreWriter(META_STORE_NAME);
    verify(mockAdmin).deleteAllVersionsInStore(CLUSTER_NAME, META_STORE_NAME);
    verify(mockPushMonitor).cleanupStoreStatus(META_STORE_NAME);

    // Verify RT topic is not truncated in parent fabric
    verify(mockAdmin, never()).truncateKafkaTopic(anyString());
  }
}
