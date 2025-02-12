package com.linkedin.venice.controller.logcompaction;

import static org.mockito.Mockito.*;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.meta.StoreInfo;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestLogCompactionService {
  private LogCompactionService logCompactionService;
  private Admin admin;
  private VeniceControllerMultiClusterConfig multiClusterConfigs;

  @BeforeMethod
  public void setUp() {
    admin = mock(Admin.class);
    multiClusterConfigs = mock(VeniceControllerMultiClusterConfig.class);
    // when(multiClusterConfigs.getScheduledLogCompactionThreadCount()).thenReturn(1);
    when(multiClusterConfigs.getClusters()).thenReturn(Collections.singleton("test_cluster"));
    when(admin.getStoresForCompaction(anyString())).thenReturn(Collections.singletonList(mock(StoreInfo.class)));

    logCompactionService = new LogCompactionService(admin, multiClusterConfigs);
  }

  // test scheduling
  @Test
  public void testScheduledExecution() throws Exception {
    // Test params
    int testDuration = 30;
    int scheduleInterval = 5;
    int expectedCompactStoreInvocationCount = 2;

    CountDownLatch latch = new CountDownLatch(expectedCompactStoreInvocationCount);

    // Mocks
    when(multiClusterConfigs.getLogCompactionIntervalMS()).thenReturn(TimeUnit.SECONDS.toMillis(scheduleInterval));

    String storeForCompaction = "hello";
    StoreInfo mockStoreInfo = mock(StoreInfo.class);
    doReturn(storeForCompaction).when(mockStoreInfo).getName();

    // compactStores() invoked for non-empty list returned
    when(admin.getStoresForCompaction(anyString())).thenReturn(Collections.emptyList());
    when(admin.getStoresForCompaction(anyString())).thenReturn(Collections.singletonList(mockStoreInfo));
    // getStoresForCompaction() returns nulls after first 2 invocations

    doAnswer(ctx -> {
      latch.countDown();
      return null;
    }).when(admin).compactStore(anyString());

    // Testing
    logCompactionService.startInner();
    if (latch.await(testDuration, TimeUnit.SECONDS)) {
      logCompactionService.stopInner();
    }
    // ensures you have invoked the compact store once
    verify(admin, atLeast(2)).getStoresForCompaction(any());
    verify(admin, times(expectedCompactStoreInvocationCount)).compactStore(storeForCompaction);
  }

  // TODO: test LogCompactionTask::run() focus on edge cases
}
