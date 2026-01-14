package com.linkedin.venice.controller.logcompaction;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controller.repush.RepushJobRequest;
import com.linkedin.venice.controllerapi.RepushJobResponse;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.TestUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class LogCompactionServiceTest {
  private Admin mockAdmin;
  private LogCompactionService logCompactionService;

  private static final String CLUSTER_VENICE_0 = "venice0";
  private static final String TEST_STORE_NAME = "test-store";
  private static long TEST_LOG_COMPACTION_INTERVAL_MS = 1000;

  @BeforeMethod(alwaysRun = true)
  public void setUp() throws Exception {
    // mock cluster config
    VeniceControllerClusterConfig mockClusterConfig = mock(VeniceControllerClusterConfig.class);
    when(mockClusterConfig.getLogCompactionThreadCount()).thenReturn(1);
    when(mockClusterConfig.getLogCompactionIntervalMS()).thenReturn(TEST_LOG_COMPACTION_INTERVAL_MS);
    when(mockClusterConfig.getLogContext()).thenReturn(mock(LogContext.class));
    when(mockClusterConfig.isLogCompactionSchedulingEnabled()).thenReturn(true);

    // mock admin
    this.mockAdmin = mock(Admin.class);

    // create LogCompactionService
    this.logCompactionService = new LogCompactionService(mockAdmin, CLUSTER_VENICE_0, mockClusterConfig);
  }

  @Test
  public void testCompactStoresInCluster() throws Exception {
    List<StoreInfo> mockStoresForCompaction = new ArrayList<>();
    StoreInfo testStoreInfo = new StoreInfo();
    testStoreInfo.setName(TEST_STORE_NAME);
    mockStoresForCompaction.add(testStoreInfo);
    when(mockAdmin.getStoresForCompaction(anyString())).thenReturn(mockStoresForCompaction);

    RepushJobResponse mockResponse = new RepushJobResponse();
    mockResponse.setName(TEST_STORE_NAME);
    when(mockAdmin.repushStore(mock(RepushJobRequest.class))).thenReturn(mockResponse);

    logCompactionService.startInner();
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      verify(mockAdmin, atLeastOnce()).repushStore(any());
    });
    logCompactionService.stopInner();
  }

  @Test
  public void testDropAndLogWhenExecutorActive() {
    Logger mockLogger = mock(Logger.class);

    RejectedExecutionHandler handler = LogCompactionService.createDropAndLogPolicy(mockLogger);

    ThreadPoolExecutor executor = mock(ThreadPoolExecutor.class);
    when(executor.isShutdown()).thenReturn(false);
    when(executor.getActiveCount()).thenReturn(2);
    when(executor.getPoolSize()).thenReturn(4);
    when(executor.getQueue()).thenReturn(new SynchronousQueue<>());

    handler.rejectedExecution(mock(Runnable.class), executor);

    verify(mockLogger).warn(contains("Dropping log compaction task"), eq(2), eq(0), eq(4));
  }

}
