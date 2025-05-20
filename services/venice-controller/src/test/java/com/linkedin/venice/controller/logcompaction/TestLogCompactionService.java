package com.linkedin.venice.controller.logcompaction;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
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
import java.util.concurrent.TimeUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestLogCompactionService {
  private Admin mockAdmin;
  private LogCompactionService logCompactionService;

  private static final String CLUSTER_VENICE_0 = "venice0";
  private static int TEST_LOG_COMPACTION_MAX_REPUSH_PER_COMPACTION_CYCLE = 3;
  private static long TEST_LOG_COMPACTION_INTERVAL_MS = 1000;

  @BeforeMethod(alwaysRun = true)
  public void setUp() throws Exception {
    // mock cluster config
    VeniceControllerClusterConfig mockClusterConfig = mock(VeniceControllerClusterConfig.class);
    when(mockClusterConfig.getLogCompactionThreadCount()).thenReturn(1);
    when(mockClusterConfig.getLogCompactionIntervalMS()).thenReturn(TEST_LOG_COMPACTION_INTERVAL_MS);
    when(mockClusterConfig.getLogContext()).thenReturn(mock(LogContext.class));
    when(mockClusterConfig.isLogCompactionSchedulingEnabled()).thenReturn(true);
    when(mockClusterConfig.getLogCompactionMaxRepushPerCompactionCycle())
        .thenReturn(TEST_LOG_COMPACTION_MAX_REPUSH_PER_COMPACTION_CYCLE);

    // mock admin
    this.mockAdmin = mock(Admin.class);

    List<StoreInfo> mockStoresForCompaction = new ArrayList<>();
    for (int i = 0; i < TEST_LOG_COMPACTION_MAX_REPUSH_PER_COMPACTION_CYCLE + 1; i++) {
      StoreInfo testStoreInfo = new StoreInfo();
      testStoreInfo.setName("test-store-" + i);
      mockStoresForCompaction.add(testStoreInfo);
    }
    when(mockAdmin.getStoresForCompaction(anyString())).thenReturn(mockStoresForCompaction);

    RepushJobResponse mockResponse = new RepushJobResponse();
    mockResponse.setName("test-store");
    when(mockAdmin.repushStore(mock(RepushJobRequest.class))).thenReturn(mockResponse);

    // create LogCompactionService
    this.logCompactionService = new LogCompactionService(mockAdmin, CLUSTER_VENICE_0, mockClusterConfig);
  }

  @Test
  public void testThrottlingRepushQuotaPerCompactionCycle() throws Exception {
    logCompactionService.startInner();

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      verify(mockAdmin, times(TEST_LOG_COMPACTION_MAX_REPUSH_PER_COMPACTION_CYCLE)).repushStore(any());
    });

    logCompactionService.stopInner();
  }
}
