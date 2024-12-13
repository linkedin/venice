package com.linkedin.venice.endToEnd.logcompaction;

import com.linkedin.venice.controller.repush.RepushJobResponse;
import com.linkedin.venice.controller.repush.RepushOrchestrator;


/**
 * This is a dummy no-op implementation of {@link RepushOrchestrator} for testing purposes in {@link TestLogCompactionService}.
 */
public class TestRepushOrchestratorImpl implements RepushOrchestrator {
  public TestRepushOrchestratorImpl() {
  }

  @Override
  public RepushJobResponse repush(String storeName) {
    return null;
  }
}
