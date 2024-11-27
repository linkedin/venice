package com.linkedin.venice.controller.repush;

public interface RepushOrchestrator {
  RepushJobResponse repush(String storeName);
}
