package com.linkedin.venice.controller;

public interface RepushOrchestratorProvider {
  public void repush(String storeName);
}
