package com.linkedin.venice.controller.repush;

/**
 * Sends requests to external service to trigger repush job for a store
 *
 * The purpose of this interface is to allow both OSS and non-OSS implementations of repush job trigger requests
 * by {@link com.linkedin.venice.controller.logcompaction.CompactionManager}
 */
public interface RepushOrchestrator {
  RepushJobResponse repush(String storeName) throws Exception; // TODO LC: probably need to throw Exception
}
