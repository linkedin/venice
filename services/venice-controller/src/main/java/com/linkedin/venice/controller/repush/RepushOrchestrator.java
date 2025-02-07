package com.linkedin.venice.controller.repush;

/**
 * Sends requests to external service to trigger repush job for a store
 *
 * The purpose of this interface is to allow both OSS and non-OSS implementations of repush job trigger requests
 * by {@link com.linkedin.venice.controller.logcompaction.CompactionManager}
 */
public interface RepushOrchestrator {
  /**
   * To pass in configs to initialise RepushOrchestrator implementation
   * This was introduced to allow flexibility in constructor params while keep the constructor method constant
   * -> ReflectUtils.callConstructor() can be left undisturbed if constructor params differ between RepushOrchestrator implementations
   * @param config is a data model for params passed into RepushOrchestrator implementation
   */
  void init(RepushOrchestratorConfig config);

  RepushJobResponse repush(String storeName) throws Exception; // TODO LC: probably need to throw Exception
}
