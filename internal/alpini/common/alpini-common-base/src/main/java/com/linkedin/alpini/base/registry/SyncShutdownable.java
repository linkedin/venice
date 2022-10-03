package com.linkedin.alpini.base.registry;

public interface SyncShutdownable {
  /**
   * Shutdown synchronously
   */
  void shutdownSynchronously();

}
