package com.linkedin.alpini.base.registry;

import java.util.concurrent.TimeoutException;


public interface Shutdownable {
  /**
   * Starts the shutdown process.
   * It is recommended to perform the actual shutdown activity in a separate thread
   * from the thread that calls shutdown
   */
  void shutdown();

  /**
   * Waits for shutdown to complete
   * @throws java.lang.InterruptedException when the wait is interrupted
   * @throws java.lang.IllegalStateException when the method is invoked when the shutdown has yet to be started
   */
  void waitForShutdown() throws InterruptedException, IllegalStateException;

  /**
   * Waits for shutdown to complete with a timeout
   * @param timeoutInMs number of milliseconds to wait before throwing TimeoutException
   * @throws java.lang.InterruptedException when the wait is interrupted
   * @throws java.lang.IllegalStateException when the method is invoked when the shutdown has yet to be started
   * @throws TimeoutException when the operation times out
   */
  void waitForShutdown(long timeoutInMs) throws InterruptedException, IllegalStateException, TimeoutException;

  static boolean shutdownNow(Object object) {
    if (object instanceof SyncShutdownable) {
      ((SyncShutdownable) object).shutdownSynchronously();
      return true;
    } else if (object instanceof Shutdownable) {
      ((Shutdownable) object).shutdown();
      try {
        ((Shutdownable) object).waitForShutdown();
        return true;
      } catch (InterruptedException ignored) {
      }
    }
    return false;
  }
}
