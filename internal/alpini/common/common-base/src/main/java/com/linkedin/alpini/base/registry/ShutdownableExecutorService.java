package com.linkedin.alpini.base.registry;

import com.linkedin.alpini.base.concurrency.ExecutorService;


/**
 * An {@link ExecutorService} interface which also extends the
 * {@link Shutdownable} interface.
 *
 * @author Antony T Curtis &lt;acurtis@linkedin.com&gt;
 */
public interface ShutdownableExecutorService extends ExecutorService, ShutdownableResource {
  /**
   * Returns an object that implements the given interface to allow access to
   * non-standard methods, or standard methods not exposed by the proxy.
   *
   * @param clazz A Class defining an interface that the result must implement.
   * @return an object that implements the interface. May be a proxy for the actual implementing object.
   * @throws ClassCastException If no object found that implements the interface
   */
  <T extends ExecutorService> T unwrap(Class<T> clazz);
}
