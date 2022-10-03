package com.linkedin.alpini.base.registry;

/**
 * @author Antony T Curtis &lt;acurtis@linkedin.com&gt;
 */
public interface ShutdownableResource extends Shutdownable {
  /**
   * Returns <tt>true</tt> if this resource has been shut down.
   *
   * @return <tt>true</tt> if this resource has been shut down
   */
  boolean isShutdown();

  /**
   * Returns <tt>true</tt> if the resource has completed shutting down.
   * Note that <tt>isTerminated</tt> is never <tt>true</tt> unless
   * <tt>shutdown</tt> was called first.
   *
   * @return <tt>true</tt> if the resource has completed shutting down.
   */
  boolean isTerminated();

}
