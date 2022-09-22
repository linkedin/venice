package com.linkedin.alpini.base.pool;

/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface PoolStats {
  /**
   * Get the total number of pool objects created between
   * the starting of the Pool and the call to getStats().
   * Does not include create errors.
   * @return The total number of pool objects created
   */
  long getTotalCreated();

  /**
   * Get the total number of pool objects destroyed between
   * the starting of the Pool and the call to getStats().
   * Includes lifecycle validation failures, disposes,
   * and timed-out objects, but does not include destroy errors.
   * @return The total number of pool objects destroyed
   */
  long getTotalDestroyed();

  /**
   * Get the total number of lifecycle create errors between
   * the starting of the Pool and the call to getStats().
   * @return The total number of create errors
   */
  long getTotalCreateErrors();

  /**
   * Get the total number of lifecycle destroy errors between
   * the starting of the Pool and the call to getStats().
   * @return The total number of destroy errors
   */
  long getTotalDestroyErrors();

  /**
   * Get the number of pool objects checked out at the time of
   * the call to getStats().
   * @return The number of checked out pool objects
   */
  double getCheckedOut1min();

  double getCheckedOut5min();

  double getCheckedOut15min();

  int getMaxCheckedOut1min();

  double getCheckedOutTimeAvg();

  double getCheckedOutTime50Pct();

  double getCheckedOutTime95Pct();

  double getCheckedOutTime99Pct();

  /**
   * Get the configured maximum pool size.
   * @return The maximum pool size
   */
  int getMaxPoolSize();

  /**
   * Get the configured minimum pool size.
   * @return The minimum pool size
   */
  int getMinPoolSize();

  /**
   * Get the pool size at the time of the call to getStats().
   * @return The pool size
   */
  int getPoolSize();

  /**
   * Get the number of objects that are idle(not checked out)
   * in the pool.
   * @return The number of idle objects
   */
  int getIdleCount();

  double getWaiters1min();

  double getWaiters5min();

  double getWaiters15min();

  /**
   * Get the average wait time to get a pooled object.
   * @return The average wait time.
   */
  double getWaitTimeAvg();

  /**
   * Get the 50 percentage wait time to get a pooled object.
   * @return 50 percentage wait time.
   */
  double getWaitTime50Pct();

  /**
   * Get the 95 percentage wait time to get a pooled object.
   * @return 95 percentage wait time.
   */
  double getWaitTime95Pct();

  /**
   * Get the 99 percentage wait time to get a pooled object.
   * @return 99 percentage wait time.
   */
  double getWaitTime99Pct();

  /**
   * Get stats collected from {@link AsyncPool.LifeCycle}
   * @return Lifecycle stats
   */
  LifeCycleStats getLifecycleStats();

  interface LifeCycleStats {
    /**
     * Get the average time to create an object.
     * @return The average create time.
     */
    double getCreateTimeAvg();

    /**
     * Get the 50 percentage time to create an object.
     * @return 50 percentage create time.
     */
    double getCreateTime50Pct();

    /**
     * Get the 95 percentage time to create an object.
     * @return 95 percentage create time.
     */
    double getCreateTime95Pct();

    /**
     * Get the 99 percentage time to create an object.
     * @return 99 percentage create time.
     */
    double getCreateTime99Pct();

    /**
     * Get the average time to create an object.
     * @return The average create time.
     */
    double getTestTimeAvg();

    /**
     * Get the 50 percentage time to create an object.
     * @return 50 percentage create time.
     */
    double getTestTime50Pct();

    /**
     * Get the 95 percentage time to create an object.
     * @return 95 percentage create time.
     */
    double getTestTime95Pct();

    /**
     * Get the 99 percentage time to create an object.
     * @return 99 percentage create time.
     */
    double getTestTime99Pct();

    /**
     * Get the average time to create an object.
     * @return The average create time.
     */
    double getDestroyTimeAvg();

    /**
     * Get the 50 percentage time to create an object.
     * @return 50 percentage create time.
     */
    double getDestroyTime50Pct();

    /**
     * Get the 95 percentage time to create an object.
     * @return 95 percentage create time.
     */
    double getDestroyTime95Pct();

    /**
     * Get the 99 percentage time to create an object.
     * @return 99 percentage create time.
     */
    double getDestroyTime99Pct();

  }
}
