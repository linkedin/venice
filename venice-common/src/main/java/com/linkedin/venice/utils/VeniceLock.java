package com.linkedin.venice.utils;

import com.linkedin.venice.stats.VeniceLockStats;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Venice wrapper around a {@link Lock} object to record metrics and emit logs when lock acquisition is taking too long.
 */
public class VeniceLock {
  protected static final Logger LOGGER = LogManager.getLogger(VeniceLock.class);
  private static final long DEFAULT_LOCK_OPERATION_REPORTING_THRESHOLD_MS = 1000;
  private final Lock lock;
  private final String lockDescription;
  private final long reportingThresholdMs;
  private final VeniceLockStats lockStats;
  private final Map<Long, Long> lockAcquiredTimeMap = new ConcurrentHashMap<>();

  /**
   * @param lock underlying {@link Lock}.
   * @param lockDescription that describes the lock to give context and information in logs and metrics.
   * @param metricsRepository to emit related metrics to.
   * @param reportingThresholdMs to emit metrics and logs when a thread is taking a long time to acquire the lock.
   *                             The same threshold is also used to emit metrics and logs upon releasing a lock that was
   *                             been held for extended period of time.
   *
   */
  public VeniceLock(Lock lock, String lockDescription, MetricsRepository metricsRepository, long reportingThresholdMs) {
    this.lock = lock;
    this.lockDescription = lockDescription;
    this.lockStats = new VeniceLockStats(metricsRepository, lockDescription);
    this.reportingThresholdMs = reportingThresholdMs;
  }

  public VeniceLock(Lock lock, String lockDescription, MetricsRepository metricsRepository) {
    this(lock, lockDescription, metricsRepository, DEFAULT_LOCK_OPERATION_REPORTING_THRESHOLD_MS);
  }

  /**
   * Attempt to acquire the lock. Metric is reported every time we failed to acquire the lock within the given reportingThresholdMs.
   * Note that the reportingThresholdMs is only used for reporting purpose. That is, no other actions upon timing out other than keep
   * trying to acquire the lock.
   */
  public void lock() {
    long acquireStartTime = System.currentTimeMillis();
    for (int attempt = 1; true; attempt++) {
      try {
        if (lock.tryLock(reportingThresholdMs, TimeUnit.MILLISECONDS)) {
          // lock acquired
          long acquiredTime = System.currentTimeMillis();
          lockStats.successfulLockAcquisition.record();
          lockStats.lockAcquisitionTimeMs.record(Math.max(0, acquiredTime - acquireStartTime));
          if (!lockAcquiredTimeMap.containsKey(Thread.currentThread().getId())) {
            lockAcquiredTimeMap.put(Thread.currentThread().getId(), acquiredTime);
          }
          return;
        }
        // failed to acquire the lock within the reportingThresholdMs
        lockStats.failedLockAcquisition.record();
        if (attempt == 1) {
          LOGGER.warn("Failed to acquire the lock: " + lock.getClass().getSimpleName() + " with description: "
              + lockDescription + " within the reporting threshold of " + reportingThresholdMs
              + " ms. Will keep retrying.");
        }
      } catch (InterruptedException e) {
        LOGGER.info("Interrupted while trying to acquire the lock: " + lock.getClass().getSimpleName()
            + " with description: " + lockDescription);
      }
    }
  }

  /**
   * Attempt to unlock. Might throw IllegalMonitorStateException if the current thread does not hold this lock.
   */
  public void unlock() {
    long threadId = Thread.currentThread().getId();
    // get the acquired timestamp and cleanup the map prior to releasing the lock to avoid possible race condition
    // where the same thread immediately grabs the lock again.
    Long acquiredTime = lockAcquiredTimeMap.remove(threadId);
    lock.unlock();
    if (acquiredTime != null) {
      long lockRetentionTimeMs = Math.max(0, System.currentTimeMillis() - acquiredTime);
      lockStats.lockRetentionTimeMs.record(lockRetentionTimeMs);
      if (lockRetentionTimeMs > reportingThresholdMs) {
        LOGGER.warn("Lock: " + lock.getClass().getSimpleName() + " with description: " + lockDescription
            + " held the lock for " + lockRetentionTimeMs + " ms which exceeded the reporting threshold of "
            + reportingThresholdMs);
      }
    }
  }
}
