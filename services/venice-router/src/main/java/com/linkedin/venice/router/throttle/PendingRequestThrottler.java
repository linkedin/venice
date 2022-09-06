package com.linkedin.venice.router.throttle;

import java.util.concurrent.atomic.AtomicLong;


/**
 * This class maintains a simple counter for capacity management.
 */
public class PendingRequestThrottler {
  private final long maxPendingRequest;

  private final AtomicLong currentPendingRequest = new AtomicLong(0);

  public PendingRequestThrottler(long maxPendingRequest) {
    this.maxPendingRequest = maxPendingRequest;
  }

  /**
   * When the incoming {@link #put()} exceeds the pre-defined counter,
   * 'false' will be returned.
   * @return
   */
  public boolean put() {
    if (currentPendingRequest.incrementAndGet() > maxPendingRequest) {
      currentPendingRequest.decrementAndGet();
      return false;
    }

    return true;
  }

  /**
   * The following function will decrease the counter by 1 to recover the capacity.
   */
  public void take() {
    currentPendingRequest.decrementAndGet();
  }

  public long getCurrentPendingRequestCount() {
    return currentPendingRequest.get();
  }

}
