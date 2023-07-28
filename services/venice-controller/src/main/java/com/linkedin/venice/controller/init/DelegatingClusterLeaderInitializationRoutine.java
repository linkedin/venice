package com.linkedin.venice.controller.init;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.concurrent.ConcurrencyUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class DelegatingClusterLeaderInitializationRoutine implements ClusterLeaderInitializationRoutine {
  private static final Logger LOGGER = LogManager.getLogger(DelegatingClusterLeaderInitializationRoutine.class);
  private ClusterLeaderInitializationRoutine delegate = null;
  private boolean allowEmptyDelegateInitializationToSucceed = false;

  @Override
  public void execute(String clusterToInit) {
    ConcurrencyUtils.executeUnderLock(() -> delegate.execute(clusterToInit), () -> {
      if (allowEmptyDelegateInitializationToSucceed) {
        LOGGER.info("Allowing initialization even though delegate is not set");
      } else {
        throw new VeniceException("Skipping initialization since delegate is not yet set");
      }
    }, () -> this.delegate != null, this);
  }

  public void setDelegate(ClusterLeaderInitializationRoutine delegate) {
    ConcurrencyUtils.executeUnderConditionalLock(() -> this.delegate = delegate, () -> this.delegate == null, this);
  }

  public void setAllowEmptyDelegateInitializationToSucceed() {
    // Ideally, we'd have guarded this under a synchronized lock directly, but Spotbugs isn't happy with it
    ConcurrencyUtils
        .executeUnderConditionalLock(() -> allowEmptyDelegateInitializationToSucceed = true, () -> true, this);
  }
}
