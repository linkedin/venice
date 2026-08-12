package com.linkedin.venice.controller;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.stats.VeniceAdminStats;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.Version.PushType;
import java.util.Objects;
import java.util.OptionalLong;
import org.apache.http.HttpStatus;


final class VersionCreationAttemptCooldownPolicy {
  private static final StackTraceElement[] EMPTY_STACK_TRACE = new StackTraceElement[0];

  private VersionCreationAttemptCooldownPolicy() {
  }

  static boolean checkAndReserve(
      Store store,
      PushType pushType,
      String pushJobId,
      long cooldownMs,
      long currentTimeMs,
      VeniceAdminStats stats) {
    if (cooldownMs <= 0 || !pushType.isBatchOrStreamReprocessing()
        || VeniceSystemStoreUtils.isSystemStore(store.getName())) {
      return false;
    }

    if (Objects.equals(store.getLastVersionCreationAttemptPushJobId(), pushJobId)) {
      return false;
    }

    OptionalLong latestVersionCreationTimeMs = store.getVersions().stream().mapToLong(Version::getCreatedTime).max();
    long effectivePriorTimeMs =
        Math.max(store.getLastVersionCreationAttemptTimestampMs(), latestVersionCreationTimeMs.orElse(0));
    if (effectivePriorTimeMs > 0) {
      long elapsedMs = Math.max(0, currentTimeMs - effectivePriorTimeMs);
      if (elapsedMs < cooldownMs) {
        long remainingCooldownMs = cooldownMs - elapsedMs;
        if (stats != null) {
          stats.recordVersionCreationAttemptCooldownRejection(pushType);
        }
        VeniceHttpException exception = new VeniceHttpException(
            HttpStatus.SC_TOO_MANY_REQUESTS,
            "Cannot admit " + pushType + " version-creation attempt with pushJobId " + pushJobId + " for store "
                + store.getName() + ": version-creation attempts must be spaced at least " + cooldownMs
                + " ms apart. The effective prior version-creation time was " + elapsedMs + " ms ago. Retry in "
                + remainingCooldownMs + " ms.",
            ErrorType.BAD_REQUEST);
        exception.setStackTrace(EMPTY_STACK_TRACE);
        throw exception;
      }
    }

    store.setLastVersionCreationAttemptTimestampMs(currentTimeMs);
    store.setLastVersionCreationAttemptPushJobId(pushJobId);
    return true;
  }
}
