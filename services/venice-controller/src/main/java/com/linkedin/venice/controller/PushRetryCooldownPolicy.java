package com.linkedin.venice.controller;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.stats.VeniceAdminStats;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.Version.PushType;
import java.util.Comparator;
import java.util.Optional;
import org.apache.http.HttpStatus;


final class PushRetryCooldownPolicy {
  private static final StackTraceElement[] EMPTY_STACK_TRACE = new StackTraceElement[0];

  private PushRetryCooldownPolicy() {
  }

  static void enforce(
      Store store,
      PushType pushType,
      String pushJobId,
      long cooldownMs,
      long currentTimeMs,
      VeniceAdminStats stats) {
    if (cooldownMs <= 0 || !pushType.isBatchOrStreamReprocessing()
        || VeniceSystemStoreUtils.isSystemStore(store.getName())) {
      return;
    }

    Optional<Version> mostRecentVersion = store.getVersions()
        .stream()
        .max(Comparator.comparingLong(Version::getCreatedTime).thenComparingInt(Version::getNumber));
    if (!mostRecentVersion.isPresent()) {
      return;
    }

    Version version = mostRecentVersion.get();
    long elapsedMs = Math.max(0, currentTimeMs - version.getCreatedTime());
    if (elapsedMs >= cooldownMs) {
      return;
    }

    long remainingCooldownMs = cooldownMs - elapsedMs;
    if (stats != null) {
      stats.recordPushRetryCooldownRejection(pushType);
    }
    VeniceHttpException exception = new VeniceHttpException(
        HttpStatus.SC_TOO_MANY_REQUESTS,
        "Cannot start " + pushType + " push with pushJobId " + pushJobId + " for store " + store.getName()
            + ": version-creating pushes must be spaced at least " + cooldownMs
            + " ms apart. The most recent persisted version (version " + version.getNumber() + ") was created "
            + elapsedMs + " ms ago. Retry in " + remainingCooldownMs + " ms.",
        ErrorType.BAD_REQUEST);
    exception.setStackTrace(EMPTY_STACK_TRACE);
    throw exception;
  }
}
