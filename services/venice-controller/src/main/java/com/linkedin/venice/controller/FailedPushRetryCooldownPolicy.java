package com.linkedin.venice.controller;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.stats.VeniceAdminStats;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.Version.PushType;
import com.linkedin.venice.meta.VersionStatus;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import org.apache.http.HttpStatus;


final class FailedPushRetryCooldownPolicy {
  private static final StackTraceElement[] EMPTY_STACK_TRACE = new StackTraceElement[0];
  private static final Set<VersionStatus> TERMINAL_VERSION_STATUSES = EnumSet.of(
      VersionStatus.PUSHED,
      VersionStatus.ONLINE,
      VersionStatus.ERROR,
      VersionStatus.PARTIALLY_ONLINE,
      VersionStatus.KILLED,
      VersionStatus.ROLLED_BACK);

  private FailedPushRetryCooldownPolicy() {
  }

  static void enforce(
      Store store,
      PushType pushType,
      String pushJobId,
      long cooldownMs,
      long currentTimeMs,
      VeniceAdminStats stats) {
    if (cooldownMs <= 0 || !pushType.isBatchOrStreamReprocessing()
        || VeniceSystemStoreType.getSystemStoreType(store.getName()) != null) {
      return;
    }

    Optional<Version> mostRecentTerminalVersion = store.getVersions()
        .stream()
        .filter(version -> TERMINAL_VERSION_STATUSES.contains(version.getStatus()))
        .max(Comparator.comparingLong(Version::getCreatedTime).thenComparingInt(Version::getNumber));
    if (!mostRecentTerminalVersion.isPresent()) {
      return;
    }

    Version version = mostRecentTerminalVersion.get();
    if (version.getStatus() != VersionStatus.ERROR && version.getStatus() != VersionStatus.KILLED) {
      return;
    }

    long elapsedMs = Math.max(0, currentTimeMs - version.getCreatedTime());
    if (elapsedMs >= cooldownMs) {
      return;
    }

    long remainingCooldownMs = cooldownMs - elapsedMs;
    if (stats != null) {
      stats.recordFailedPushRetryCooldownRejection(pushType);
    }
    VeniceHttpException exception = new VeniceHttpException(
        HttpStatus.SC_TOO_MANY_REQUESTS,
        "Cannot start " + pushType + " push with pushJobId " + pushJobId + " for store " + store.getName()
            + ": the most recent terminal version-creating push (version " + version.getNumber() + ", status "
            + version.getStatus() + ") started " + elapsedMs + " ms ago and is subject to a " + cooldownMs
            + " ms failed-push retry cooldown. Retry in " + remainingCooldownMs + " ms.",
        ErrorType.BAD_REQUEST);
    exception.setStackTrace(EMPTY_STACK_TRACE);
    throw exception;
  }
}
