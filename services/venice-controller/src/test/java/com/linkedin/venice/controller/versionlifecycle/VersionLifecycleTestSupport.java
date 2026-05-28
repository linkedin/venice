package com.linkedin.venice.controller.versionlifecycle;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;


/**
 * Shared constants and mock factories for the {@link VersionLifecyclePolicy} test classes
 * ({@link NewPushCapacityGuardsTest}, {@link VersionSelectionTest}, {@link PushStatusAggregationTest},
 * {@link LifecyclePredicatesTest}). Package-private — extend cautiously, anything added here
 * silently becomes a contract every test file may depend on.
 */
final class VersionLifecycleTestSupport {
  static final String CLUSTER_NAME = "test-cluster";
  static final String STORE_NAME = "test-store";
  static final int MIN_VERSIONS_TO_PRESERVE = 2;
  static final long MIN_CLEANUP_DELAY_MS = TimeUnit.HOURS.toMillis(1);
  static final long ROLLED_BACK_RETENTION_MS = TimeUnit.HOURS.toMillis(24);

  private VersionLifecycleTestSupport() {
  }

  static Version mockVersion(int number, VersionStatus status) {
    Version v = mock(Version.class);
    doReturn(number).when(v).getNumber();
    doReturn(status).when(v).getStatus();
    return v;
  }

  static Store mockStoreWithVersions(int currentVersion, long promotedMsAgo, Version... versions) {
    Store store = mock(Store.class);
    doReturn(currentVersion).when(store).getCurrentVersion();
    doReturn(System.currentTimeMillis() - promotedMsAgo).when(store).getLatestVersionPromoteToCurrentTimestamp();
    doReturn(versions.length == 0 ? Collections.emptyList() : Arrays.asList(versions)).when(store).getVersions();
    return store;
  }

  static Store mockStoreWithPromoteTimestamp(int currentVersion, long promoteTimestampMs, Version... versions) {
    Store store = mock(Store.class);
    doReturn(currentVersion).when(store).getCurrentVersion();
    doReturn(promoteTimestampMs).when(store).getLatestVersionPromoteToCurrentTimestamp();
    doReturn(versions.length == 0 ? Collections.emptyList() : Arrays.asList(versions)).when(store).getVersions();
    return store;
  }

  static Set<String> setOf(String... values) {
    Set<String> out = new HashSet<>();
    Collections.addAll(out, values);
    return out;
  }
}
