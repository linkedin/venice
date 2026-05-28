package com.linkedin.venice.controller.versionlifecycle;

import static com.linkedin.venice.controller.versionlifecycle.VersionLifecycleTestSupport.mockVersion;
import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;


/**
 * Tests for the version-selection helper on {@link VersionLifecyclePolicy}:
 * {@code getBackupVersionNumber}.
 */
public class VersionSelectionTest {
  @Test
  public void getBackupVersionNumberReturnsLargestOnlineBelowCurrent() {
    List<Version> versions = Arrays.asList(
        mockVersion(1, VersionStatus.ONLINE),
        mockVersion(2, VersionStatus.ONLINE),
        mockVersion(3, VersionStatus.ONLINE)); // current

    assertEquals(VersionLifecyclePolicy.getBackupVersionNumber(versions, 3), 2);
  }

  @Test
  public void getBackupVersionNumberSkipsNonOnlineVersions() {
    // v3 is current; v2 is ERROR — should be skipped — v1 ONLINE wins
    List<Version> versions = Arrays.asList(
        mockVersion(1, VersionStatus.ONLINE),
        mockVersion(2, VersionStatus.ERROR),
        mockVersion(3, VersionStatus.ONLINE));

    assertEquals(VersionLifecyclePolicy.getBackupVersionNumber(versions, 3), 1);
  }

  @Test
  public void getBackupVersionNumberReturnsNonExistingWhenNoOnlineBelowCurrent() {
    List<Version> versions = Arrays.asList(mockVersion(1, VersionStatus.ERROR), mockVersion(2, VersionStatus.ONLINE));

    assertEquals(VersionLifecyclePolicy.getBackupVersionNumber(versions, 2), NON_EXISTING_VERSION);
  }

  @Test
  public void getBackupVersionNumberWalksFromHighestToLowest() {
    // Sort happens in-place desc; the first ONLINE below current wins.
    List<Version> versions = Arrays.asList(
        mockVersion(5, VersionStatus.ONLINE), // current
        mockVersion(4, VersionStatus.ONLINE), // should win
        mockVersion(3, VersionStatus.ONLINE));

    assertEquals(VersionLifecyclePolicy.getBackupVersionNumber(versions, 5), 4);
  }
}
