package com.linkedin.venice.meta;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.exceptions.StoreDisabledException;
import com.linkedin.venice.exceptions.StoreVersionNotFoundException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test cases for Venice Store.
 */
public class TestZKStore {
  @Test
  public void testVersionsAreAddedInOrdered() {
    Store s = TestUtils.createTestStore("s1", "owner", System.currentTimeMillis());
    s.addVersion(new VersionImpl(s.getName(), 4));
    s.addVersion(new VersionImpl(s.getName(), 2));
    s.addVersion(new VersionImpl(s.getName(), 3));
    s.addVersion(new VersionImpl(s.getName(), 1));

    List<Version> versions = s.getVersions();
    Assert.assertEquals(versions.size(), 4, "The Store version list is expected to contain 4 items!");
    for (int i = 0; i < versions.size(); i++) {
      int expectedVersion = i + 1;
      Assert.assertEquals(
          versions.get(i).getNumber(),
          i + 1,
          "The Store version list is expected to contain version " + expectedVersion + " at index " + i);
    }
  }

  @Test
  public void testDeleteVersion() {
    Store s = TestUtils.createTestStore("s1", "owner", System.currentTimeMillis());
    s.addVersion(new VersionImpl(s.getName(), 4));
    s.addVersion(new VersionImpl(s.getName(), 2));
    s.addVersion(new VersionImpl(s.getName(), 3));
    s.addVersion(new VersionImpl(s.getName(), 1));

    List<Version> versions = s.getVersions();
    Assert.assertEquals(versions.size(), 4, "The Store version list is expected to contain 4 items!");

    s.deleteVersion(3);
    versions = s.getVersions();
    Assert.assertEquals(versions.size(), 3, "The Store version list is expected to contain 3 items!");
    for (int i: new int[] { 1, 2, 4 }) {
      boolean foundVersion = false;
      for (Version version: versions) {
        if (version.getNumber() == i) {
          foundVersion = true;
          break;
        }
      }
      Assert.assertTrue(foundVersion, "The expected store version " + i + " was not found!");
    }
  }

  @Test
  public void testCloneStore() {
    Store s = TestUtils.createTestStore("s1", "owner", System.currentTimeMillis());
    Store clonedStore = s.cloneStore();
    Assert.assertTrue(s.equals(clonedStore), "The cloned store is expected to be equal!");
    clonedStore.setCurrentVersion(100);
    Assert.assertEquals(s.getCurrentVersion(), 0, "The cloned store's version is expected to be 0!");
    Assert.assertEquals(s.peekNextVersionNumber(), 1, "clone should peek at biggest used version plus 1");

    Store s2 = TestUtils.createTestStore("s2", "owner", System.currentTimeMillis());
    s2.addVersion(new VersionImpl(s2.getName(), s2.getLargestUsedVersionNumber() + 1, "pushJobId"));
    Store s2clone = s2.cloneStore();
    Assert.assertEquals(s2, s2clone);
    s2clone.setEnableWrites(false);
    Assert.assertNotEquals(s2, s2clone);
  }

  private static void assertVersionsEquals(
      Store store,
      int versionToPreserve,
      List<Version> expectedVersions,
      String message) {
    List<Version> actualVersions = store.retrieveVersionsToDelete(versionToPreserve);
    Set<Integer> versionSet = actualVersions.stream().map(v -> v.getNumber()).collect(Collectors.toSet());
    // TestNG calls the assertEquals(Collection, Collection) though it should have called
    // assertEquals(set,set) when using the HashSet intermittently.
    // Doing manual set comparison for now

    Assert.assertEquals(actualVersions.size(), expectedVersions.size(), message + " -->size of lists does not match");
    for (Version version: expectedVersions) {
      Assert.assertTrue(
          versionSet.contains(version.getNumber()),
          message + " --> version " + version + " is missing in actual");
    }
  }

  @Test
  public void testRetrieveVersionsToDelete() {
    Store store = TestUtils.createTestStore("retrieveDeleteStore", "owner", System.currentTimeMillis());
    Assert
        .assertEquals(store.retrieveVersionsToDelete(1).size(), 0, "Store with no active version returns empty array");

    Version version1 = new VersionImpl(store.getName(), 1);
    store.addVersion(version1);
    Assert.assertEquals(store.retrieveVersionsToDelete(1).size(), 0, "only one version, it should be preserved.");

    Version version2 = new VersionImpl(store.getName(), 2);
    store.addVersion(version2);
    Assert.assertEquals(store.retrieveVersionsToDelete(1).size(), 1, "two versions, one should be deleted ");

    version1.setStatus(VersionStatus.ONLINE);
    Assert.assertEquals(
        store.retrieveVersionsToDelete(1).size(),
        0,
        "one version is active and the last version should be preserved, nothing to delete.");

    version2.setStatus(VersionStatus.ONLINE);
    store.setCurrentVersion(2);

    assertVersionsEquals(store, 1, Arrays.asList(version1), "two version active, one should be deleted");
    Assert.assertEquals(store.retrieveVersionsToDelete(2).size(), 0, "Only two active versions, nothing to delete");

    // Add one more version in error.
    Version version3 = new VersionImpl(store.getName(), 3);
    store.addVersion(version3);

    assertVersionsEquals(store, 1, Arrays.asList(version1), "two version active, one should be deleted");

    version3.setStatus(VersionStatus.ERROR);

    assertVersionsEquals(store, 1, Arrays.asList(version1, version3), "error version should be deleted");

    // current version is in the middle, delete 1 and 3
    version3.setStatus(VersionStatus.ONLINE);
    assertVersionsEquals(store, 1, Arrays.asList(version1, version3), "version 1 and 3 should be deleted");

    Version version4 = new VersionImpl(store.getName(), 4);
    store.addVersion(version4);
    version4.setStatus(VersionStatus.ERROR);
    version3.setStatus(VersionStatus.ERROR);

    assertVersionsEquals(store, 2, Arrays.asList(version3, version4), "error versions should be deleted.");
    assertVersionsEquals(
        store,
        1,
        Arrays.asList(version1, version3, version4),
        "lower active and 2 error version should be deleted");

    Version version5 = new VersionImpl(store.getName(), 5);
    store.addVersion(version5);
    store.setCurrentVersion(5);
    version5.setStatus(VersionStatus.ONLINE);

    assertVersionsEquals(store, 2, Arrays.asList(version1, version3, version4), "delete all but 2 active versions");
    assertVersionsEquals(store, 5, Arrays.asList(version3, version4), "delete all error versions");
  }

  @Test
  public void testRetrieveVersionsToDeleteWithStoreLevelConfig() {
    Store store =
        TestUtils.createTestStore("retrieveDeleteStoreWithStoreLevelConfig", "owner", System.currentTimeMillis());
    int numVersionsToPreserve = 5;
    store.setNumVersionsToPreserve(numVersionsToPreserve);
    for (int i = 0; i < numVersionsToPreserve; i++) {
      Version v = new VersionImpl(store.getName(), i + 1);
      v.setStatus(VersionStatus.ONLINE);
      store.addVersion(v);
    }

    Assert.assertEquals(
        store.retrieveVersionsToDelete(2).size(),
        0,
        "Once enable store level config, this store should preserve all current versions.");

    int newNumVersionToPreserve = 1;
    store.setNumVersionsToPreserve(1);
    List<Version> versions = store.retrieveVersionsToDelete(numVersionsToPreserve);
    Assert.assertEquals(versions.size(), numVersionsToPreserve - newNumVersionToPreserve);
    for (Version v: versions) {
      // we should keep the latest version.
      Assert.assertTrue(v.getNumber() < numVersionsToPreserve + 1);
    }
  }

  @Test
  public void testDisableStoreWrite() {
    String storeName = "testDisableStoreWrite";
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    store.setEnableWrites(false);
    // add a new version to disabled store.
    try {
      store.addVersion(new VersionImpl(storeName, 1));
      Assert.fail("Store is disabled to write, can not add new store to it.");
    } catch (StoreDisabledException e) {
    }
    // increase version number for disabled store.
    try {
      store.addVersion(new VersionImpl(store.getName(), store.getLargestUsedVersionNumber() + 1, "pushJobId"));
      Assert.fail("Store is disabled to write, can not add new store to it.");
    } catch (StoreDisabledException e) {
    }

    try {
      store.setCurrentVersion(1);
      Assert.fail("Store is disabled to write, can not set current version.");
    } catch (StoreDisabledException e) {
    }

    try {
      store.updateVersionStatus(1, VersionStatus.ONLINE);
      Assert.fail("Store is disabled to write, can not activated a new version");
    } catch (StoreDisabledException e) {
    }

    store.setEnableWrites(true);
    // After store is enabled to write, add/increase/reserve version for this store as normal
    store.addVersion(new VersionImpl(storeName, 1));
    Assert.assertEquals(store.getVersions().get(0).getNumber(), 1);
    store.addVersion(new VersionImpl(store.getName(), store.getLargestUsedVersionNumber() + 1, "pushJobId"));
    Assert.assertEquals(store.getVersions().get(1).getNumber(), 2);
    Assert.assertEquals(store.peekNextVersionNumber(), 3);
    store.setCurrentVersion(1);
    Assert.assertEquals(store.getCurrentVersion(), 1);
    store.updateVersionStatus(2, VersionStatus.ONLINE);
    Assert.assertEquals(store.getVersions().get(1).getStatus(), VersionStatus.ONLINE);
  }

  @Test
  public void canCloneDisabledStore() {
    String storeName = Utils.getUniqueString("store");
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    store.addVersion(new VersionImpl(storeName, 1));
    store.setEnableWrites(false);
    store.setEnableReads(false);
    Assert.assertFalse(store.isEnableWrites());
    Store cloned = store.cloneStore();
    Assert.assertFalse(cloned.isEnableWrites(), "clone of disabled store must be disabled");
    Assert.assertFalse(cloned.isEnableReads(), "clone of disabled store must be disabled");
  }

  @Test
  public void testEnableStoreWrite() {
    String storeName = Utils.getUniqueString("store");
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    Version pushedVersion = new VersionImpl(storeName, 1);
    // Add a pushed version
    pushedVersion.setStatus(VersionStatus.PUSHED);
    store.addVersion(new VersionImpl(storeName, 2));
    store.setEnableWrites(false);
    // Update status to PUSHED after store is disabled to write.
    store.updateVersionStatus(2, VersionStatus.PUSHED);
    // Enable store to write.
    store.setEnableWrites(true);

    for (Version version: store.getVersions()) {
      Assert.assertEquals(
          version.getStatus(),
          VersionStatus.PUSHED,
          "After enabling a store to write, all of PUSHED will stay as PUSHED. Deferred swap service will mark them as online");
    }
  }

  @Test
  public void testDisableAndEnableStoreRead() {
    String storeName = "testDisableStoreRead";
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    Version version = new VersionImpl(store.getName(), store.getLargestUsedVersionNumber() + 1, "pushJobId");
    store.addVersion(version);
    store.updateVersionStatus(version.getNumber(), VersionStatus.ONLINE);
    store.setCurrentVersion(version.getNumber());
    Assert.assertEquals(
        store.getCurrentVersion(),
        version.getNumber(),
        "Version:" + version.getNumber() + " should be ready to serve");
    // Disable store to read.
    store.setEnableReads(false);
    Assert.assertFalse(store.isEnableReads(), "Store has been disabled to read");
    // Only disable read, store could continue to increase version and update version status.
    version = new VersionImpl(store.getName(), store.getLargestUsedVersionNumber() + 1, "pushJobId2");
    store.addVersion(version);
    store.updateVersionStatus(version.getNumber(), VersionStatus.ONLINE);
    store.setCurrentVersion(version.getNumber());
    store.setEnableReads(true);
    Assert.assertEquals(
        store.getCurrentVersion(),
        version.getNumber(),
        "After enabling store to read, a current version should be ready to serve.");
  }

  @Test
  public void testUseTheDeletedVersionNumber() {
    String storeName = Utils.getUniqueString("store");
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    store.addVersion(new VersionImpl(store.getName(), store.getLargestUsedVersionNumber() + 1, "pushJobId"));
    store.addVersion(new VersionImpl(store.getName(), store.getLargestUsedVersionNumber() + 1, "pushJobId2"));
    // largest version number is 2
    store.deleteVersion(2);
    Version version = new VersionImpl(store.getName(), store.getLargestUsedVersionNumber() + 1, "pushJobId3");
    store.addVersion(version);
    Assert.assertEquals(version.getNumber(), 3);
    Assert.assertEquals(store.peekNextVersionNumber(), 4);
  }

  @Test
  public void testAddVersion() {
    String storeName = Utils.getUniqueString("store");
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    assertMissingVersion(store, 2, 5, 6);
    assertVersionCount(store, 0);

    store.addVersion(new VersionImpl(storeName, 5));
    assertMissingVersion(store, 2, 6);
    assertPresentVersion(store, 5);
    assertVersionCount(store, 1);
    // largest used version is 5
    assertEquals(store.peekNextVersionNumber(), 6);

    store.addVersion(new VersionImpl(storeName, 2));
    assertMissingVersion(store, 6);
    assertPresentVersion(store, 2, 5);
    assertVersionCount(store, 2);
    // largest used version is still 5
    Assert.assertEquals(store.peekNextVersionNumber(), 6);

    Version version = new VersionImpl(store.getName(), store.getLargestUsedVersionNumber() + 1, "pushJobId");
    Assert.assertEquals(version.getNumber(), 6);
    store.addVersion(version);
    Assert.assertEquals(store.peekNextVersionNumber(), 7);
    assertPresentVersion(store, 2, 5, 6);
    assertVersionCount(store, 3);
  }

  private void assertMissingVersion(Store store, int... versionNumbers) {
    for (int versionNumber: versionNumbers) {
      assertNull(store.getVersion(versionNumber));
      assertThrows(StoreVersionNotFoundException.class, () -> store.getVersionOrThrow(versionNumber));
      assertFalse(store.getVersionNumbers().contains(versionNumber));
    }
  }

  private void assertPresentVersion(Store store, int... versionNumbers) {
    for (int versionNumber: versionNumbers) {
      assertNotNull(store.getVersion(versionNumber));
      assertEquals(store.getVersionOrThrow(versionNumber).getNumber(), versionNumber);
      assertTrue(store.getVersionNumbers().contains(versionNumber));
    }
  }

  private void assertVersionCount(Store store, int versionCount) {
    assertEquals(store.getVersions().size(), versionCount);
    assertEquals(store.getVersionNumbers().size(), versionCount);
  }

  @Test
  public void testNativeReplicationConfigOnVersionPush() {
    // Test that it's set to false if the cluster is set to false
    String storeName = Utils.getUniqueString("store");
    String pushJobID = Utils.getUniqueString("FOO-ID");
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    store.setNativeReplicationEnabled(false);
    store.addVersion(new VersionImpl(storeName, 0, pushJobID));
    Assert.assertFalse(store.getVersion(0).isNativeReplicationEnabled());

    // Test that tne next new version is true if the cluster is later set to true
    store.setNativeReplicationEnabled(true);
    String anotherPushJonID = Utils.getUniqueString("FOO-ID-AGAIN");
    store.addVersion(new VersionImpl(storeName, 1, anotherPushJonID));
    Assert.assertTrue(store.getVersion(1).isNativeReplicationEnabled());
  }

  @Test
  public void testValidStoreNames() {
    List<String> valid = Arrays.asList("foo", "Bar", "foo_bar", "foo-bar", "f00Bar");
    List<String> invalid = Arrays.asList("foo bar", "foo.bar", " foo", ".bar", "!", "@", "#", "$", "%");
    for (String name: valid) {
      Assert.assertTrue(StoreName.isValidStoreName(name));
    }
    for (String name: invalid) {
      Assert.assertFalse(StoreName.isValidStoreName(name));
    }
  }

  @Test(expectedExceptions = VeniceException.class)
  public void invalidStoreNameThrows() {
    new ZKStore(
        "My Store Name",
        "owner",
        System.currentTimeMillis(),
        PersistenceType.IN_MEMORY,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1);
  }

  @Test
  public void testStoreLevelAcl() {
    Store store = new ZKStore(
        "storeName",
        "owner",
        System.currentTimeMillis(),
        PersistenceType.IN_MEMORY,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1);
    Assert.assertTrue(store.isAccessControlled());
  }

  @Test
  public void testUpdateVersionForDaVinciHeartbeat() {
    String storeName = "testUpdateVersionForDaVinciHeartbeat";
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    store.addVersion(new VersionImpl(storeName, 1, "test"));
    store.updateVersionForDaVinciHeartbeat(1, true);
    Assert.assertTrue(store.getVersion(1).getIsDavinciHeartbeatReported());
  }
}
