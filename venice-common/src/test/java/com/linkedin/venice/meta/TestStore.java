package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.StorePausedException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.TestUtils;

import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test cases for Venice Store.
 */
public class TestStore {
  @Test
  public void testVersionsAreAddedInOrdered(){
    Store s = TestUtils.createTestStore("s1", "owner", System.currentTimeMillis());
    s.addVersion(new Version(s.getName(), 4, System.currentTimeMillis()));
    s.addVersion(new Version(s.getName(), 2, System.currentTimeMillis()));
    s.addVersion(new Version(s.getName(), 3, System.currentTimeMillis()));
    s.addVersion(new Version(s.getName(), 1, System.currentTimeMillis()));

    List<Version> versions = s.getVersions();
    Assert.assertEquals(versions.size(), 4, "The Store version list is expected to contain 4 items!");
    for(int i=0;i<versions.size();i++){
      int expectedVersion = i + 1;
      Assert.assertEquals(versions.get(i).getNumber(), i + 1,
          "The Store version list is expected to contain version " + expectedVersion + " at index " + i);
    }
  }

  @Test
  public void testDeleteVersion(){
    Store s = TestUtils.createTestStore("s1", "owner", System.currentTimeMillis());
    s.addVersion(new Version(s.getName(), 4));
    s.addVersion(new Version(s.getName(), 2));
    s.addVersion(new Version(s.getName(), 3));
    s.addVersion(new Version(s.getName(), 1));

    List<Version> versions = s.getVersions();
    Assert.assertEquals(versions.size(), 4, "The Store version list is expected to contain 4 items!");

    s.deleteVersion(3);
    versions = s.getVersions();
    Assert.assertEquals(versions.size(), 3, "The Store version list is expected to contain 3 items!");
    for (int i: new int[] {1,2,4}) {
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
  public void testCloneStore(){
    Store s = TestUtils.createTestStore("s1", "owner", System.currentTimeMillis());
    Store clonedStore = s.cloneStore();
    Assert.assertTrue(s.equals(clonedStore), "The cloned store is expected to be equal!");
    clonedStore.setCurrentVersion(100);
    Assert.assertEquals(s.getCurrentVersion(), 0, "The cloned store's version is expected to be 0!");
    Assert.assertEquals(s.peekNextVersion().getNumber(), 1, "clone should peek at biggest used version plus 1");

    Store s2 = TestUtils.createTestStore("s2", "owner", System.currentTimeMillis());
    s2.increaseVersion();
    Store s2clone = s2.cloneStore();
    Assert.assertEquals(s2, s2clone);
    s2clone.setPaused(true);
    Assert.assertNotEquals(s2, s2clone);
  }

  private static void assertVersionsEquals(Store store, int versionToPreserve, List<Version> expectedVersions, String message) {
    List<Version> actualVersions = store.retrieveVersionsToDelete(versionToPreserve);
    // TestNG calls the assertEquals(Collection, Collection) though it should have called
    // assertEquals(set,set) when using the HashSet intermittently.
    // Doing manual set comparison for now

    Assert.assertEquals(actualVersions.size(), expectedVersions.size(),  message + " -->size of lists does not match");
    for(Version version: expectedVersions) {
      Assert.assertTrue( actualVersions.contains(version) , message + " --> version " + version + " is missing in actual");
    }
  }


  @Test
  public void testRetrieveVersionsToDelete() {
    Store store = TestUtils.createTestStore("retrieveDeleteStore", "owner", System.currentTimeMillis());
    Assert.assertEquals(store.retrieveVersionsToDelete(1).size(), 0, "Store with no active version returns empty array");

    Version version1 = new Version(store.getName(), 1);
    store.addVersion(version1);
    Assert.assertEquals(store.retrieveVersionsToDelete(1).size(), 0, "all unfinished version returns empty array ");

    Version version2 = new Version(store.getName(), 2);
    store.addVersion(version2);
    Assert.assertEquals(store.retrieveVersionsToDelete(1).size(), 0, "all unfinished versions returns empty array ");

    version1.setStatus(VersionStatus.ONLINE);
    Assert.assertEquals(store.retrieveVersionsToDelete(1).size(), 0, "Only one active version there, nothing to delete");

    version2.setStatus(VersionStatus.ONLINE);
    assertVersionsEquals(store, 1, Arrays.asList(version1), "two version active, one should be deleted");
    Assert.assertEquals(store.retrieveVersionsToDelete(2).size(), 0, "Only two active versions, nothing to delete");

    // Add one more version in error.
    Version version3 = new Version(store.getName(), 3);
    store.addVersion(version3);
    assertVersionsEquals(store, 1, Arrays.asList(version1), "two version active, one should be deleted");

    version3.setStatus(VersionStatus.ERROR);
    assertVersionsEquals( store, 1, Arrays.asList(version1), "highest version is never deleted");

    Version version4 = new Version(store.getName(), 4);
    store.addVersion(version4);

    version4.setStatus(VersionStatus.ERROR);
    assertVersionsEquals(store, 2, Arrays.asList(version3), "lower error versions should be deleted.");

    assertVersionsEquals( store, 1, Arrays.asList(version1, version3), "lower active and error version should be deleted");

    Version version5 = new Version(store.getName(), 5);
    store.addVersion(version5);
    version5.setStatus(VersionStatus.ONLINE);

    assertVersionsEquals(store, 2, Arrays.asList(version1, version3, version4), "delete all but 2 active versions");

    assertVersionsEquals( store, 5, Arrays.asList(version3 , version4),"delete all error versions");
  }


  @Test
  public void testOperatingPausedStore(){
    String storeName = "testPausedStore";
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    store.setPaused(true);
    // add a new version to paused store.
    try{
      store.addVersion(new Version(storeName, 1));
      Assert.fail("Store is paused, can not add new store to it.");
    }catch (StorePausedException e){
    }
    // increase version number for paused store.
    try{
      store.increaseVersion();
      Assert.fail("Store is paused, can not add new store to it.");
    }catch (StorePausedException e){
    }

    try{
      store.setCurrentVersion(1);
      Assert.fail("Store is paused, can not set current version.");
    }catch (StorePausedException e){
    }

    try{
      store.updateVersionStatus(1, VersionStatus.ONLINE);
      Assert.fail("Store is paused, can not activated a new version");
    }catch (StorePausedException e){
    }

    store.setPaused(false);
    // After store is resume, add/increase/reserve version for this store as normal
    store.addVersion(new Version(storeName, 1));
    Assert.assertEquals(store.getVersions().get(0).getNumber(), 1);
    store.increaseVersion();
    Assert.assertEquals(store.getVersions().get(1).getNumber(), 2);
    Assert.assertEquals(store.peekNextVersion().getNumber(), 3);
    store.setCurrentVersion(1);
    Assert.assertEquals(store.getCurrentVersion(), 1);
    store.updateVersionStatus(2, VersionStatus.ONLINE);
    Assert.assertEquals(store.getVersions().get(1).getStatus(), VersionStatus.ONLINE);
  }

  @Test
  public void canClonePausedStore(){
    String storeName = TestUtils.getUniqueString("store");
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    store.addVersion(new Version(storeName, 1));
    store.setPaused(true);
    Assert.assertTrue(store.isPaused());
    Store cloned = store.cloneStore();
    Assert.assertTrue(cloned.isPaused(), "clone of paused store must be paused");
  }

  @Test
  public void testResumeStore() {
    String storeName = TestUtils.getUniqueString("store");
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    Version pushedVersion = new Version(storeName, 1);
    //Add a pushed version
    pushedVersion.setStatus(VersionStatus.PUSHED);
    store.addVersion(new Version(storeName, 2));
    store.setPaused(true);
    // Update status to PUSHED after store is paused.
    store.updateVersionStatus(2, VersionStatus.PUSHED);
    //resume store.
    store.setPaused(false);

    for (Version version : store.getVersions()) {
      Assert.assertEquals(version.getStatus(), VersionStatus.ONLINE,
          "After resuming a store, all of PUSHED version should be activated.");
    }
  }

  @Test
  public void testUseTheDeletedVersionNumber() {
    String storeName = TestUtils.getUniqueString("store");
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    store.increaseVersion();
    store.increaseVersion();
    //largest version number is 2
    store.deleteVersion(2);
    Version version = store.increaseVersion();
    Assert.assertEquals(version.getNumber(), 3);
    Assert.assertEquals(store.peekNextVersion().getNumber(), 4);
  }

  @Test
  public void testAddVersion() {
    String storeName = TestUtils.getUniqueString("store");
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    store.addVersion(new Version(storeName, 5));
    Assert.assertEquals(store.getVersions().size(), 1);
    //largest used version is 5
    Assert.assertEquals(store.peekNextVersion().getNumber(), 6);
    store.addVersion(new Version(storeName, 2));
    Assert.assertEquals(store.getVersions().size(), 2);
    //largest used version is still 5
    Assert.assertEquals(store.peekNextVersion().getNumber(), 6);
    Version version = store.increaseVersion();
    Assert.assertEquals(version.getNumber(), 6);
  }

  @Test
  public void testValidStoreNames(){
    List<String> valid = Arrays.asList("foo", "Bar", "foo_bar", "foo-bar", "f00Bar");
    List<String> invalid = Arrays.asList("foo bar", "foo.bar", " foo", ".bar", "!", "@", "#", "$", "%");
    for (String name : valid){
      Assert.assertTrue(Store.isValidStoreName(name));
    }
    for (String name : invalid){
      Assert.assertFalse(Store.isValidStoreName(name));
    }
  }

  @Test (expectedExceptions = VeniceException.class)
  public void invalidStoreNameThrows(){
    Store store = new Store("My Store Name", "owner", System.currentTimeMillis(), PersistenceType.IN_MEMORY, RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
  }
}
