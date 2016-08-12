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
    s.reserveVersionNumber(5);
    Store clonedStore = s.cloneStore();
    Assert.assertTrue(s.equals(clonedStore), "The cloned store is expected to be equal!");
    clonedStore.setCurrentVersion(100);
    Assert.assertEquals(s.getCurrentVersion(), 0, "The cloned store's version is expected to be 0!");
    Assert.assertEquals(s.peekNextVersion().getNumber(), 6, "clone should peek at reserved version plus 1");

    Store s2 = TestUtils.createTestStore("s2", "owner", System.currentTimeMillis());
    s2.increaseVersion();
    Store s2clone = s2.cloneStore();
    Assert.assertEquals(s2, s2clone);
    s2clone.reserveVersionNumber(5);
    Assert.assertNotEquals(s2, s2clone);
  }

  @Test
  public void testVersionReservation(){
    Store store = TestUtils.createTestStore("myStore", "owner", System.currentTimeMillis());
    Assert.assertEquals(store.peekNextVersion().getNumber(), 1);
    store.reserveVersionNumber(2);
    reserveVersionFails(store, 2);
    Assert.assertEquals(store.peekNextVersion().getNumber(), 3, "next version should respect reservation");
    Version increasedV = store.increaseVersion();
    Assert.assertEquals(increasedV.getNumber(), 3, "increased version must be greater than reserved versions");
    Assert.assertEquals(store.peekNextVersion().getNumber(), 4, "peek must be greater than existing versions");
    store.reserveVersionNumber(8);
    reserveVersionFails(store, 7);
    reserveVersionFails(store, 8);
    Assert.assertEquals(store.peekNextVersion().getNumber(), 9, "peek is bigger than reserved");
  }

  private static void reserveVersionFails(Store store, int version) {
    try {
      store.reserveVersionNumber(version);
      Assert.fail("Shouldn't be able to reserve version " + version);
    } catch (VeniceException e) {
    }
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

    version1.setStatus(VersionStatus.ACTIVE);
    Assert.assertEquals(store.retrieveVersionsToDelete(1).size(), 0, "Only one active version there, nothing to delete");

    version2.setStatus(VersionStatus.ACTIVE);
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
    version5.setStatus(VersionStatus.ACTIVE);

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
    // reserve version for paused store.
    try{
      store.reserveVersionNumber(2);
      Assert.fail("Store is paused, can not reserve a new version.");
    }catch (StorePausedException e){
    }

    try{
      store.setCurrentVersion(1);
      Assert.fail("Store is paused, can not set current version.");
    }catch (StorePausedException e){
    }

    try{
      store.updateVersionStatus(1, VersionStatus.ACTIVE);
      Assert.fail("Store is paused, can not activated a new version");
    }catch (StorePausedException e){
    }

    store.setPaused(false);
    // After store is resume, add/increase/reserve version for this store as normal
    store.addVersion(new Version(storeName, 1));
    Assert.assertEquals(store.getVersions().get(0).getNumber(), 1);
    store.increaseVersion();
    Assert.assertEquals(store.getVersions().get(1).getNumber(), 2);
    store.reserveVersionNumber(3);
    Assert.assertEquals(store.peekNextVersion().getNumber(), 4);
    store.setCurrentVersion(1);
    Assert.assertEquals(store.getCurrentVersion(), 1);
    store.updateVersionStatus(2, VersionStatus.ACTIVE);
    Assert.assertEquals(store.getVersions().get(1).getStatus(), VersionStatus.ACTIVE);
  }
}
