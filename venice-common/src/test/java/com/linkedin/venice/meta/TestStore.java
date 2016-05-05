package com.linkedin.venice.meta;

import com.linkedin.venice.utils.TestUtils;
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
    Assert.assertTrue(store.reserveVersionNumber(2), "Store must be able to reserve version 2");
    Assert.assertFalse(store.reserveVersionNumber(2), "Store shouldn't reserve an already reserved version");
    Assert.assertEquals(store.peekNextVersion().getNumber(), 3, "next version should respect reservation");
    Version increasedV = store.increaseVersion();
    Assert.assertEquals(increasedV.getNumber(), 3, "increased version must be greater than reserved versions");
    Assert.assertEquals(store.peekNextVersion().getNumber(), 4, "peek must be greater than existing versions");
    Assert.assertTrue(store.reserveVersionNumber(8), "can reserve non-sequential version");
    Assert.assertFalse(store.reserveVersionNumber(7), "cant reserve smaller than currently reserved");
    Assert.assertFalse(store.reserveVersionNumber(8), "cant reserve already reserved");
    Assert.assertEquals(store.peekNextVersion().getNumber(), 9, "peek is bigger than reserved");
  }
}
