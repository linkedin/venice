package com.linkedin.venice.meta;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;

import org.testng.annotations.Test;


public class NameRepositoryTest {
  @Test
  public void test() {
    NameRepository nameRepository = new NameRepository();

    // Only valid stores and store-versions should be possible
    assertThrows(IllegalArgumentException.class, () -> nameRepository.getStoreName("invalid--store--name"));
    assertThrows(IllegalArgumentException.class, () -> nameRepository.getStoreVersionName("missingTheUnderscoreV23"));

    assertThrows(IllegalArgumentException.class, () -> new StoreName("invalid--store--name"));
    assertThrows(
        IllegalArgumentException.class,
        () -> new StoreVersionName("missingTheUnderscoreV23", new StoreName("store")));

    // Basic "happy path" stuff
    String storeNameString = "store";
    StoreName storeName = nameRepository.getStoreName(storeNameString);
    assertNotNull(storeName);
    assertEquals(storeName.getName(), storeNameString);

    int versionNumber = 1;
    StoreVersionName storeVersionName = nameRepository.getStoreVersionName(storeNameString, versionNumber);
    assertNotNull(storeVersionName);
    String storeVersionNameString = storeNameString + "_v" + versionNumber;
    assertEquals(storeVersionName.getName(), storeVersionNameString);
    assertEquals(storeVersionName.getStore(), storeName);
    assertSame(storeVersionName.getStore(), storeName, "These should not only be equal, but also the same ref!");
    assertEquals(storeVersionName.getStoreName(), storeName.getName());
    assertEquals(storeVersionName.getVersionNumber(), versionNumber);

    StoreVersionName storeVersionName2 = nameRepository.getStoreVersionName(storeVersionNameString);
    assertEquals(storeVersionName, storeVersionName2);
    assertSame(storeVersionName, storeVersionName2, "These should not only be equal, but also the same ref!");

    // Ensure that equals and hashCode works correctly,
    // even if different refs are compared (which could happen if the cache grows too big)
    StoreName storeNameOtherRef = new StoreName(storeNameString);
    assertEquals(storeName, storeNameOtherRef);
    assertEquals(storeName.toString(), storeNameOtherRef.toString());
    assertNotSame(storeName, storeNameOtherRef, "These should not be the same ref!");
    assertEquals(storeName.hashCode(), storeNameOtherRef.hashCode());

    StoreVersionName storeVersionNameOtherRef = new StoreVersionName(storeVersionNameString, storeName);
    assertEquals(storeVersionName, storeVersionNameOtherRef);
    assertEquals(storeVersionName.toString(), storeVersionNameOtherRef.toString());
    assertNotSame(storeVersionName, storeVersionNameOtherRef, "These should not be the same ref!");
    assertEquals(storeVersionName.hashCode(), storeVersionNameOtherRef.hashCode());

    // Not equals
    StoreName differentStoreName = nameRepository.getStoreName("differentStore");
    assertNotNull(differentStoreName);
    assertNotEquals(storeName, differentStoreName);

    StoreVersionName differentStoreVersionName = nameRepository.getStoreVersionName("differentStore", 1);
    assertNotNull(differentStoreVersionName);
    assertNotEquals(storeVersionName, differentStoreVersionName);

    // Nonsensical equals
    assertNotEquals(storeName, new Object());
    assertNotEquals(new Object(), storeName);
    assertNotEquals(storeVersionName, new Object());
    assertNotEquals(new Object(), storeVersionName);
  }
}
