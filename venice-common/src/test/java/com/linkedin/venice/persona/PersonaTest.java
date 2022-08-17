package com.linkedin.venice.persona;

import java.util.HashSet;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class PersonaTest {
  String name = "testUser";
  long quotaNumber = 23;
  Set<String> storesToEnforce;
  Set<String> owners;

  @BeforeMethod
  public void setUp() {
    int numStores = 10;
    int numOwners = 10;

    storesToEnforce = new HashSet<>();
    for (int i = 0; i < numStores; i++) {
      storesToEnforce.add("testStore" + i);
    }

    owners = new HashSet<>();
    for (int i = 0; i < numOwners; i++) {
      owners.add("testOwner" + i);
    }

  }

  @Test
  public void testCreateReadPersona() {
    ReadPersona readPersona = new ReadPersona(name, quotaNumber, storesToEnforce, owners);
    Assert.assertEquals(readPersona.getName(), name);
    Assert.assertEquals(readPersona.getQuotaNumber(), quotaNumber);
    Assert.assertEquals(readPersona.getStoresToEnforce(), storesToEnforce);
    Assert.assertEquals(readPersona.getOwners(), owners);
  }

  @Test
  public void testCreateStoragePersona() {
    StoragePersona storagePersona = new StoragePersona(name, quotaNumber, storesToEnforce, owners);
    Assert.assertEquals(storagePersona.getName(), name);
    Assert.assertEquals(storagePersona.getQuotaNumber(), quotaNumber);
    Assert.assertEquals(storagePersona.getStoresToEnforce(), storesToEnforce);
    Assert.assertEquals(storagePersona.getOwners(), owners);
  }

}
