package com.linkedin.venice.persona;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class StoragePersonaJSONSerializerTest {
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
  public void testPersonaSerializeAndDeserialize() throws IOException {

    StoragePersona persona = new StoragePersona(name, quotaNumber, storesToEnforce, owners);
    StoragePersonaJSONSerializer serializer = new StoragePersonaJSONSerializer();
    byte[] data = serializer.serialize(persona, null);

    Assert.assertEquals(persona, serializer.deserialize(data, null));
    // Update status and compare again.
    persona.setName("testUserNew");
    data = serializer.serialize(persona, null);
    Assert.assertEquals(serializer.deserialize(data, null), persona);
  }
}
