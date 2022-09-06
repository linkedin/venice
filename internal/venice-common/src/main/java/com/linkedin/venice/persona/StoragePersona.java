package com.linkedin.venice.persona;

import java.util.Set;


/**
 * A type of {@link Persona} that enforces a storage (size) limit across multiple stores.
 * */
public class StoragePersona extends Persona {
  public StoragePersona(String name, long quotaNumber, Set<String> storesToEnforce, Set<String> owners) {
    super(name, quotaNumber, storesToEnforce, owners);
  }

  public StoragePersona() {
    super();
  }

  public StoragePersona(StoragePersona storagePersona) {
    super(storagePersona);
  }

  @Override
  public String toString() {
    return StoragePersona.class.getSimpleName() + "(\n" + "name: " + name + ",\n" + "quotaNumber: " + quotaNumber
        + ",\n" + "storesToEnforce: " + storesToEnforce.toString() + ",\n" + "owners: " + owners.toString() + ")";
  }

}
