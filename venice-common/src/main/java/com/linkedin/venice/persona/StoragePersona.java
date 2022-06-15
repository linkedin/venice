package com.linkedin.venice.persona;

import java.util.Set;

/**
 * A type of {@link Persona} that enforces a storage (size) limit across multiple stores.
 * */
public class StoragePersona extends Persona {

  public StoragePersona(String name, long quotaNumber, Set<String> storesToEnforce,
      Set<String> owners) {
    super(name, quotaNumber, storesToEnforce, owners);
  }

  public StoragePersona(StoragePersona storagePersona) {
    super(storagePersona);
  }

}
