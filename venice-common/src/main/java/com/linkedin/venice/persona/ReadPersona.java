package com.linkedin.venice.persona;

import java.util.Set;


/**
 * A type of {@link Persona} that enforces a read (bandwidth) limit across multiple stores.
 * */
public class ReadPersona extends Persona {
  public ReadPersona(String name, long quotaNumber, Set<String> storesToEnforce, Set<String> owners) {
    super(name, quotaNumber, storesToEnforce, owners);
  }
}
