package com.linkedin.venice.store.exception;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;


public class InvalidDatabaseNameException extends VeniceException {
  public InvalidDatabaseNameException(PersistenceType type, String invalidDBName) {
    super("Invalid database name: " + invalidDBName + " for persistence type: " + type);
  }
}
