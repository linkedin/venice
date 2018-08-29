package com.linkedin.venice.restart;

import com.linkedin.venice.meta.PersistenceType;


public class TestRestartServerDuringIngestionForBDB extends TestRestartServerDuringIngestion {
  @Override
  protected PersistenceType getPersistenceType() {
    return PersistenceType.BDB;
  }
}
