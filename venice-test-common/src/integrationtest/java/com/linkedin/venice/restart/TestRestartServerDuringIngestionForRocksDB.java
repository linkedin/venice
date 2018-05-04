package com.linkedin.venice.restart;

import com.linkedin.venice.meta.PersistenceType;


public class TestRestartServerDuringIngestionForRocksDB extends TestRestartServerDuringIngestion {
  @Override
  protected PersistenceType getPersistenceType() {
    return PersistenceType.ROCKS_DB;
  }
}
