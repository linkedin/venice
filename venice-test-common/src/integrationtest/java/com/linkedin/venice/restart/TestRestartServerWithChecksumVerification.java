package com.linkedin.venice.restart;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.meta.PersistenceType;
import java.util.Properties;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.store.rocksdb.RocksDBServerConfig.*;


public class TestRestartServerWithChecksumVerification extends TestRestartServerDuringIngestion {
  @Override
  protected PersistenceType getPersistenceType() {
    return PersistenceType.ROCKS_DB;
  }

  @Override
  protected Properties getExtraProperties() {
    Properties extraProperties = new Properties();
    extraProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    extraProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    return extraProperties;
  }
}
