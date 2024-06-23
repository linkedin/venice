package com.linkedin.davinci.bootstrap;

public interface RocksDbBootstrapper {
  void bootstrapDatabase(String storeName, int versionNumber, int partitionID) throws Exception;
}
