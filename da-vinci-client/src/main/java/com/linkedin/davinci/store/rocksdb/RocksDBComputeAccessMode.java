package com.linkedin.davinci.store.rocksdb;

public enum RocksDBComputeAccessMode {
  SINGLE_GET(0),
  SINGLE_GET_WITH_REUSE(1);

  private int value;

  RocksDBComputeAccessMode(int value) {
    this.value = value;
  }
}
