package com.linkedin.venice.store.rocksdb;

public enum RocksDBComputeAccessMode {
  SINGLE_GET(0),
  SINGLE_GET_WITH_REUSE(1);

  private int value;

  RocksDBComputeAccessMode(int value) {
    this.value = value;
  }

  public static RocksDBComputeAccessMode fromInt(int i) {
    for (RocksDBComputeAccessMode rocksDBComputeAccessMode : values()) {
      if (rocksDBComputeAccessMode.ordinal() == i) {
        return rocksDBComputeAccessMode;
      }
    }
    return SINGLE_GET;
  }
}
