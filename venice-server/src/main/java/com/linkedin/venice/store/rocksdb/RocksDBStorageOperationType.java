package com.linkedin.venice.store.rocksdb;

public enum RocksDBStorageOperationType {
  SINGLE_GET(0),
  SINGLE_GET_WITH_REUSE(1);

  private int value;

  RocksDBStorageOperationType(int value) {
    this.value = value;
  }

  public static RocksDBStorageOperationType fromInt(int i) {
    for (RocksDBStorageOperationType rocksDBStorageOperationType : values()) {
      if (rocksDBStorageOperationType.ordinal() == i) {
        return rocksDBStorageOperationType;
      }
    }
    return SINGLE_GET;
  }
}
