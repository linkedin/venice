package com.linkedin.davinci.store.memory;

import java.nio.ByteBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;


public class InMemoryStoragePartitionTest {
  private static final int PARTITION_NUM = 0;
  private static final String KEY = "chiave";

  @Test
  public void testGet() {
    InMemoryStoragePartition storagePartition = new InMemoryStoragePartition(PARTITION_NUM);
    ByteBuffer keyBuffer = ByteBuffer.wrap(KEY.getBytes());
    Assert.assertNull(storagePartition.get(keyBuffer));
    storagePartition.put(KEY.getBytes(), keyBuffer);
    Assert.assertNotNull(storagePartition.get(keyBuffer));
  }
}
