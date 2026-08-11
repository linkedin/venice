package com.linkedin.venice.meta;

import com.linkedin.venice.utils.CollectionUtils;
import com.linkedin.venice.utils.VeniceEnumValueTest;
import java.util.Map;


public class StorageModeTest extends VeniceEnumValueTest<StorageMode> {
  public StorageModeTest() {
    super(StorageMode.class);
  }

  @Override
  protected Map<Integer, StorageMode> expectedMapping() {
    return CollectionUtils.<Integer, StorageMode>mapBuilder()
        .put(0, StorageMode.INTERNAL)
        .put(1, StorageMode.DUAL_WRITE)
        .put(2, StorageMode.EXTERNAL)
        .build();
  }
}
