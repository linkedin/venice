package com.linkedin.venice.meta;

import com.linkedin.venice.utils.CollectionUtils;
import com.linkedin.venice.utils.VeniceEnumValueTest;
import java.util.Map;


public class ExternalStorageReadModeTest extends VeniceEnumValueTest<ExternalStorageReadMode> {
  public ExternalStorageReadModeTest() {
    super(ExternalStorageReadMode.class);
  }

  @Override
  protected Map<Integer, ExternalStorageReadMode> expectedMapping() {
    return CollectionUtils.<Integer, ExternalStorageReadMode>mapBuilder()
        .put(0, ExternalStorageReadMode.VENICE_ONLY)
        .put(1, ExternalStorageReadMode.DUAL_MODE_CONSISTENCY_CHECK)
        .put(2, ExternalStorageReadMode.DUAL_MODE_EARLY_RETURN)
        .put(3, ExternalStorageReadMode.EXTERNAL_ONLY)
        .build();
  }
}
