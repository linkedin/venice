package com.linkedin.venice.meta;

import com.linkedin.venice.systemstore.schemas.StoreLifecycleHooksRecord;
import java.util.Map;


public interface LifecycleHooksRecord extends DataModelBackedStructure<StoreLifecycleHooksRecord> {
  String getStoreLifecycleHooksClassName();

  Map<String, String> getStoreLifecycleHooksParams();
}
