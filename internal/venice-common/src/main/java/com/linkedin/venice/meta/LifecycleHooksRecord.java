package com.linkedin.venice.meta;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.linkedin.venice.systemstore.schemas.StoreLifecycleHooksRecord;
import java.util.Map;


@JsonDeserialize(as = LifecycleHooksRecordImpl.class)
public interface LifecycleHooksRecord extends DataModelBackedStructure<StoreLifecycleHooksRecord> {
  String getStoreLifecycleHooksClassName();

  Map<String, String> getStoreLifecycleHooksParams();

  void setStoreLifecycleHooksClassName(String className);

  void setStoreLifecycleHooksParams(Map<String, String> params);
}
