package com.linkedin.venice.meta;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.venice.systemstore.schemas.StoreLifecycleHooksRecord;
import com.linkedin.venice.utils.CollectionUtils;
import java.util.Map;


public class LifecycleHooksRecordImpl implements LifecycleHooksRecord {
  StoreLifecycleHooksRecord storeLifecycleHooksRecord;

  public LifecycleHooksRecordImpl() {
    storeLifecycleHooksRecord = new StoreLifecycleHooksRecord();
  }

  public LifecycleHooksRecordImpl(
      @JsonProperty("storeLifecycleHooksClassName") String storeLifecycleHooksClassName,
      @JsonProperty("storeLifecycleHooksParams") Map<String, String> storeLifecycleHooksParams) {
    this();
    this.storeLifecycleHooksRecord.setStoreLifecycleHooksClassName(storeLifecycleHooksClassName);
    this.storeLifecycleHooksRecord
        .setStoreLifecycleHooksParams(CollectionUtils.convertStringMapToCharSequenceMap(storeLifecycleHooksParams));
  }

  @Override
  public String getStoreLifecycleHooksClassName() {
    return this.storeLifecycleHooksRecord.getStoreLifecycleHooksClassName().toString();
  }

  @Override
  public Map<String, String> getStoreLifecycleHooksParams() {
    return CollectionUtils
        .convertCharSequenceMapToStringMap(this.storeLifecycleHooksRecord.getStoreLifecycleHooksParams());
  }

  public void setStoreLifecycleHooksClassName(String className) {
    this.storeLifecycleHooksRecord.setStoreLifecycleHooksClassName(className);
  }

  public void setStoreLifecycleHooksParams(Map<String, String> params) {
    this.storeLifecycleHooksRecord
        .setStoreLifecycleHooksParams(CollectionUtils.convertStringMapToCharSequenceMap(params));
  }

  @Override
  public StoreLifecycleHooksRecord dataModel() {
    return this.storeLifecycleHooksRecord;
  }
}
