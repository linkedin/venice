package com.linkedin.venice.controller.util;

import com.linkedin.venice.meta.Store;
import java.util.HashSet;
import java.util.Set;


public class UpdateStoreWrapper {
  public final Set<CharSequence> updatedConfigs;
  public final Store originalStore;
  public final Store updatedStore;

  public UpdateStoreWrapper(Store originalStore) {
    this.originalStore = originalStore;
    this.updatedConfigs = new HashSet<>();
    this.updatedStore = originalStore.cloneStore();
  }
}
