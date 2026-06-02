package com.linkedin.venice.controller.storeconfig;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.LifecycleHooksRecord;
import com.linkedin.venice.meta.Store;
import java.util.Collections;
import java.util.List;
import java.util.Optional;


/**
 * Validation rules for the per-store lifecycle-hooks list, applied at update-store time.
 * Pure computation against in-memory state; no I/O.
 */
public final class StoreLifecycleHooksPolicy {
  private StoreLifecycleHooksPolicy() {
  }

  /**
   * Resolve the effective lifecycle-hooks list for a store update: when the caller did not
   * supply a new list, keep the current one; when they did, reject any entry without a class
   * name and otherwise return the new list as-is.
   */
  public static List<LifecycleHooksRecord> validateLifecycleHooks(
      Store oldStore,
      Optional<List<LifecycleHooksRecord>> newLifecycleHooks) {
    List<LifecycleHooksRecord> currLifecycleHooks = oldStore.getStoreLifecycleHooks();

    // No change to existing store lifecycle hooks
    if (!newLifecycleHooks.isPresent()) {
      return currLifecycleHooks.isEmpty() ? Collections.emptyList() : currLifecycleHooks;
    }

    // Validate new lifecycle hooks
    for (LifecycleHooksRecord lifecycleHooksRecord: newLifecycleHooks.get()) {
      String className = lifecycleHooksRecord.getStoreLifecycleHooksClassName();
      if (className == null || className.trim().isEmpty()) {
        throw new VeniceException("Cannot add lifecycle hooks with blank class name");
      }

      // Normalize incoming class names to avoid storing accidental surrounding whitespace.
      lifecycleHooksRecord.setStoreLifecycleHooksClassName(className.trim());
    }

    return newLifecycleHooks.get();
  }
}
