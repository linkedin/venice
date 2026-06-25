package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.hooks.StoreLifecycleHooks;
import com.linkedin.venice.hooks.StoreVersionLifecycleEventOutcome;
import com.linkedin.venice.meta.LifecycleHooksRecord;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Instantiates and caches {@link StoreLifecycleHooks} by class name, and dispatches
 * {@link StoreLifecycleHooks#postStoreVersionSwap} for all hooks registered on a store.
 *
 * <p>Shared across {@link DeferredVersionSwapService}, {@link VeniceHelixAdmin}, and
 * {@link com.linkedin.venice.pushmonitor.AbstractPushMonitor} so hook instantiation logic
 * is not duplicated.
 */
public class StoreLifecycleHookExecutor {
  private static final Logger LOGGER = LogManager.getLogger(StoreLifecycleHookExecutor.class);

  private final VeniceProperties globalProps;
  private final Map<String, StoreLifecycleHooks> hooksCache = new ConcurrentHashMap<>();

  public StoreLifecycleHookExecutor(VeniceProperties globalProps) {
    this.globalProps = globalProps;
  }

  /**
   * Returns a cached (or freshly constructed) {@link StoreLifecycleHooks} instance for the
   * given fully-qualified class name. Returns {@code null} and logs a warning if the class
   * cannot be loaded or instantiated.
   *
   * <p>Package-private so {@link DeferredVersionSwapService} can reuse the cache while keeping
   * its own per-hook outcome control flow.
   *
   * <p>Note: {@link ConcurrentHashMap} does not permit null values, so failed instantiations are
   * not cached. This means a bad class name will be retried on every call, but that is acceptable
   * since failures are expected to be rare and the log warning is still emitted.
   */
  @Nullable
  StoreLifecycleHooks getOrInstantiateHook(String className) {
    StoreLifecycleHooks cached = hooksCache.get(className);
    if (cached != null) {
      return cached;
    }
    try {
      StoreLifecycleHooks hook = ReflectUtils.callConstructor(
          ReflectUtils.loadClass(className),
          new Class<?>[] { VeniceProperties.class },
          new Object[] { globalProps });
      hooksCache.put(className, hook);
      return hook;
    } catch (Exception e) {
      LOGGER.warn("Failed to instantiate StoreLifecycleHooks class {}: {}", className, e.getMessage(), e);
      return null;
    }
  }

  /**
   * Invokes {@link StoreLifecycleHooks#postStoreVersionSwap} for every hook registered on
   * {@code store}. Outcomes of WAIT / ABORT / ROLLBACK are logged and a warning is emitted,
   * but do NOT cause a rollback — the version swap has already been committed by the caller.
   *
   * <p>Any exception thrown by a hook is caught, logged, and skipped (consistent with the
   * {@link StoreLifecycleHooks} contract that all hook exceptions are swallowed).
   */
  public void invokePostVersionSwapHooks(
      String clusterName,
      Store store,
      int versionNumber,
      String regionName,
      @Nullable Lazy<JobStatusQueryResponse> jobStatus) {
    for (LifecycleHooksRecord record: store.getStoreLifecycleHooks()) {
      String className = record.getStoreLifecycleHooksClassName();
      StoreLifecycleHooks hook = getOrInstantiateHook(className);
      if (hook == null) {
        continue;
      }
      Properties props = new Properties();
      props.putAll(record.getStoreLifecycleHooksParams());
      try {
        StoreVersionLifecycleEventOutcome outcome = hook.postStoreVersionSwap(
            clusterName,
            store.getName(),
            versionNumber,
            regionName,
            jobStatus,
            new VeniceProperties(props));
        if (!StoreVersionLifecycleEventOutcome.PROCEED.equals(outcome)) {
          LOGGER.warn(
              "postStoreVersionSwap hook {} returned {} for store {} v{} in region {} — "
                  + "swap already committed, ignoring non-PROCEED outcome",
              className,
              outcome,
              store.getName(),
              versionNumber,
              regionName);
        } else {
          LOGGER.debug(
              "postStoreVersionSwap hook {} returned PROCEED for store {} v{} in region {}",
              className,
              store.getName(),
              versionNumber,
              regionName);
        }
      } catch (Exception e) {
        LOGGER.error(
            "Exception in postStoreVersionSwap hook {} for store {} v{}: {}",
            className,
            store.getName(),
            versionNumber,
            e.getMessage(),
            e);
      }
    }
  }
}
