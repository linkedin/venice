package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.hooks.StoreLifecycleHooks;
import com.linkedin.venice.hooks.StoreVersionLifecycleEventOutcome;
import com.linkedin.venice.meta.LifecycleHooksRecord;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Instantiates and caches {@link StoreLifecycleHooks} by class name, and dispatches
 * {@link StoreLifecycleHooks#postStoreVersionSwap} for all hooks registered on a store.
 *
 * <p>Shared across {@link DeferredVersionSwapService}, {@link VeniceHelixAdmin}, and
 * {@link com.linkedin.venice.pushmonitor.AbstractPushMonitor} so hook instantiation and
 * invocation logic is not duplicated.
 */
public class StoreLifecycleHooksCache {
  private static final Logger LOGGER = LogManager.getLogger(StoreLifecycleHooksCache.class);

  private final VeniceProperties globalProps;
  private final Map<String, StoreLifecycleHooks> hooksCache = new ConcurrentHashMap<>();
  private final Set<String> failedClasses = ConcurrentHashMap.newKeySet();

  public StoreLifecycleHooksCache(VeniceProperties globalProps) {
    this.globalProps = globalProps;
  }

  /**
   * Returns a cached (or freshly constructed) {@link StoreLifecycleHooks} instance for the
   * given fully-qualified class name. Returns {@code null} and logs a warning if the class
   * cannot be loaded or instantiated.
   *
   * <p>Null or blank class names return {@code null} immediately. The name is trimmed before
   * lookup so whitespace variants do not create separate cache entries.
   *
   * <p>Failed instantiations are cached in {@code failedClasses} so that a bad class name is
   * only attempted once; subsequent calls return {@code null} immediately without re-attempting
   * reflection.
   */
  @Nullable
  public StoreLifecycleHooks getOrInstantiateHook(String className) {
    if (className == null || className.trim().isEmpty()) {
      return null;
    }
    String normalizedName = className.trim();
    if (failedClasses.contains(normalizedName)) {
      return null;
    }
    StoreLifecycleHooks cached = hooksCache.get(normalizedName);
    if (cached != null) {
      return cached;
    }
    try {
      StoreLifecycleHooks hook = ReflectUtils.callConstructor(
          ReflectUtils.loadClass(normalizedName),
          new Class<?>[] { VeniceProperties.class },
          new Object[] { globalProps });
      hooksCache.put(normalizedName, hook);
      return hook;
    } catch (Exception e) {
      LOGGER.warn("Failed to instantiate StoreLifecycleHooks class {}: {}", normalizedName, e.getMessage(), e);
      failedClasses.add(normalizedName);
      return null;
    }
  }

  /**
   * Invokes {@link StoreLifecycleHooks#postStoreVersionSwap} for every hook registered on
   * {@code store} and returns the worst outcome across all hooks
   * (ABORT = ROLLBACK &gt; WAIT &gt; PROCEED).
   *
   * <p>Null or malformed {@link LifecycleHooksRecord}s are skipped. Any exception thrown by a
   * hook is caught, logged, and the hook is skipped without affecting the accumulated outcome.
   */
  public StoreVersionLifecycleEventOutcome invokePostVersionSwapHooks(
      String clusterName,
      Store store,
      int versionNumber,
      int previousVersion,
      String regionName,
      @Nullable Lazy<JobStatusQueryResponse> jobStatus) {
    StoreVersionLifecycleEventOutcome worstOutcome = StoreVersionLifecycleEventOutcome.PROCEED;
    List<LifecycleHooksRecord> hooks = store.getStoreLifecycleHooks();
    if (hooks == null || hooks.isEmpty()) {
      return worstOutcome;
    }
    for (LifecycleHooksRecord record: hooks) {
      if (record == null || record.getStoreLifecycleHooksClassName() == null) {
        continue;
      }
      StoreLifecycleHooks hook = getOrInstantiateHook(record.getStoreLifecycleHooksClassName());
      if (hook == null) {
        continue;
      }
      Map<String, String> params = record.getStoreLifecycleHooksParams();
      Properties props = new Properties();
      if (params != null) {
        props.putAll(params);
      }
      StoreVersionLifecycleEventOutcome outcome;
      try {
        outcome = hook.postStoreVersionSwap(
            clusterName,
            store.getName(),
            versionNumber,
            previousVersion,
            regionName,
            jobStatus,
            new VeniceProperties(props));
      } catch (Exception e) {
        LOGGER.error(
            "Exception in postStoreVersionSwap hook {} for store {} v{}: {}",
            record.getStoreLifecycleHooksClassName(),
            store.getName(),
            versionNumber,
            e.getMessage(),
            e);
        continue;
      }
      if (StoreVersionLifecycleEventOutcome.PROCEED.equals(outcome)) {
        LOGGER.debug(
            "postStoreVersionSwap hook {} returned PROCEED for store {} v{} in region {}",
            record.getStoreLifecycleHooksClassName(),
            store.getName(),
            versionNumber,
            regionName);
      } else {
        LOGGER.warn(
            "postStoreVersionSwap hook {} returned {} for store {} v{} in region {}",
            record.getStoreLifecycleHooksClassName(),
            outcome,
            store.getName(),
            versionNumber,
            regionName);
        if (isWorse(outcome, worstOutcome)) {
          worstOutcome = outcome;
        }
      }
    }
    return worstOutcome;
  }

  private static boolean isWorse(
      StoreVersionLifecycleEventOutcome candidate,
      StoreVersionLifecycleEventOutcome current) {
    return severity(candidate) > severity(current);
  }

  private static int severity(StoreVersionLifecycleEventOutcome outcome) {
    switch (outcome) {
      case ABORT:
      case ROLLBACK:
        return 2;
      case WAIT:
        return 1;
      default:
        return 0;
    }
  }
}
