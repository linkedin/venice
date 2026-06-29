package com.linkedin.venice.controller;

import com.linkedin.venice.hooks.StoreLifecycleHooks;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Instantiates and caches {@link StoreLifecycleHooks} by class name.
 *
 * <p>Shared across {@link DeferredVersionSwapService}, {@link VeniceHelixAdmin}, and
 * {@link com.linkedin.venice.pushmonitor.AbstractPushMonitor} so hook instantiation logic
 * is not duplicated.
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
   * <p>Failed instantiations are cached in {@code failedClasses} so that a bad class name is
   * only retried once; subsequent calls return {@code null} immediately without re-attempting
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
}
