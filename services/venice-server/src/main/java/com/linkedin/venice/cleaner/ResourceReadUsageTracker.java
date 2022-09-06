package com.linkedin.venice.cleaner;

/**
 * This interface defines the API to track the read usage per resource.
 */
public interface ResourceReadUsageTracker {
  void recordReadUsage(String resourceName);
}
