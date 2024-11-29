package com.linkedin.venice.router;

import java.util.TreeMap;


/**
 * A facade for the {@link VeniceRouterConfig}, so that retry-related configs can be passed around without giving access
 * to the rest of the configs.
 */
public interface RouterRetryConfig {
  TreeMap<Integer, Integer> getLongTailRetryForBatchGetThresholdMs();

  int getLongTailRetryForSingleGetThresholdMs();

  int getLongTailRetryMaxRouteForMultiKeyReq();

  int getSmartLongTailRetryAbortThresholdMs();

  boolean isSmartLongTailRetryEnabled();
}
