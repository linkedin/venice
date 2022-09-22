package com.linkedin.venice.router.stats;

import com.linkedin.alpini.router.monitoring.ScatterGatherStats;
import com.linkedin.venice.router.api.path.VenicePath;
import java.util.function.Function;


/**
 * This class is a stats generator for dds router framework to record scattering/gathering
 * related metrics.
 * Venice is using this class to expose the internal stats to Venice and report them.
 */
public class LongTailRetryStatsProvider implements Function<VenicePath, ScatterGatherStats> {
  private final RouterStats<AggRouterHttpRequestStats> routerStats;

  public LongTailRetryStatsProvider(RouterStats<AggRouterHttpRequestStats> routerStats) {
    this.routerStats = routerStats;
  }

  @Override
  public ScatterGatherStats apply(VenicePath venicePath) {
    String storeName = venicePath.getStoreName();

    return routerStats.getStatsByType(venicePath.getRequestType()).getScatterGatherStatsForStore(storeName);
  }
}
