package com.linkedin.venice.router.stats;

import com.linkedin.ddsstorage.router.monitoring.ScatterGatherStats;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.api.path.VenicePath;
import java.util.function.Function;


/**
 * This class is a stats generator for dds router framework to record scattering/gathering
 * related metrics.
 * Venice is using this class to expose the internal stats to Venice and report them.
 */
public class LongTailRetryStatsProvider implements Function<VenicePath, ScatterGatherStats> {
  private final AggRouterHttpRequestStats statsForSingleGet;
  private final AggRouterHttpRequestStats statsForMultiGet;

  public LongTailRetryStatsProvider(AggRouterHttpRequestStats statsForSingleGet,
      AggRouterHttpRequestStats statsForMultiGet) {
    this.statsForSingleGet = statsForSingleGet;
    this.statsForMultiGet = statsForMultiGet;
  }

  @Override
  public ScatterGatherStats apply(VenicePath venicePath) {
    String storeName = venicePath.getStoreName();
    RequestType requestType = venicePath.getRequestType();

    if (requestType.equals(RequestType.SINGLE_GET)) {
      return statsForSingleGet.getScatterGatherStatsForStore(storeName);
    }
    return statsForMultiGet.getScatterGatherStatsForStore(storeName);
  }
}
