package com.linkedin.venice.router.stats;

import com.linkedin.ddsstorage.router.monitoring.ScatterGatherStats;
import com.linkedin.venice.exceptions.VeniceException;
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
  private final AggRouterHttpRequestStats statsForCompute;

  public LongTailRetryStatsProvider(AggRouterHttpRequestStats statsForSingleGet,
      AggRouterHttpRequestStats statsForMultiGet, AggRouterHttpRequestStats statsForCompute) {
    this.statsForSingleGet = statsForSingleGet;
    this.statsForMultiGet = statsForMultiGet;
    this.statsForCompute = statsForCompute;
  }

  @Override
  public ScatterGatherStats apply(VenicePath venicePath) {
    String storeName = venicePath.getStoreName();

    switch (venicePath.getRequestType()) {
      case SINGLE_GET:
        return statsForSingleGet.getScatterGatherStatsForStore(storeName);
      case MULTI_GET:
        return statsForMultiGet.getScatterGatherStatsForStore(storeName);
      case COMPUTE:
        return statsForCompute.getScatterGatherStatsForStore(storeName);
      default:
        throw new VeniceException("Request type " + venicePath.getRequestType() + " is not supported!");
    }
  }
}
