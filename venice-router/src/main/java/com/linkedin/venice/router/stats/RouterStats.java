package com.linkedin.venice.router.stats;

import static com.linkedin.venice.read.RequestType.*;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.read.RequestType;
import java.util.function.Function;


public class RouterStats<STAT_TYPE> {
  private final STAT_TYPE statsForSingleGet;
  private final STAT_TYPE statsForMultiGet;
  private final STAT_TYPE statsForCompute;
  private final STAT_TYPE statsForMultiGetStreaming;
  private final STAT_TYPE statsForComputeStreaming;

  public RouterStats(Function<RequestType, STAT_TYPE> supplier) {
    this.statsForSingleGet = supplier.apply(SINGLE_GET);
    this.statsForMultiGet = supplier.apply(MULTI_GET);
    this.statsForCompute = supplier.apply(COMPUTE);
    this.statsForMultiGetStreaming = supplier.apply(MULTI_GET_STREAMING);
    this.statsForComputeStreaming = supplier.apply(COMPUTE_STREAMING);
  }

  public STAT_TYPE getStatsByType(RequestType requestType) {
    switch (requestType) {
      case SINGLE_GET:
        return statsForSingleGet;
      case MULTI_GET:
        return statsForMultiGet;
      case COMPUTE:
        return statsForCompute;
      case MULTI_GET_STREAMING:
        return statsForMultiGetStreaming;
      case COMPUTE_STREAMING:
        return statsForComputeStreaming;
      default:
        throw new VeniceException("Unknown request type: " + requestType);
    }
  }

}
