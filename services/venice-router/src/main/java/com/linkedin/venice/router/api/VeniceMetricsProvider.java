package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.base.misc.Metrics;
import com.linkedin.ddsstorage.netty4.misc.BasicFullHttpRequest;
import java.util.function.Function;


/**
 * This is used to initialize a {@link Metrics} for each request.
 */
public class VeniceMetricsProvider implements Function<BasicFullHttpRequest, Metrics> {
  @Override
  public Metrics apply(BasicFullHttpRequest basicFullHttpRequest) {
    return new Metrics();
  }
}
