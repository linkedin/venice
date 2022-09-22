package com.linkedin.alpini.router.api;

import com.linkedin.alpini.base.misc.BasicRequest;
import com.linkedin.alpini.base.misc.Metrics;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface MetricsProvider {
  Metrics provider(BasicRequest request);
}
