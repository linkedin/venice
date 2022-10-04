package com.linkedin.alpini.router.api;

import com.linkedin.alpini.base.misc.Headers;
import com.linkedin.alpini.base.misc.Metrics;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface MetricsDecoder {
  Metrics decode(Headers headers);
}
