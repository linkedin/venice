package com.linkedin.alpini.router.api;

import com.linkedin.alpini.base.misc.BasicRequest;
import com.linkedin.alpini.base.misc.Metrics;
import java.util.List;
import javax.annotation.Nonnull;


/**
 * Created by acurtis on 6/19/17.
 */
public interface ResponseAggregatorFactory<BASIC_HTTP_REQUEST extends BasicRequest, HTTP_RESPONSE> {
  @Nonnull
  HTTP_RESPONSE buildResponse(
      @Nonnull BASIC_HTTP_REQUEST request,
      Metrics metrics,
      @Nonnull List<HTTP_RESPONSE> gatheredResponses);
}
