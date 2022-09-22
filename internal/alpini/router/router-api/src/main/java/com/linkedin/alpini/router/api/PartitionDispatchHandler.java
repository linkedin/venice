package com.linkedin.alpini.router.api;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.concurrency.AsyncPromise;
import com.linkedin.alpini.base.misc.BasicRequest;
import java.util.List;
import java.util.concurrent.Executor;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface PartitionDispatchHandler<H, P extends ResourcePath<K>, K, HTTP_REQUEST extends BasicRequest, HTTP_RESPONSE, HTTP_RESPONSE_STATUS> {
  /**
   * Performs the work.
   * @param scatter The scatter-gather request.
   * @param part The part of the scatter-gather request to be executed.
   * @param path The derived path of this part of the request.
   * @param request The original HttpRequest object.
   * @param hostSelected To be completed with the actual host selected.
   * @param responseFuture To be completed with HttpResponse parts.
   * @param retryFuture To be completed with the HttpResponseStatus.
   * @param timeoutFuture Listen for timeout.
   * @param contextExecutor For executing work on context executor. A single thread is available for each request.
   *                        For efficiency, the AsyncPromises should be completed on the context executor thread.
   */
  void dispatch(
      @Nonnull Scatter<H, P, K> scatter,
      @Nonnull ScatterGatherRequest<H, K> part,
      @Nonnull P path,
      @Nonnull HTTP_REQUEST request,
      @Nonnull AsyncPromise<H> hostSelected,
      @Nonnull AsyncPromise<List<HTTP_RESPONSE>> responseFuture,
      @Nonnull AsyncPromise<HTTP_RESPONSE_STATUS> retryFuture,
      @Nonnull AsyncFuture<Void> timeoutFuture,
      @Nonnull Executor contextExecutor) throws RouterException;
}
