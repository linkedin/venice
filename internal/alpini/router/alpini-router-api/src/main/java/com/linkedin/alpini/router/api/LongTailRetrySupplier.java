package com.linkedin.alpini.router.api;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.misc.BasicRequest;
import java.util.function.LongSupplier;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface LongTailRetrySupplier<P extends ResourcePath<K>, K> {
  @Nonnull
  AsyncFuture<LongSupplier> getLongTailRetryMilliseconds(@Nonnull P path, @Nonnull String methodName);

  // Along with the path also accepts request as one of the parameters, the supplier can use information
  // such as routing policy from the headers to determine the long tail retry time.
  default AsyncFuture<LongSupplier> getLongTailRetryMilliseconds(@Nonnull P path, @Nonnull BasicRequest request) {
    return getLongTailRetryMilliseconds(path, request.getMethodName());
  }
}
