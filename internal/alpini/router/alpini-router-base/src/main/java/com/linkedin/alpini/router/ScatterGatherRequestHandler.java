package com.linkedin.alpini.router;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.alpini.router.api.ResourcePath;
import com.linkedin.alpini.router.api.RouterTimeoutProcessor;
import com.linkedin.alpini.router.api.ScatterGatherHelper;
import java.util.Objects;
import java.util.concurrent.Executor;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public abstract class ScatterGatherRequestHandler<H, P extends ResourcePath<K>, K, R> {
  protected static final Logger LOG = LogManager.getLogger(ScatterGatherRequestHandler.class);
  protected static final Runnable NOP = () -> {};

  protected final @Nonnull RouterTimeoutProcessor _timeoutProcessor;

  protected ScatterGatherRequestHandler(@Nonnull RouterTimeoutProcessor timeoutProcessor) {
    _timeoutProcessor = Objects.requireNonNull(timeoutProcessor, "timeoutProcessor");
  }

  protected ScatterGatherRequestHandler(@Nonnull TimeoutProcessor timeoutProcessor) {
    this(RouterTimeoutProcessor.adapt(timeoutProcessor));
  }

  @SuppressWarnings("unchecked")
  public static <H, P extends ResourcePath<K>, K, R> ScatterGatherRequestHandler<H, P, K, R> make(
      @Nonnull ScatterGatherHelper<H, P, K, R, ?, ?, ?> scatterGatherHelper,
      @Nonnull TimeoutProcessor timeoutProcessor,
      @Nonnull Executor executor) {
    return make(scatterGatherHelper, RouterTimeoutProcessor.adapt(timeoutProcessor), executor);
  }

  @SuppressWarnings("unchecked")
  public static <H, P extends ResourcePath<K>, K, R> ScatterGatherRequestHandler<H, P, K, R> make(
      @Nonnull ScatterGatherHelper<H, P, K, R, ?, ?, ?> scatterGatherHelper,
      @Nonnull RouterTimeoutProcessor timeoutProcessor,
      @Nonnull Executor executor) {
    try {
      switch (scatterGatherHelper.dispatcherNettyVersion()) {
        case NETTY_3:
          return Class.forName(ScatterGatherRequestHandler.class.getName() + "3")
              .asSubclass(ScatterGatherRequestHandler.class)
              .getConstructor(scatterGatherHelper.getClass(), RouterTimeoutProcessor.class, Executor.class)
              .newInstance(scatterGatherHelper, timeoutProcessor, executor);
        case NETTY_4_1:
          return Class.forName(ScatterGatherRequestHandler.class.getName() + "4")
              .asSubclass(ScatterGatherRequestHandler.class)
              .getConstructor(scatterGatherHelper.getClass(), RouterTimeoutProcessor.class, Executor.class)
              .newInstance(scatterGatherHelper, timeoutProcessor, executor);
        default:
          throw new IllegalStateException();
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public abstract @Nonnull ScatterGatherHelper<H, P, K, R, ?, ?, ?> getScatterGatherHelper();

  protected abstract boolean isTooLongFrameException(Throwable cause);
}
