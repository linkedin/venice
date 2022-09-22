package com.linkedin.alpini.netty4.misc;

import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Created by acurtis on 2/1/17.
 */
public final class Futures {
  private static final Logger LOG = LogManager.getLogger(Futures.class);

  private Futures() {
    throw new IllegalStateException("Never instantiated");
  }

  @SuppressWarnings("unchecked")
  public static <T> CompletableFuture<T> asCompletableFuture(Future<T> future) {
    CompletableFuture<T> completableFuture;

    if (future.isSuccess()) {
      completableFuture = CompletableFuture.completedFuture(future.getNow());
    } else {
      completableFuture = new CompletableFuture() {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
          return super.cancel(mayInterruptIfRunning) && future.cancel(mayInterruptIfRunning);
        }
      };
      if (future.isCancelled()) {
        completableFuture.cancel(false);
      } else {
        future.addListener(listener(completableFuture));
      }
    }
    return completableFuture;
  }

  public static <T> Future<T> asNettyFuture(@Nonnull CompletableFuture<T> completableFuture) {
    return asNettyFuture(ImmediateEventExecutor.INSTANCE, completableFuture);
  }

  public static <T> Future<T> asNettyFuture(
      @Nonnull EventExecutor eventExecutor,
      @Nonnull CompletableFuture<T> completableFuture) {
    Promise<T> promise = new DefaultPromise<T>(eventExecutor) {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return super.cancel(mayInterruptIfRunning) && completableFuture.cancel(mayInterruptIfRunning);
      }

      @Override
      protected void checkDeadLock() {
        // No check
      }
    };
    completableFuture.whenComplete(complete(promise));
    if (completableFuture.isCancelled()) {
      promise.cancel(false);
    }
    return promise;
  }

  public static Future<Void> allOf(Future<?>... futures) {
    return allOf(ImmediateEventExecutor.INSTANCE, futures);
  }

  public static Future<Void> allOf(@Nonnull EventExecutor eventExecutor, Future<?>... futures) {
    return asNettyFuture(
        eventExecutor,
        CompletableFuture.allOf(
            Stream.of(futures)
                .filter(Objects::nonNull)
                .map(Futures::asCompletableFuture)
                .toArray(CompletableFuture[]::new)));
  }

  public static Future<?> anyOf(Future<?>... futures) {
    return asNettyFuture(
        CompletableFuture.anyOf(
            Stream.of(futures)
                .filter(Objects::nonNull)
                .map(Futures::asCompletableFuture)
                .toArray(CompletableFuture[]::new)));
  }

  public static <V> BiConsumer<V, Throwable> complete(@Nonnull Promise<V> promise) {
    return complete(promise, Futures::defaultDisposal);
  }

  public static <V> BiConsumer<V, Throwable> complete(@Nonnull Promise<V> promise, @Nonnull Consumer<V> dispose) {
    return (result, ex) -> {
      if (ex == null) {
        if (!promise.trySuccess(result)) {
          dispose.accept(result);
        }
      } else {
        if (!promise.tryFailure(ex)) {
          logException(promise, ex);
        }
      }
    };
  }

  public static <V> BiConsumer<Future<V>, Throwable> completeFuture(@Nonnull Promise<V> promise) {
    return (result, ex) -> {
      if (ex == null) {
        if (result != null) {
          result.addListener(listener(promise));
        } else {
          LOG.warn("unexpected null future", new NullPointerException());
        }
      } else {
        if (!promise.tryFailure(ex)) {
          logException(promise, ex);
        }
      }
    };
  }

  public static <T, V> FutureListener<T> voidListener(@Nonnull Promise<V> promise) {
    return f -> {
      if (f.isSuccess()) {
        promise.trySuccess(null);
      } else {
        if (!promise.tryFailure(f.cause())) {
          logException(promise, f.cause());
        }
      }
    };
  }

  public static <V> FutureListener<V> listener(@Nonnull Promise<V> promise) {
    return listener(promise, Futures::defaultDisposal);
  }

  public static <V> FutureListener<V> listener(@Nonnull Promise<V> promise, @Nonnull Consumer<V> dispose) {
    return f -> {
      if (f.isSuccess()) {
        V value = f.getNow();
        if (!promise.trySuccess(value)) {
          dispose.accept(value);
        }
      } else {
        if (!promise.tryFailure(f.cause())) {
          logException(promise, f.cause());
        }
      }
    };
  }

  public static <V> FutureListener<V> listener(@Nonnull CompletableFuture<V> completableFuture) {
    return listener(completableFuture, Futures::defaultDisposal);
  }

  public static <V> FutureListener<V> listener(
      @Nonnull CompletableFuture<V> completableFuture,
      @Nonnull Consumer<V> dispose) {
    return f -> {
      if (f.isSuccess()) {
        V value = f.getNow();
        if (!completableFuture.complete(value)) {
          dispose.accept(value);
        }
      } else {
        if (!completableFuture.completeExceptionally(f.cause())) {
          logException(completableFuture, f.cause());
        }
      }
    };
  }

  private static void defaultDisposal(Object o) {
    if (o != null) {
      LOG.debug("Failed to propagate value: {}", o);
    }
  }

  private static void logException(java.util.concurrent.Future<?> f, Throwable e) {
    LOG.log(f.isCancelled() ? Level.DEBUG : Level.WARN, "Failed to complete future exceptionally", e);
  }
}
