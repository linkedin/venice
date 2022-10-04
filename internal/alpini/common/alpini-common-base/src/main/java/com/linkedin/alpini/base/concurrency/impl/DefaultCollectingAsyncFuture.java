package com.linkedin.alpini.base.concurrency.impl;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.misc.CollectionUtil;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;


/**
 * A specialised instance of @{link AsyncFuture} for collecting the results of one or more
 * futures of lists into a single list.
 *
 * @author acurtis
 */
public class DefaultCollectingAsyncFuture<V> extends DefaultAsyncFuture<List<V>> {
  /**
   * Creates a new instance.
   *
   * @param futuresList List of @{AsyncFuture}s to be collated.
   * @param cancellable {@code true} if and only if this future can be canceled
   */
  public DefaultCollectingAsyncFuture(@Nonnull List<AsyncFuture<List<V>>> futuresList, boolean cancellable) {
    this(futuresList, cancellable, Function.identity());
  }

  /**
   * Creates a new instance.
   *
   * @param futuresList List of @{AsyncFuture}s to be collated.
   * @param cancellable {@code true} if and only if this future can be canceled
   * @param filter a {@linkplain Function} to filter the completing futures.
   */
  public DefaultCollectingAsyncFuture(
      @Nonnull List<AsyncFuture<List<V>>> futuresList,
      boolean cancellable,
      @Nonnull Function<List<V>, List<V>> filter) {
    super(cancellable);
    Objects.requireNonNull(filter, "filter must not be null");

    CompletableFuture<Stream<V>>[] futures = futuresList.stream()
        .map(future -> future.thenApply(List::stream).exceptionally(this::exception))
        .toArray(this::makeArray);

    CompletableFuture.allOf(futures)
        .thenApply(
            aVoid -> (isDone()
                ? Optional.<List<V>>empty()
                : Optional.of(
                    Stream.of(futures)
                        .filter(CompletableFuture::isDone)
                        .flatMap(CompletableFuture::join)
                        .collect(Collectors.toList()))).map(filter)
                            .filter(CollectionUtil::isNotEmpty)
                            .orElseGet(Collections::emptyList))
        .whenComplete(this::setComplete);
  }

  private Stream<V> exception(Throwable throwable) {
    completeExceptionally(throwable);
    return Stream.empty();
  }

  @SuppressWarnings("unchecked")
  private CompletableFuture<Stream<V>>[] makeArray(int i) {
    if (i == 0) {
      throw new IllegalArgumentException("List argument cannot be empty");
    }
    return new CompletableFuture[i];
  }
}
