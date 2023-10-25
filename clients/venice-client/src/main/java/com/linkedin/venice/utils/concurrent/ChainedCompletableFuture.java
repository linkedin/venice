package com.linkedin.venice.utils.concurrent;

import java.util.concurrent.CompletableFuture;


/**
 * A utility class to chain two completable futures together. This is useful when the future is initially an incomplete
 * future, and we want to allow the caller to complete the future later. The caller can use the original future to
 * complete, and use the result future to get the result.
 *
 * Ideally, the result is a part of the chain of the original future and executes on completion of the original future
 * (or any of its dependent tasks).
 * @param <I>
 * @param <O>
 */
public class ChainedCompletableFuture<I, O> {
  private CompletableFuture<I> originalFuture;
  private CompletableFuture<O> resultFuture;

  public ChainedCompletableFuture(CompletableFuture<I> originalFuture, CompletableFuture<O> resultFuture) {
    this.originalFuture = originalFuture;
    this.resultFuture = resultFuture;
  }

  public CompletableFuture<I> getOriginalFuture() {
    return originalFuture;
  }

  public CompletableFuture<O> getResultFuture() {
    return resultFuture;
  }
}
