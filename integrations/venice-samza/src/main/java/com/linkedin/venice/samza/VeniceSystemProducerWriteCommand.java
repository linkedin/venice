package com.linkedin.venice.samza;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;


/** Immutable serialized write plus its private submission and caller-visible durability phases. */
final class VeniceSystemProducerWriteCommand {
  static final int CALLBACK_IGNORED = -1;
  static final int CALLBACK_DEFERRED = 0;
  static final int CALLBACK_READY = 1;

  enum Operation {
    PUT, UPDATE, DELETE
  }

  final Operation operation;
  final byte[] key;
  final byte[] value;
  final int valueSchemaId;
  final int derivedSchemaId;
  final long logicalTimestamp;

  private final CompletableFuture<Void> submissionFuture = new CompletableFuture<>();
  private final CompletableFuture<Void> durableFuture = new DurableWriteFuture(submissionFuture);
  private boolean submissionComplete;
  private boolean callbackInvoked;
  private boolean hasDeferredCompletion;
  private Throwable deferredFailure;

  VeniceSystemProducerWriteCommand(
      Operation operation,
      byte[] key,
      byte[] value,
      int valueSchemaId,
      int derivedSchemaId,
      long logicalTimestamp) {
    this.operation = operation;
    this.key = key;
    this.value = value;
    this.valueSchemaId = valueSchemaId;
    this.derivedSchemaId = derivedSchemaId;
    this.logicalTimestamp = logicalTimestamp;
  }

  CompletableFuture<Void> getDurableFuture() {
    return durableFuture;
  }

  static Future<Void> getSubmissionFuture(CompletableFuture<Void> durableFuture) {
    return durableFuture instanceof DurableWriteFuture
        ? ((DurableWriteFuture) durableFuture).submissionFuture
        : CompletableFuture.completedFuture(null);
  }

  synchronized Runnable finishSubmission(Throwable failure) {
    if (submissionComplete) {
      return null;
    }
    if (failure != null) {
      callbackInvoked = true;
    }
    submissionComplete = true;
    completeSubmission(failure);
    Throwable completionFailure = failure == null ? deferredFailure : failure;
    boolean shouldCompleteDurable = failure != null || hasDeferredCompletion;
    return shouldCompleteDurable ? () -> completeDurable(completionFailure) : null;
  }

  synchronized int registerCallback(Throwable failure) {
    if (callbackInvoked) {
      return CALLBACK_IGNORED;
    }
    callbackInvoked = true;
    if (!submissionComplete) {
      hasDeferredCompletion = true;
      deferredFailure = failure;
      return CALLBACK_DEFERRED;
    }
    return CALLBACK_READY;
  }

  void completeDurable(Throwable failure) {
    if (failure == null) {
      durableFuture.complete(null);
    } else {
      durableFuture.completeExceptionally(failure);
    }
  }

  private void completeSubmission(Throwable failure) {
    if (failure == null) {
      submissionFuture.complete(null);
    } else {
      submissionFuture.completeExceptionally(failure);
    }
  }

  private static final class DurableWriteFuture extends CompletableFuture<Void> {
    private final Future<Void> submissionFuture;

    private DurableWriteFuture(Future<Void> submissionFuture) {
      this.submissionFuture = submissionFuture;
    }
  }
}
