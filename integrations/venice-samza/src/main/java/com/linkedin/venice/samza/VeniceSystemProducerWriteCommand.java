package com.linkedin.venice.samza;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;


/**
 * An immutable, already-serialized VeniceSystemProducer write (put, update, or delete) plus the two
 * futures that coordinate its lifecycle when it is dispatched asynchronously through the striped executor.
 *
 * <p>Two futures are tracked because a caller cares about two distinct moments:</p>
 * <ul>
 *   <li>the <em>submission</em> future completes once a worker thread has handed the record to the Venice
 *       writer (i.e. {@code writer.put/update/delete} returned) — this is what public {@code put}/{@code
 *       delete} and the Samza envelope {@code send} wait on to preserve their historic "submitted before
 *       return" contract;</li>
 *   <li>the <em>durable</em> future completes on the writer's asynchronous broker acknowledgement — this is
 *       the future the caller ultimately observes for durability.</li>
 * </ul>
 *
 * <p>The durable future is a {@link DurableWriteFuture} carrying its submission future so that
 * {@link #awaitSubmission(CompletableFuture)} can wait on submission without the producer having to thread
 * the pair around. A synchronous failure from the writer (size/hook/adapter) fails both futures and becomes
 * sticky; an asynchronous callback failure fails the durable future and becomes sticky. Once a synchronous
 * failure is recorded, a late asynchronous callback is ignored.</p>
 */
class VeniceSystemProducerWriteCommand {
  enum Operation {
    PUT, UPDATE, DELETE
  }

  private final Operation operation;
  private final byte[] key;
  private final byte[] value;
  private final int valueSchemaId;
  private final int derivedSchemaId;
  private final long logicalTimestamp;

  private final CompletableFuture<Void> submissionFuture = new CompletableFuture<>();
  private final DurableWriteFuture durableFuture = new DurableWriteFuture(submissionFuture);

  // Coordinates the terminal state between the synchronous submission and the asynchronous callback.
  private boolean submissionDone;
  private boolean submissionFailed;
  private boolean callbackArrived;
  private Throwable callbackFailure;

  private VeniceSystemProducerWriteCommand(
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

  static VeniceSystemProducerWriteCommand put(byte[] key, byte[] value, int valueSchemaId, long logicalTimestamp) {
    return new VeniceSystemProducerWriteCommand(Operation.PUT, key, value, valueSchemaId, -1, logicalTimestamp);
  }

  static VeniceSystemProducerWriteCommand update(
      byte[] key,
      byte[] value,
      int valueSchemaId,
      int derivedSchemaId,
      long logicalTimestamp) {
    return new VeniceSystemProducerWriteCommand(
        Operation.UPDATE,
        key,
        value,
        valueSchemaId,
        derivedSchemaId,
        logicalTimestamp);
  }

  static VeniceSystemProducerWriteCommand delete(byte[] key, long logicalTimestamp) {
    return new VeniceSystemProducerWriteCommand(Operation.DELETE, key, null, -1, -1, logicalTimestamp);
  }

  byte[] getKey() {
    return key;
  }

  DurableWriteFuture getDurableFuture() {
    return durableFuture;
  }

  /**
   * Invokes the matching writer operation exactly once with {@code callback}. Any synchronous exception
   * propagates to the caller (the worker), which converts it into a submission failure.
   */
  void submit(AbstractVeniceWriter<byte[], byte[], byte[]> writer, PubSubProducerCallback callback) {
    switch (operation) {
      case DELETE:
        writer.delete(key, logicalTimestamp, callback);
        break;
      case UPDATE:
        writer.update(key, value, valueSchemaId, derivedSchemaId, logicalTimestamp, callback);
        break;
      case PUT:
      default:
        writer.put(key, value, valueSchemaId, logicalTimestamp, callback);
        break;
    }
  }

  /**
   * Marks the submission complete and completes the submission future directly (so public wrappers unblock).
   * Returns a {@link Runnable} carrying the <em>durable</em> completion when one is now owed — a failure (which
   * also blocks any later asynchronous callback), or a success whose asynchronous callback already arrived — so
   * the dispatcher can run that completion off the stripe worker thread; returns {@code null} when no durable
   * completion is owed yet. Idempotent: a second call returns {@code null}.
   */
  synchronized Runnable finishSubmission(Throwable failure) {
    if (submissionDone) {
      return null;
    }
    submissionDone = true;
    if (failure != null) {
      submissionFailed = true;
      submissionFuture.completeExceptionally(failure);
      return () -> durableFuture.completeExceptionally(failure);
    }
    submissionFuture.complete(null);
    if (callbackArrived) {
      Throwable durableFailure = callbackFailure;
      return () -> completeDurable(durableFailure);
    }
    return null;
  }

  /**
   * Records the asynchronous writer callback. Returns a {@link Runnable} that completes the durable future when
   * the submission has already finished (the caller runs it directly, on the asynchronous callback thread), or
   * {@code null} when the callback is ignored (submission already failed, or a duplicate) or deferred (the
   * submission has not finished yet, so the worker's {@link #finishSubmission(Throwable)} will carry the
   * durable completion instead).
   */
  synchronized Runnable registerCallback(Throwable failure) {
    if (submissionFailed || callbackArrived) {
      return null;
    }
    callbackArrived = true;
    callbackFailure = failure;
    if (submissionDone) {
      return () -> completeDurable(failure);
    }
    return null;
  }

  private void completeDurable(Throwable failure) {
    if (failure != null) {
      durableFuture.completeExceptionally(failure);
    } else {
      durableFuture.complete(null);
    }
  }

  /**
   * A durable-write future that also carries the submission future it was created with, letting
   * {@link #awaitSubmission(CompletableFuture)} recognize producer-owned futures and wait on submission.
   */
  static class DurableWriteFuture extends CompletableFuture<Void> {
    private final CompletableFuture<Void> submissionFuture;

    DurableWriteFuture(CompletableFuture<Void> submissionFuture) {
      this.submissionFuture = submissionFuture;
    }

    CompletableFuture<Void> getSubmissionFuture() {
      return submissionFuture;
    }
  }

  /**
   * Waits for writer submission if {@code future} is a producer-owned {@link DurableWriteFuture}; otherwise
   * (inline path or a foreign future returned by a subclass override) returns immediately without waiting or
   * casting. Submission failures are surfaced to preserve the synchronous put/delete/send contract. The wait is
   * uninterruptible: once the command has been admitted the write proceeds regardless, so an interrupt cannot
   * abandon the wait (which would let the caller retry and duplicate the write); the interrupt is remembered and
   * the thread's interrupt flag is restored before this method returns or throws.
   */
  static void awaitSubmission(CompletableFuture<Void> future) {
    if (!(future instanceof DurableWriteFuture)) {
      return;
    }
    CompletableFuture<Void> submissionFuture = ((DurableWriteFuture) future).getSubmissionFuture();
    boolean interrupted = false;
    try {
      while (true) {
        try {
          submissionFuture.get();
          return;
        } catch (InterruptedException e) {
          // The command was already admitted and the write continues on the stripe worker, so abandoning the
          // wait would let the caller retry and duplicate the write. Remember the interrupt and keep waiting
          // for submission to finish; the flag is restored below before returning or throwing.
          interrupted = true;
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          if (cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
          }
          // A fatal Error (e.g. OutOfMemoryError, assertion) must propagate with its original identity rather
          // than being wrapped, so callers and the JVM see the true failure.
          if (cause instanceof Error) {
            throw (Error) cause;
          }
          throw new VeniceException("Write submission failed", cause);
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
