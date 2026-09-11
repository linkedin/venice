package com.linkedin.venice.samza;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;


/**
 * An immutable, already-serialized VeniceSystemProducer write (put, update, or delete) with a two-phase future
 * contract for asynchronous dispatch: the <em>submission</em> future completes once a worker hands the record to the
 * writer (what public {@code put}/{@code delete} and the Samza {@code send} wait on to keep their "submitted before
 * return" contract), while the <em>durable</em> future completes on the writer's async broker acknowledgement (what
 * the caller observes for durability). The durable future is a {@link DurableWriteFuture} that carries its submission
 * future; once a synchronous failure is recorded a late callback is ignored.
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
   * A durable-write future that also carries its submission future, letting {@link #awaitSubmission(CompletableFuture)}
   * recognize producer-owned futures and wait on submission.
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
   * Waits for writer submission only when {@code future} is a producer-owned {@link DurableWriteFuture} (the inline
   * path and foreign subclass futures return immediately), surfacing submission failures to preserve the synchronous
   * contract. The wait is uninterruptible: an admitted write proceeds regardless, so an interrupt must not abandon
   * the wait and let the caller retry and duplicate it; the interrupt is remembered and restored before returning.
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
          interrupted = true;
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          if (cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
          }
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
