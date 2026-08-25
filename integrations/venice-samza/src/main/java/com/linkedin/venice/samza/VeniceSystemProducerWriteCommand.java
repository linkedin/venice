package com.linkedin.venice.samza;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;


/** Serialized write command with distinct submission and broker-durability completion phases. */
final class VeniceSystemProducerWriteCommand {
  static final int CALLBACK_IGNORED = -1;
  static final int CALLBACK_DEFERRED = 0;
  static final int CALLBACK_READY = 1;

  private enum Operation {
    PUT, UPDATE, DELETE
  }

  private final Operation operation;
  private final byte[] key;
  private final byte[] value;
  private final int valueSchemaId;
  private final int derivedSchemaId;
  private final long logicalTimestamp;
  private final CompletableFuture<Void> submissionFuture = new CompletableFuture<>();
  private final CompletableFuture<Void> durableFuture = new DurableWriteFuture(submissionFuture);
  private boolean submissionComplete;
  private boolean callbackInvoked;
  private boolean hasDeferredCompletion;
  private Throwable deferredFailure;

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

  void submit(AbstractVeniceWriter<byte[], byte[], byte[]> writer, PubSubProducerCallback callback) {
    switch (operation) {
      case PUT:
        writer.put(key, value, valueSchemaId, logicalTimestamp, callback);
        break;
      case UPDATE:
        writer.update(key, value, valueSchemaId, derivedSchemaId, logicalTimestamp, callback);
        break;
      case DELETE:
        writer.delete(key, logicalTimestamp, callback);
        break;
      default:
        throw new VeniceException("Unsupported Venice write operation: " + operation);
    }
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
