package com.linkedin.venice.samza;

import static com.linkedin.venice.samza.VeniceSystemProducerWriteCommand.CALLBACK_IGNORED;
import static com.linkedin.venice.samza.VeniceSystemProducerWriteCommand.CALLBACK_READY;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.PartitionedVeniceWriteExecutor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/** STREAM-only routing, execution, callback, flush, and stop coordination for {@link VeniceSystemProducer}. */
final class VeniceSystemProducerWriteDispatcher {
  private static final Logger LOGGER = LogManager.getLogger(VeniceSystemProducerWriteDispatcher.class);
  private static final long SHUTDOWN_TIMEOUT_SECONDS = 60;
  private static final long MARKER_FAILURE_POLL_MILLISECONDS = 100;

  private final AbstractVeniceWriter<byte[], byte[], byte[]> writer;
  private final PartitionedVeniceWriteExecutor executor;
  private final long shutdownTimeoutNanos;
  private final Executor completionHandoffExecutor;
  private final LongConsumer markerAdmissionWait;
  private final VeniceSystemProducerWriteLifecycle lifecycle = new VeniceSystemProducerWriteLifecycle();
  private final AtomicBoolean legacyRoutingWarningLogged = new AtomicBoolean();
  private volatile boolean writerClosed;

  VeniceSystemProducerWriteDispatcher(
      AbstractVeniceWriter<byte[], byte[], byte[]> writer,
      int workerCount,
      int workerQueueCapacity,
      int callbackThreadCount,
      int callbackQueueCapacity,
      String storeName) {
    this(
        writer,
        new PartitionedVeniceWriteExecutor(
            workerCount,
            workerQueueCapacity,
            callbackThreadCount,
            callbackQueueCapacity,
            storeName,
            null,
            "venice-system-producer"));
  }

  VeniceSystemProducerWriteDispatcher(
      AbstractVeniceWriter<byte[], byte[], byte[]> writer,
      PartitionedVeniceWriteExecutor executor) {
    this(writer, executor, SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS, ForkJoinPool.commonPool());
  }

  VeniceSystemProducerWriteDispatcher(
      AbstractVeniceWriter<byte[], byte[], byte[]> writer,
      PartitionedVeniceWriteExecutor executor,
      long shutdownTimeout,
      TimeUnit shutdownTimeoutUnit,
      Executor completionHandoffExecutor) {
    this(writer, executor, shutdownTimeout, shutdownTimeoutUnit, completionHandoffExecutor, LockSupport::parkNanos);
  }

  VeniceSystemProducerWriteDispatcher(
      AbstractVeniceWriter<byte[], byte[], byte[]> writer,
      PartitionedVeniceWriteExecutor executor,
      long shutdownTimeout,
      TimeUnit shutdownTimeoutUnit,
      Executor completionHandoffExecutor,
      LongConsumer markerAdmissionWait) {
    if (shutdownTimeout <= 0) {
      throw new IllegalArgumentException("Shutdown timeout must be greater than zero");
    }
    this.writer = writer;
    this.executor = executor;
    this.shutdownTimeoutNanos = shutdownTimeoutUnit.toNanos(shutdownTimeout);
    this.completionHandoffExecutor = completionHandoffExecutor;
    this.markerAdmissionWait = markerAdmissionWait;
  }

  CompletableFuture<Void> put(byte[] key, byte[] value, int valueSchemaId, long logicalTimestamp) {
    return dispatch(VeniceSystemProducerWriteCommand.put(key, value, valueSchemaId, logicalTimestamp));
  }

  CompletableFuture<Void> update(
      byte[] key,
      byte[] value,
      int valueSchemaId,
      int derivedSchemaId,
      long logicalTimestamp) {
    return dispatch(
        VeniceSystemProducerWriteCommand.update(key, value, valueSchemaId, derivedSchemaId, logicalTimestamp));
  }

  CompletableFuture<Void> delete(byte[] key, long logicalTimestamp) {
    return dispatch(VeniceSystemProducerWriteCommand.delete(key, logicalTimestamp));
  }

  Future<Void> getSubmissionFuture(CompletableFuture<Void> durableFuture) {
    return VeniceSystemProducerWriteCommand.getSubmissionFuture(durableFuture);
  }

  void checkForFailure() {
    lifecycle.checkForFailure();
  }

  void flush() {
    lifecycle.runFlushFence(() -> {
      for (CompletableFuture<Void> marker: enqueueMarkers(0)) {
        await(marker, "Venice SystemProducer flush marker failed", 0);
      }
      flushWriter();
      checkForFailure();
    });
  }

  void stop() {
    AtomicBoolean restoreInterrupt = new AtomicBoolean(Thread.interrupted());
    long deadlineNanos = System.nanoTime() + shutdownTimeoutNanos;
    try {
      boolean workersTerminated;
      boolean fenceCompleted = false;
      boolean forceWorkerShutdown = false;
      boolean stopAdmissionDrained;
      VeniceSystemProducerWriteLifecycle.StopStatus stopStatus = lifecycle.beginStop(deadlineNanos, restoreInterrupt);
      try {
        if (stopStatus == VeniceSystemProducerWriteLifecycle.StopStatus.ALREADY_STOPPED) {
          checkForFailure();
          return;
        }
        forceWorkerShutdown = stopStatus == VeniceSystemProducerWriteLifecycle.StopStatus.FAILED;

        if (!forceWorkerShutdown) {
          try {
            for (CompletableFuture<Void> marker: enqueueMarkers(deadlineNanos)) {
              await(marker, "Venice SystemProducer stop marker failed", deadlineNanos);
            }
            flushWriter();
            checkForFailure();
            fenceCompleted = true;
          } catch (Throwable throwable) {
            recordFailure(throwable);
            forceWorkerShutdown = true;
          }
        }
        stopAdmissionDrained = lifecycle.isStopAdmissionDrained();
        lifecycle.releaseStopAdmission();

        if (forceWorkerShutdown) {
          executor.shutdownWorkersNow();
        } else {
          executor.shutdownWorkers();
        }
        workersTerminated = executor.shutdownWorkersAndAwait(remainingNanos(deadlineNanos), TimeUnit.NANOSECONDS);
        captureInterrupt(restoreInterrupt);
        if (!workersTerminated) {
          recordFailure(new VeniceException("Timed out while draining Venice SystemProducer workers"));
        } else if (stopAdmissionDrained && !writerClosed) {
          if (fenceCompleted) {
            closeWriter();
          } else {
            flushAndCloseWriter();
          }
          captureInterrupt(restoreInterrupt);
        }

        if (workersTerminated && writerClosed) {
          /*
           * Callback admission transfers completion ownership. Accepted callback tasks and exceptional handoffs may
           * finish after stop because CompletableFuture completion can run arbitrary user continuations inline.
           */
          executor.shutdownCallbacks();
          lifecycle.markStopped();
        }
      } finally {
        lifecycle.finishStop();
      }
      checkForFailure();
    } finally {
      captureInterrupt(restoreInterrupt);
      if (restoreInterrupt.get()) {
        Thread.currentThread().interrupt();
      }
    }
  }

  boolean isStopped() {
    return lifecycle.isStopped();
  }

  CompletableFuture<Void> dispatch(VeniceSystemProducerWriteCommand command) {
    lifecycle.beginAdmission();
    try {
      int partition = executor.isWorkersEnabled() ? getPartition(command.getKey()) : 0;
      executor.submit(partition, () -> execute(command), rejection -> reject(command, rejection));
    } catch (RejectedExecutionException exception) {
      throw new VeniceException("Venice write command was not accepted", exception);
    } finally {
      lifecycle.finishAdmission();
    }
    return command.getDurableFuture();
  }

  private int getPartition(byte[] key) {
    try {
      return writer.getPartitionId(key);
    } catch (UnsupportedOperationException unsupportedRouting) {
      if (legacyRoutingWarningLogged.compareAndSet(false, true)) {
        LOGGER.warn(
            "Writer {} does not expose partition routing; falling back to serialized-key hashing for worker striping",
            writer.getClass().getName());
      }
      return Arrays.hashCode(key);
    } catch (Throwable throwable) {
      recordFailure(throwable);
      throw propagate("Unable to determine the Venice partition", throwable);
    }
  }

  private void execute(VeniceSystemProducerWriteCommand command) {
    Throwable submissionFailure = null;
    PubSubProducerCallback callback =
        (PubSubProduceResult produceResult, Exception exception) -> onCompletion(command, exception);
    try {
      command.submit(writer, callback);
    } catch (Throwable throwable) {
      submissionFailure = throwable;
      recordFailure(throwable);
    }
    finishSubmission(command, submissionFailure);
    if (submissionFailure instanceof Error) {
      throw (Error) submissionFailure;
    }
  }

  private void reject(VeniceSystemProducerWriteCommand command, Throwable rejection) {
    recordFailure(rejection);
    finishSubmission(command, rejection);
  }

  private void finishSubmission(VeniceSystemProducerWriteCommand command, Throwable failure) {
    Runnable completion = command.finishSubmission(failure);
    if (completion != null) {
      handoffCompletion(completion);
    }
  }

  private void onCompletion(VeniceSystemProducerWriteCommand command, Throwable failure) {
    int callbackState = command.registerCallback(failure);
    if (callbackState == CALLBACK_IGNORED) {
      return;
    }
    if (failure != null) {
      recordFailure(failure);
    }
    if (callbackState == CALLBACK_READY) {
      executeCallbackCompletion(command, failure);
    }
  }

  private void executeCallbackCompletion(VeniceSystemProducerWriteCommand command, Throwable failure) {
    AtomicBoolean completionClaimed = new AtomicBoolean();
    Runnable directCompletion = () -> {
      if (completionClaimed.compareAndSet(false, true)) {
        command.completeDurable(failure);
      }
    };
    Runnable fallbackHandoff = () -> {
      if (completionClaimed.compareAndSet(false, true)) {
        handoffCompletion(() -> command.completeDurable(failure));
      }
    };
    /*
     * Direct completion normally preserves callbackThreadCount=0 semantics, but a writer may deliver a callback from
     * flush/close while the lifecycle fence is held. Hand off in that case so user continuations cannot reenter a fenced
     * flush or stop on the callback thread.
     */
    if (!executor.isCallbackExecutorEnabled() && lifecycle.isFenceHeld()) {
      fallbackHandoff.run();
      return;
    }
    if (!executor.tryExecuteCallback(directCompletion, ignored -> fallbackHandoff.run())) {
      fallbackHandoff.run();
    }
  }

  /**
   * Transfers exceptional completion ownership without making user continuations part of producer shutdown.
   */
  private void handoffCompletion(Runnable completion) {
    Runnable guardedCompletion = () -> {
      try {
        completion.run();
      } catch (Throwable throwable) {
        recordFailure(throwable);
      }
    };
    try {
      completionHandoffExecutor.execute(guardedCompletion);
    } catch (RejectedExecutionException rejection) {
      if (completionHandoffExecutor == ForkJoinPool.commonPool()) {
        guardedCompletion.run();
        return;
      }
      try {
        ForkJoinPool.commonPool().execute(guardedCompletion);
      } catch (RejectedExecutionException fallbackRejection) {
        guardedCompletion.run();
      }
    }
  }

  private List<CompletableFuture<Void>> enqueueMarkers(long deadlineNanos) {
    List<CompletableFuture<Void>> markers = new ArrayList<>(executor.getWorkerCount());
    for (int stripe = 0; stripe < executor.getWorkerCount(); stripe++) {
      CompletableFuture<Void> marker = new CompletableFuture<>();
      while (true) {
        checkForFailure();
        if (Thread.currentThread().isInterrupted()) {
          throw new VeniceException("Interrupted while enqueuing Venice SystemProducer flush marker");
        }
        if (deadlineNanos > 0 && remainingNanos(deadlineNanos) == 0) {
          throw new VeniceException("Timed out while enqueuing Venice SystemProducer flush marker");
        }
        if (executor.trySubmit(stripe, () -> marker.complete(null), marker::completeExceptionally)) {
          markers.add(marker);
          break;
        }
        long waitNanos = TimeUnit.MILLISECONDS.toNanos(MARKER_FAILURE_POLL_MILLISECONDS);
        markerAdmissionWait.accept(deadlineNanos > 0 ? Math.min(waitNanos, remainingNanos(deadlineNanos)) : waitNanos);
      }
    }
    return markers;
  }

  private void flushAndCloseWriter() {
    boolean restoreInterrupt = Thread.interrupted();
    try {
      flushWriter();
    } catch (Throwable ignored) {
      // The sticky failure is rethrown after physical cleanup.
    } finally {
      restoreInterrupt |= Thread.interrupted();
    }
    closeWriter();
    restoreInterrupt |= Thread.interrupted();
    if (restoreInterrupt) {
      Thread.currentThread().interrupt();
    }
  }

  private void closeWriter() {
    try {
      writer.close();
      writerClosed = true;
    } catch (Throwable throwable) {
      recordFailure(throwable);
    }
  }

  private void flushWriter() {
    try {
      writer.flush();
    } catch (Throwable throwable) {
      recordFailure(throwable);
      throw propagate("Venice SystemProducer writer flush failed", throwable);
    }
  }

  private void await(CompletableFuture<Void> future, String message, long deadlineNanos) {
    while (true) {
      try {
        future.get(MARKER_FAILURE_POLL_MILLISECONDS, TimeUnit.MILLISECONDS);
        return;
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        throw new VeniceException(message, exception);
      } catch (ExecutionException exception) {
        Throwable cause = exception.getCause() == null ? exception : exception.getCause();
        throw new VeniceException(message, cause);
      } catch (TimeoutException exception) {
        checkForFailure();
        if (deadlineNanos > 0 && remainingNanos(deadlineNanos) == 0) {
          throw new VeniceException(message + " before shutdown timeout", exception);
        }
      }
    }
  }

  private void recordFailure(Throwable failure) {
    lifecycle.recordFailure(failure);
  }

  int getPendingAdmissions() {
    return lifecycle.getPendingAdmissions();
  }

  private static long remainingNanos(long deadlineNanos) {
    return VeniceSystemProducerWriteLifecycle.remainingNanos(deadlineNanos);
  }

  private static void captureInterrupt(AtomicBoolean restoreInterrupt) {
    if (Thread.interrupted()) {
      restoreInterrupt.set(true);
    }
  }

  private static RuntimeException propagate(String message, Throwable throwable) {
    if (throwable instanceof Error) {
      throw (Error) throwable;
    }
    return throwable instanceof RuntimeException
        ? (RuntimeException) throwable
        : new VeniceException(message, throwable);
  }
}
