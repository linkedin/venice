package com.linkedin.venice.samza;

import static com.linkedin.venice.samza.VeniceSystemProducerWriteCommand.CALLBACK_IGNORED;
import static com.linkedin.venice.samza.VeniceSystemProducerWriteCommand.CALLBACK_READY;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.utils.VeniceCompletionExecutor;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.PartitionedVeniceWriteExecutor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
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
  private volatile boolean terminalWriterFlushFinished;
  private volatile boolean writerClosed;
  private Future<?> writerCleanupFuture;

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
    this(writer, executor, SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS, VeniceCompletionExecutor::execute);
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
            checkForFailure();
          } catch (Throwable throwable) {
            recordFailure(throwable);
            forceWorkerShutdown = true;
          }
        }
        stopAdmissionDrained = lifecycle.isStopAdmissionDrained();
        lifecycle.releaseStopAdmission();

        if (forceWorkerShutdown) {
          executor.shutdownWorkersNow();
        }
        workersTerminated = executor.shutdownWorkersAndAwait(remainingNanos(deadlineNanos), TimeUnit.NANOSECONDS);
        captureInterrupt(restoreInterrupt);
        if (!workersTerminated) {
          recordFailure(new VeniceException("Timed out while draining Venice SystemProducer workers"));
        } else if (stopAdmissionDrained && !writerClosed) {
          startWriterCleanupIfNeeded();
          awaitWriterCleanup(deadlineNanos, restoreInterrupt);
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
            "Writer {} does not expose partition routing; falling back to stripe 0 to preserve write ordering",
            writer.getClass().getName());
      }
      return 0;
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
    if (!executor.isCallbackExecutorEnabled()) {
      handoffCompletion(() -> command.completeDurable(failure));
      return;
    }
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
    if (!executor.tryExecuteCallback(directCompletion, ignored -> fallbackHandoff.run())) {
      fallbackHandoff.run();
    }
  }

  /** Transfers completion ownership without making user continuations part of producer shutdown. */
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
      VeniceCompletionExecutor.execute(guardedCompletion);
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

  private void startWriterCleanupIfNeeded() {
    if (writerCleanupFuture != null && !writerCleanupFuture.isDone()) {
      return;
    }
    boolean flushRequired = !terminalWriterFlushFinished;
    FutureTask<Void> cleanupTask = new FutureTask<>(() -> {
      cleanupWriter(flushRequired);
      return null;
    });
    writerCleanupFuture = cleanupTask;
    VeniceCompletionExecutor.execute(cleanupTask);
  }

  private void cleanupWriter(boolean flushRequired) {
    Throwable cleanupFailure = null;
    if (flushRequired) {
      try {
        writer.flush();
      } catch (Throwable throwable) {
        recordFailure(throwable);
        cleanupFailure = throwable;
      } finally {
        terminalWriterFlushFinished = true;
      }
    }
    try {
      writer.close();
      writerClosed = true;
    } catch (Throwable throwable) {
      recordFailure(throwable);
      if (cleanupFailure == null) {
        cleanupFailure = throwable;
      }
    }
    if (cleanupFailure != null) {
      throw propagate("Venice SystemProducer writer cleanup failed", cleanupFailure);
    }
  }

  private void awaitWriterCleanup(long deadlineNanos, AtomicBoolean restoreInterrupt) {
    try {
      writerCleanupFuture.get(remainingNanos(deadlineNanos), TimeUnit.NANOSECONDS);
    } catch (InterruptedException exception) {
      restoreInterrupt.set(true);
      throw new VeniceException("Interrupted while cleaning up Venice SystemProducer writer", exception);
    } catch (ExecutionException exception) {
      Throwable cause = exception.getCause() == null ? exception : exception.getCause();
      recordFailure(cause);
    } catch (TimeoutException exception) {
      throw new VeniceException("Timed out while cleaning up Venice SystemProducer writer", exception);
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

  boolean awaitStopAdmission(long timeout, TimeUnit unit) throws InterruptedException {
    return lifecycle.awaitStopAdmission(timeout, unit);
  }

  boolean awaitFence(long timeout, TimeUnit unit) throws InterruptedException {
    return lifecycle.awaitFence(timeout, unit);
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
