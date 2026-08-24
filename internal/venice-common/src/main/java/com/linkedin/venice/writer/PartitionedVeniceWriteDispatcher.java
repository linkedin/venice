package com.linkedin.venice.writer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Coordinates partition-striped writes to an {@link AbstractVeniceWriter}.
 *
 * <p>Callers submit immutable, serialized write commands. Each returned {@link WriteHandle} has a submission future,
 * which completes after the worker calls the core writer, and a durable future, which completes from the PubSub callback.
 * The first worker or PubSub failure is sticky and is surfaced by later submissions, flushes, and shutdown.</p>
 *
 * <p>The admission lock is deliberately local to this coordinator. It provides producer-wide flush fences without
 * adding lock or marker bookkeeping to Online Venice Producer's executor hot path.</p>
 */
public class PartitionedVeniceWriteDispatcher {
  private static final Logger LOGGER = LogManager.getLogger(PartitionedVeniceWriteDispatcher.class);
  private static final long SHUTDOWN_TIMEOUT_SECONDS = 60;

  private final AbstractVeniceWriter<byte[], byte[], byte[]> writer;
  private final PartitionedVeniceWriteExecutor executor;
  private final ReentrantReadWriteLock admissionLock = new ReentrantReadWriteLock(true);
  private final ReentrantLock fenceLock = new ReentrantLock(true);
  private final ReentrantLock generationLock = new ReentrantLock();
  private final Condition generationDrained = generationLock.newCondition();
  private final Set<CompletableFuture<Void>> deferredCompletions = ConcurrentHashMap.newKeySet();
  private final ThreadLocal<CompletableFuture<Void>> activeDeferredCompletion = new ThreadLocal<>();
  private final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
  private final AtomicBoolean legacyRoutingFallbackLogged = new AtomicBoolean();
  private volatile boolean accepting = true;
  private volatile boolean inlineFenceInProgress = false;
  private volatile Generation currentGeneration = new Generation();

  public PartitionedVeniceWriteDispatcher(
      AbstractVeniceWriter<byte[], byte[], byte[]> writer,
      int workerCount,
      int workerQueueCapacity,
      int callbackThreadCount,
      int callbackQueueCapacity,
      String storeName) {
    this.writer = writer;
    this.executor = new PartitionedVeniceWriteExecutor(
        workerCount,
        workerQueueCapacity,
        callbackThreadCount,
        callbackQueueCapacity,
        storeName,
        null,
        "venice-system-producer");
  }

  public WriteHandle put(byte[] key, byte[] value, int valueSchemaId, long logicalTimestamp) {
    return submit(new WriteCommand(Operation.PUT, key, value, valueSchemaId, -1, logicalTimestamp));
  }

  public WriteHandle update(byte[] key, byte[] update, int valueSchemaId, int derivedSchemaId, long logicalTimestamp) {
    return submit(new WriteCommand(Operation.UPDATE, key, update, valueSchemaId, derivedSchemaId, logicalTimestamp));
  }

  public WriteHandle delete(byte[] key, long logicalTimestamp) {
    return submit(new WriteCommand(Operation.DELETE, key, null, -1, -1, logicalTimestamp));
  }

  /**
   * Resolve the submission phase associated with a durable future returned by this dispatcher. Futures returned by an
   * overriding adapter method are treated as already submitted so existing subclass seams remain compatible.
   */
  public CompletableFuture<Void> getSubmissionFuture(CompletableFuture<Void> durableFuture) {
    if (durableFuture instanceof DurableWriteFuture) {
      return ((DurableWriteFuture) durableFuture).submissionFuture;
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Prevent new admission, wait for every previously accepted command to reach the core writer, flush the writer, and
   * recheck asynchronous failures.
   */
  public void flush() {
    lockFence();
    try {
      if (executor.getWorkerCount() == 0) {
        flushInline();
        return;
      }

      FenceSnapshot fence = rotateGeneration(true, false);
      awaitGeneration(fence.generation, 0L);
      awaitStripeMarkers(fence.markers, 0L);
      checkForFailure();
      flushWriter();
      checkForFailure();
    } finally {
      fenceLock.unlock();
    }
  }

  /**
   * Stop admission and drain all worker stripes. The writer remains open so the adapter can close it only after this
   * method returns.
   */
  public void stopAndDrain() {
    boolean restoreInterrupt = false;
    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(SHUTDOWN_TIMEOUT_SECONDS);
    boolean fenceLockAcquired = false;
    while (!fenceLockAcquired) {
      long remainingNanos = deadlineNanos - System.nanoTime();
      if (remainingNanos <= 0) {
        if (restoreInterrupt) {
          Thread.currentThread().interrupt();
        }
        VeniceException exception = new VeniceException("Timed out while waiting to stop Venice write dispatcher");
        recordFailure(exception);
        throw exception;
      }
      try {
        fenceLockAcquired = fenceLock.tryLock(remainingNanos, TimeUnit.NANOSECONDS);
      } catch (InterruptedException exception) {
        restoreInterrupt = true;
      }
    }

    try {
      RuntimeException fenceFailure = null;
      FenceSnapshot fence = null;
      try {
        fence = rotateGeneration(false, true);
        awaitGeneration(fence.generation, deadlineNanos);
        awaitStripeMarkers(fence.markers, deadlineNanos);
      } catch (RuntimeException exception) {
        fenceFailure = exception;
        if (Thread.currentThread().isInterrupted()) {
          Thread.interrupted();
          restoreInterrupt = true;
        }
      }

      executor.shutdownWorkers();
      boolean workersTerminated = false;
      long completionDeadlineNanos = deadlineNanos;
      while (!workersTerminated) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
          break;
        }
        try {
          workersTerminated = executor.awaitWorkerTermination(remainingNanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException exception) {
          restoreInterrupt = true;
        }
      }
      if (!workersTerminated) {
        RejectedExecutionException exception =
            new RejectedExecutionException("Timed out while draining Venice write workers");
        recordFailure(exception);
        if (fenceFailure == null) {
          fenceFailure = new VeniceException("Timed out while draining Venice write workers", exception);
        }
        executor.shutdownWorkersNow();
        completionDeadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(SHUTDOWN_TIMEOUT_SECONDS);
        while (!workersTerminated) {
          long remainingNanos = completionDeadlineNanos - System.nanoTime();
          if (remainingNanos <= 0) {
            break;
          }
          try {
            workersTerminated = executor.awaitWorkerTermination(remainingNanos, TimeUnit.NANOSECONDS);
          } catch (InterruptedException interruptedException) {
            restoreInterrupt = true;
          }
        }
      } else {
        try {
          awaitDeferredCompletions(completionDeadlineNanos);
        } catch (RuntimeException exception) {
          if (fenceFailure == null) {
            fenceFailure = exception;
          }
        }
      }
      if (workersTerminated && fenceFailure != null) {
        try {
          awaitDeferredCompletions(completionDeadlineNanos);
        } catch (RuntimeException exception) {
          fenceFailure.addSuppressed(exception);
        }
      }

      if (restoreInterrupt) {
        Thread.currentThread().interrupt();
      }
      if (fenceFailure != null) {
        if (!workersTerminated) {
          throw fenceFailure;
        }
        // Workers are confirmed drained, so adapters may safely flush and close the writer.
        // Preserve the earlier interruption/fence problem as a sticky failure for that cleanup path to surface.
        recordFailure(fenceFailure);
      }
    } finally {
      fenceLock.unlock();
    }
  }

  /**
   * Shut down callback delivery after the adapter has closed the writer.
   */
  public void shutdownCallbacks() {
    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(SHUTDOWN_TIMEOUT_SECONDS);
    executor.shutdownCallbacks();
    try {
      if (!executor.awaitCallbackTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        RejectedExecutionException exception =
            new RejectedExecutionException("Timed out while draining Venice write callbacks");
        recordFailure(exception);
        executor.shutdownCallbacksNow();
        throw new VeniceException("Timed out while draining Venice write callbacks", exception);
      }
      awaitDeferredCompletions(deadlineNanos);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      executor.shutdownCallbacksNow();
      throw new VeniceException("Interrupted while draining Venice write callbacks", exception);
    }
    checkForFailure();
  }

  /**
   * Throw the sticky first write failure, if any.
   */
  public void checkForFailure() {
    Throwable failure = firstFailure.get();
    if (failure != null) {
      throw new VeniceException("Venice write dispatcher observed a prior write failure", failure);
    }
  }

  Throwable getFirstFailure() {
    return firstFailure.get();
  }

  private WriteHandle submit(WriteCommand command) {
    checkForFailure();
    int partition = 0;
    if (executor.getWorkerCount() > 0) {
      try {
        partition = writer.getPartitionId(command.key);
      } catch (UnsupportedOperationException exception) {
        partition = Arrays.hashCode(command.key);
        if (legacyRoutingFallbackLogged.compareAndSet(false, true)) {
          LOGGER.warn(
              "Writer {} does not expose exact Venice partition routing; falling back to serialized-key striping",
              writer.getClass().getName());
        }
      } catch (RuntimeException exception) {
        VeniceException routingException = new VeniceException(
            "Unable to determine the Venice partition from writer " + writer.getClass().getName(),
            exception);
        command.fail(routingException);
        recordFailure(routingException);
        throw routingException;
      }
    }

    lockAdmissionForSubmission(command);
    try {
      ensureAccepting();
      if (inlineFenceInProgress) {
        throw new VeniceException("A Venice write fence is in progress in inline mode");
      }
      checkForFailure();
      Generation generation = currentGeneration;
      generation.accept();
      command.generation = generation;
    } finally {
      admissionLock.readLock().unlock();
    }
    try {
      executor.submit(partition, () -> execute(command), command::fail);
    } catch (RejectedExecutionException exception) {
      command.fail(exception);
      throw new VeniceException("Venice write command was not accepted", exception);
    }
    return command.handle;
  }

  private void execute(WriteCommand command) {
    PubSubProducerCallback callback =
        (PubSubProduceResult produceResult, Exception exception) -> command.onCompletion(exception);

    Throwable submissionFailure = null;
    try {
      switch (command.operation) {
        case PUT:
          writer.put(command.key, command.value, command.valueSchemaId, command.logicalTimestamp, callback);
          break;
        case UPDATE:
          writer.update(
              command.key,
              command.value,
              command.valueSchemaId,
              command.derivedSchemaId,
              command.logicalTimestamp,
              callback);
          break;
        case DELETE:
          writer.delete(command.key, command.logicalTimestamp, callback);
          break;
        default:
          throw new VeniceException("Unsupported Venice write operation: " + command.operation);
      }
    } catch (Throwable throwable) {
      recordFailure(throwable);
      submissionFailure = throwable;
    } finally {
      command.completeGeneration();
      command.completeSubmissionFuture(submissionFailure);
    }
    if (submissionFailure instanceof Error) {
      throw (Error) submissionFailure;
    }
  }

  private void completeDurableWrite(WriteCommand command, Throwable exception) {
    Runnable completion = executor.isCallbackExecutorEnabled()
        ? () -> scheduleDeferredCompletion(command, exception)
        : () -> completeDurableWriteNow(command, exception);
    try {
      executor.executeCallback(completion, rejection -> {
        recordFailure(rejection);
        completeDurableWriteAfterCallbackRejection(command, rejection);
      });
    } catch (RejectedExecutionException ignored) {
      // The rejection callback records the failure and moves completion off the PubSub thread.
    }
  }

  private void completeDurableWriteAfterCallbackRejection(WriteCommand command, Throwable rejection) {
    scheduleDeferredCompletion(
        command,
        new VeniceException("Venice callback executor rejected durable completion", rejection));
  }

  private void completeDurableWriteNow(WriteCommand command, Throwable exception) {
    if (exception == null) {
      command.handle.durableFuture.complete(null);
    } else {
      command.handle.durableFuture.completeExceptionally(exception);
    }
  }

  private FenceSnapshot rotateGeneration(boolean requireAccepting, boolean stopAdmission) {
    lockAdmissionForFence();
    try {
      if (requireAccepting) {
        ensureAccepting();
      }
      if (stopAdmission) {
        accepting = false;
      }
      Generation fencedGeneration = currentGeneration;
      currentGeneration = new Generation();
      List<CompletableFuture<Void>> markers = new ArrayList<>(executor.getWorkerCount());
      for (int stripe = 0; stripe < executor.getWorkerCount(); stripe++) {
        CompletableFuture<Void> marker = new CompletableFuture<>();
        markers.add(marker);
        try {
          executor.submitControl(stripe, () -> marker.complete(null), marker::completeExceptionally);
        } catch (RejectedExecutionException exception) {
          recordFailure(exception);
          throw new VeniceException("Unable to enqueue Venice write fence marker", exception);
        }
      }
      return new FenceSnapshot(fencedGeneration, markers);
    } finally {
      admissionLock.writeLock().unlock();
    }
  }

  private void awaitGeneration(Generation generation, long deadlineNanos) {
    generationLock.lock();
    try {
      while (generation.pendingSubmissions.get() > 0) {
        checkForFailure();
        try {
          if (deadlineNanos == 0L) {
            generationDrained.await();
          } else {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0 || !generationDrained.await(remainingNanos, TimeUnit.NANOSECONDS)) {
              throw new VeniceException("Timed out while waiting for Venice write fence");
            }
          }
        } catch (InterruptedException exception) {
          Thread.currentThread().interrupt();
          throw new VeniceException("Interrupted while waiting for Venice write fence", exception);
        }
      }
    } finally {
      generationLock.unlock();
    }
  }

  private void completeGeneration(Generation generation) {
    if (generation == null || generation.pendingSubmissions.decrementAndGet() != 0) {
      return;
    }
    generationLock.lock();
    try {
      generationDrained.signalAll();
    } finally {
      generationLock.unlock();
    }
  }

  private void awaitStripeMarkers(List<CompletableFuture<Void>> markers, long deadlineNanos) {
    for (CompletableFuture<Void> marker: markers) {
      while (!marker.isDone()) {
        checkForFailure();
        try {
          long waitNanos = TimeUnit.MILLISECONDS.toNanos(100);
          if (deadlineNanos != 0L) {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
              throw new VeniceException("Timed out while waiting for Venice write fence marker");
            }
            waitNanos = Math.min(waitNanos, remainingNanos);
          }
          marker.get(waitNanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException exception) {
          Thread.currentThread().interrupt();
          throw new VeniceException("Interrupted while waiting for Venice write fence marker", exception);
        } catch (ExecutionException exception) {
          Throwable cause = exception.getCause() == null ? exception : exception.getCause();
          recordFailure(cause);
          throw new VeniceException("Venice write fence marker failed", cause);
        } catch (java.util.concurrent.TimeoutException ignored) {
          // Poll so a failure from another stripe can wake this fence promptly.
        }
      }
      try {
        marker.get();
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        throw new VeniceException("Interrupted while reading Venice write fence marker", exception);
      } catch (ExecutionException exception) {
        Throwable cause = exception.getCause() == null ? exception : exception.getCause();
        recordFailure(cause);
        throw new VeniceException("Venice write fence marker failed", cause);
      }
    }
  }

  private void flushInline() {
    lockAdmissionForFence();
    Generation fencedGeneration;
    try {
      ensureAccepting();
      inlineFenceInProgress = true;
      fencedGeneration = currentGeneration;
      currentGeneration = new Generation();
    } finally {
      admissionLock.writeLock().unlock();
    }

    try {
      awaitGeneration(fencedGeneration, 0L);
      checkForFailure();
      flushWriter();
      checkForFailure();
    } finally {
      admissionLock.writeLock().lock();
      try {
        inlineFenceInProgress = false;
      } finally {
        admissionLock.writeLock().unlock();
      }
    }
  }

  private void flushWriter() {
    try {
      writer.flush();
    } catch (RuntimeException exception) {
      recordFailure(exception);
      throw exception;
    }
  }

  private void scheduleDeferredCompletion(WriteCommand command, Throwable exception) {
    scheduleDeferredTask(() -> completeDurableWriteNow(command, exception));
  }

  private void scheduleDeferredTask(Runnable completion) {
    CompletableFuture<Void> handoff = new CompletableFuture<>();
    deferredCompletions.add(handoff);
    Runnable trackedCompletion = () -> {
      activeDeferredCompletion.set(handoff);
      try {
        completion.run();
        handoff.complete(null);
      } catch (Throwable completionFailure) {
        handoff.completeExceptionally(completionFailure);
      } finally {
        activeDeferredCompletion.remove();
        deferredCompletions.remove(handoff);
      }
    };
    try {
      CompletableFuture.runAsync(trackedCompletion);
    } catch (RejectedExecutionException rejectedExecutionException) {
      trackedCompletion.run();
    }
  }

  private void awaitDeferredCompletions(long deadlineNanos) {
    CompletableFuture<Void> currentCompletion = activeDeferredCompletion.get();
    CompletableFuture<?>[] completions = deferredCompletions.stream()
        .filter(completion -> completion != currentCompletion)
        .toArray(CompletableFuture<?>[]::new);
    if (completions.length == 0) {
      return;
    }
    long remainingNanos = deadlineNanos - System.nanoTime();
    if (remainingNanos <= 0) {
      throw new VeniceException("Timed out while draining Venice durable completions");
    }
    try {
      CompletableFuture.allOf(completions).get(remainingNanos, TimeUnit.NANOSECONDS);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new VeniceException("Interrupted while draining Venice durable completions", exception);
    } catch (ExecutionException | java.util.concurrent.TimeoutException exception) {
      throw new VeniceException("Failed while draining Venice durable completions", exception);
    }
  }

  private void lockAdmissionForSubmission(WriteCommand command) {
    try {
      admissionLock.readLock().lockInterruptibly();
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      command.fail(exception);
      throw new VeniceException("Interrupted before Venice write command admission", exception);
    }
  }

  private void lockAdmissionForFence() {
    try {
      admissionLock.writeLock().lockInterruptibly();
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new VeniceException("Interrupted before Venice write fence");
    }
  }

  private void lockFence() {
    try {
      fenceLock.lockInterruptibly();
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new VeniceException("Interrupted before Venice write fence");
    }
  }

  private void ensureAccepting() {
    if (!accepting) {
      throw new VeniceException("Venice write dispatcher is no longer accepting writes");
    }
  }

  private void recordFailure(Throwable throwable) {
    if (firstFailure.compareAndSet(null, throwable)) {
      generationLock.lock();
      try {
        generationDrained.signalAll();
      } finally {
        generationLock.unlock();
      }
    }
  }

  private static final class Generation {
    private final AtomicInteger pendingSubmissions = new AtomicInteger();

    private void accept() {
      pendingSubmissions.incrementAndGet();
    }
  }

  private static final class FenceSnapshot {
    private final Generation generation;
    private final List<CompletableFuture<Void>> markers;

    private FenceSnapshot(Generation generation, List<CompletableFuture<Void>> markers) {
      this.generation = generation;
      this.markers = markers;
    }
  }

  private enum Operation {
    PUT, UPDATE, DELETE
  }

  /**
   * Two-phase result for an accepted write.
   */
  public static final class WriteHandle {
    private final CompletableFuture<Void> submissionFuture = new CompletableFuture<>();
    private final CompletableFuture<Void> durableFuture = new DurableWriteFuture(submissionFuture);

    public CompletableFuture<Void> getSubmissionFuture() {
      return submissionFuture;
    }

    public CompletableFuture<Void> getDurableFuture() {
      return durableFuture;
    }
  }

  private static final class DurableWriteFuture extends CompletableFuture<Void> {
    private final CompletableFuture<Void> submissionFuture;

    private DurableWriteFuture(CompletableFuture<Void> submissionFuture) {
      this.submissionFuture = submissionFuture;
    }
  }

  private final class WriteCommand {
    private final Operation operation;
    private final byte[] key;
    private final byte[] value;
    private final int valueSchemaId;
    private final int derivedSchemaId;
    private final long logicalTimestamp;
    private final WriteHandle handle = new WriteHandle();
    private final AtomicBoolean callbackInvoked = new AtomicBoolean(false);
    private final AtomicBoolean failed = new AtomicBoolean(false);
    private final AtomicBoolean submissionCompleted = new AtomicBoolean(false);
    private Generation generation;
    private boolean durableCompletionReady;
    private boolean hasDeferredCompletion;
    private Throwable deferredCompletionException;

    private WriteCommand(
        Operation operation,
        byte[] key,
        byte[] value,
        int valueSchemaId,
        int derivedSchemaId,
        long logicalTimestamp) {
      this.operation = operation;
      this.key = Arrays.copyOf(key, key.length);
      this.value = value == null ? null : Arrays.copyOf(value, value.length);
      this.valueSchemaId = valueSchemaId;
      this.derivedSchemaId = derivedSchemaId;
      this.logicalTimestamp = logicalTimestamp;
    }

    private void fail(Throwable throwable) {
      if (!failed.compareAndSet(false, true)) {
        return;
      }
      recordFailure(throwable);
      completeGeneration();
      completeSubmissionFuture(throwable);
    }

    private void completeGeneration() {
      if (submissionCompleted.compareAndSet(false, true)) {
        PartitionedVeniceWriteDispatcher.this.completeGeneration(generation);
      }
    }

    private void completeSubmissionFuture(Throwable throwable) {
      scheduleDeferredTask(() -> {
        if (throwable == null) {
          handle.submissionFuture.complete(null);
        } else {
          handle.submissionFuture.completeExceptionally(throwable);
        }
        markDurableCompletionReady();
        if (throwable != null) {
          onCompletion(throwable);
        }
      });
    }

    private void markDurableCompletionReady() {
      Throwable deferredException = null;
      boolean completeDeferred = false;
      synchronized (this) {
        durableCompletionReady = true;
        if (hasDeferredCompletion) {
          completeDeferred = true;
          deferredException = deferredCompletionException;
          hasDeferredCompletion = false;
        }
      }
      if (completeDeferred) {
        Throwable completionException = deferredException;
        scheduleDeferredCompletion(this, completionException);
      }
    }

    private void onCompletion(Throwable throwable) {
      if (!callbackInvoked.compareAndSet(false, true)) {
        return;
      }
      if (throwable != null) {
        recordFailure(throwable);
      }
      synchronized (this) {
        if (!durableCompletionReady) {
          hasDeferredCompletion = true;
          deferredCompletionException = throwable;
          return;
        }
      }
      completeDurableWrite(this, throwable);
    }
  }
}
