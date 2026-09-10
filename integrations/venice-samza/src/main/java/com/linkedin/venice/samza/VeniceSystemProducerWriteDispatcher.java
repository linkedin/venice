package com.linkedin.venice.samza;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.concurrent.PartitionStripedExecutor;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Routes already-serialized {@link VeniceSystemProducerWriteCommand}s onto a {@link PartitionStripedExecutor} so the
 * {@link VeniceSystemProducer} STREAM path can submit writes without waiting for the writer. Records for a Venice
 * partition always land on the same stripe (per-partition FIFO, writer DIV locks preserved). The dispatcher owns the
 * fan-out, {@link #flush()} fence, {@link #stop()}, and one sticky failure; it never closes the writer.
 */
class VeniceSystemProducerWriteDispatcher {
  private static final Logger LOGGER = LogManager.getLogger(VeniceSystemProducerWriteDispatcher.class);

  static final String WORKER_COUNT_CONFIG = ConfigKeys.VENICE_SYSTEM_PRODUCER_WORKER_COUNT;
  static final String WORKER_QUEUE_CAPACITY_CONFIG = ConfigKeys.VENICE_SYSTEM_PRODUCER_WORKER_QUEUE_CAPACITY;
  static final int DEFAULT_WORKER_COUNT = 4;
  static final int DEFAULT_WORKER_QUEUE_CAPACITY = 100_000;

  private static final long SHUTDOWN_AWAIT_SECONDS = 60;

  private final PartitionStripedExecutor kernel;
  private final AbstractVeniceWriter<byte[], byte[], byte[]> writer;
  private final String storeName;

  // VSP-owned pool for worker-originated durable completions: completing on the stripe worker could run a caller
  // continuation (a retry calling flush()/stop()) inline and self-deadlock, so those are handed off here. Its queue
  // is unbounded because only a synchronous callback or a command's first synchronous failure ever reaches it.
  private final ExecutorService completionExecutor;

  /** Read lock guards a single admission; the write lock is the flush fence that excludes new admissions. */
  private final ReentrantReadWriteLock admissionLock = new ReentrantReadWriteLock();
  private final AtomicBoolean accepting = new AtomicBoolean(true);
  private final AtomicReference<Throwable> stickyFailure = new AtomicReference<>();
  private final Object stopLock = new Object();
  private boolean stopped;

  VeniceSystemProducerWriteDispatcher(
      AbstractVeniceWriter<byte[], byte[], byte[]> writer,
      int workerCount,
      int queueCapacity,
      String storeName) {
    this.writer = writer;
    this.storeName = storeName;
    this.kernel = new PartitionStripedExecutor(workerCount, queueCapacity, "venice-samza-writer-" + storeName);
    this.completionExecutor = Executors
        .newFixedThreadPool(workerCount, new DaemonThreadFactory("venice-samza-writer-completion-" + storeName));
  }

  /**
   * Routes {@code command} to the stripe for its Venice partition and returns the durable future without waiting for
   * the writer. A stopped-dispatcher rejection fails only this submission and is not sticky, so a normal stop never
   * poisons a later {@link #flush()}; any failure while still accepting is recorded sticky.
   */
  VeniceSystemProducerWriteCommand.DurableWriteFuture dispatch(VeniceSystemProducerWriteCommand command) {
    checkForFailure();
    admissionLock.readLock().lock();
    try {
      checkForFailure();
      if (!accepting.get()) {
        runDurableCompletion(
            command.finishSubmission(new VeniceException("VeniceSystemProducer write dispatcher is stopped")));
        return command.getDurableFuture();
      }
      try {
        int partition = writer.getPartitionId(command.getKey());
        try {
          kernel.submit(partition, () -> execute(command));
        } catch (RejectedExecutionException rejection) {
          if (accepting.get()) {
            recordSticky(rejection);
            runDurableCompletion(command.finishSubmission(rejection));
          } else {
            runDurableCompletion(
                command.finishSubmission(
                    new VeniceException("VeniceSystemProducer write dispatcher is stopped", rejection)));
          }
        }
      } catch (RuntimeException e) {
        recordSticky(e);
        runDurableCompletion(command.finishSubmission(e));
      } catch (Error e) {
        recordSticky(e);
        runDurableCompletion(command.finishSubmission(e));
        throw e;
      }
      return command.getDurableFuture();
    } finally {
      admissionLock.readLock().unlock();
    }
  }

  private void execute(VeniceSystemProducerWriteCommand command) {
    try {
      command.submit(writer, new ForwardingProducerCallback(command));
      handOffDurableCompletion(command.finishSubmission(null));
    } catch (RuntimeException e) {
      recordSticky(e);
      handOffDurableCompletion(command.finishSubmission(e));
    } catch (Error e) {
      recordSticky(e);
      handOffDurableCompletion(command.finishSubmission(e));
      throw e;
    }
  }

  private void onCallback(VeniceSystemProducerWriteCommand command, Exception exception) {
    if (exception != null) {
      recordSticky(exception);
    }
    runDurableCompletion(command.registerCallback(exception));
  }

  /**
   * Stores the internal callback the writer chains via {@link PubSubProducerCallback#setInternalCallback}, invokes it
   * first, then forwards result and exception to {@link #onCallback}. A bare lambda would drop that internal callback.
   */
  private final class ForwardingProducerCallback implements PubSubProducerCallback {
    private final VeniceSystemProducerWriteCommand command;
    private PubSubProducerCallback internalCallback;

    ForwardingProducerCallback(VeniceSystemProducerWriteCommand command) {
      this.command = command;
    }

    @Override
    public void onCompletion(PubSubProduceResult produceResult, Exception exception) {
      if (internalCallback != null) {
        internalCallback.onCompletion(produceResult, exception);
      }
      onCallback(command, exception);
    }

    @Override
    public void setInternalCallback(PubSubProducerCallback internalCallback) {
      this.internalCallback = internalCallback;
    }
  }

  private void handOffDurableCompletion(Runnable durableCompletion) {
    if (durableCompletion != null) {
      completionExecutor.execute(durableCompletion);
    }
  }

  // Runs a durable completion inline; safe only where no stripe worker is present to self-deadlock.
  private static void runDurableCompletion(Runnable durableCompletion) {
    if (durableCompletion != null) {
      durableCompletion.run();
    }
  }

  /**
   * Pre-fence checkpoint with standard {@code producer.flush()} semantics: writes admitted before the fence reach the
   * writer before {@link AbstractVeniceWriter#flush()} runs. The admission write lock is released before awaiting the
   * fence and flushing, so a callback-driven retry that re-admits cannot deadlock the flush.
   */
  void flush() {
    int stripes = kernel.getStripeCount();
    CountDownLatch fence = new CountDownLatch(stripes);
    admissionLock.writeLock().lock();
    try {
      for (int stripe = 0; stripe < stripes; stripe++) {
        kernel.executeOnStripe(stripe, fence::countDown);
      }
    } finally {
      admissionLock.writeLock().unlock();
    }
    try {
      fence.await();
      writer.flush();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new VeniceException("Interrupted while flushing VeniceSystemProducer write dispatcher", e);
    }
    checkForFailure();
  }

  /**
   * Rejects new writes and drains the workers losslessly before returning; idempotent and never closes the writer.
   * The drain is unbounded, so a wedged writer blocks stop rather than dropping accepted work. An interrupt seen while
   * draining is reported via the return value without being re-asserted, leaving interrupt ownership to the caller.
   *
   * @return {@code true} if the draining thread observed an interrupt; {@code false} for a no-op idempotent call
   */
  boolean stop() {
    synchronized (stopLock) {
      if (stopped) {
        return false;
      }
      stopped = true;
    }
    accepting.set(false);
    kernel.shutdown();
    boolean interrupted = false;
    boolean terminated = false;
    while (!terminated) {
      try {
        terminated = kernel.awaitTermination(SHUTDOWN_AWAIT_SECONDS, TimeUnit.SECONDS);
        if (!terminated) {
          LOGGER.warn("Still draining VeniceSystemProducer write workers for store {}", storeName);
        }
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    completionExecutor.shutdown();
    return interrupted;
  }

  private void recordSticky(Throwable failure) {
    stickyFailure.compareAndSet(null, failure);
  }

  private void checkForFailure() {
    Throwable failure = stickyFailure.get();
    if (failure != null) {
      throw new VeniceException("VeniceSystemProducer asynchronous write previously failed", failure);
    }
  }
}
