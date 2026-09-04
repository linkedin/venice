package com.linkedin.venice.samza;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.concurrent.PartitionStripedExecutor;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Routes already-serialized {@link VeniceSystemProducerWriteCommand}s onto a {@link PartitionStripedExecutor}
 * so that the {@link VeniceSystemProducer} STREAM path can submit writes without the caller waiting for the
 * Venice writer. Records that map to the same Venice partition always land on the same stripe, so a partition
 * blocked by leader rebalance cannot stall a different partition, while per-partition FIFO and the writer's
 * own DIV locks are preserved.
 *
 * <p>Cross-stripe progress is a guarantee of <em>normal dispatch</em>: outside an explicit flush fence, a
 * partition blocked on one stripe never stalls a partition on a different stripe. Partitions that share a
 * stripe are still serialized behind one another (four-stripe isolation, accepted by design). {@link #flush()}
 * is deliberately different: it is a global pre-fence durability boundary, not a per-stripe concern.</p>
 *
 * <p>The dispatcher owns only the striped fan-out, a lossless flush fence, a lean stop, and a single sticky
 * failure. It does not own the writer's lifecycle: {@link #stop()} drains the workers but never closes the
 * writer (the producer does that after the workers have quiesced).</p>
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

  /**
   * VSP-owned handoff pool for the two worker-originated durable completions: a synchronous writer failure,
   * and a callback that fired synchronously inside the writer before submission returned. Completing either on
   * the stripe worker would run a caller continuation (e.g. a retry that calls {@link #flush()} or
   * {@link #stop()}) inline on the worker and self-deadlock, so those are handed here instead. It is sized to
   * the worker count and uses daemon threads that start lazily.
   *
   * <p>Its work queue is an ordinary unbounded queue and is deliberately not backpressured, because it is only
   * ever fed a synchronous successful callback or the first synchronous failure of a command. Sustained failure
   * admission is prevented by the sticky failure (after the first failure, further dispatch fails fast rather
   * than reaching a worker), real Kafka callbacks are asynchronous and complete directly via {@link #onCallback}
   * without touching this pool, and a worker never blocks handing off. The residual accepted risk is a blocked
   * user continuation occupying a pool thread — that is the caller's own code and outside this dispatcher's
   * concern.</p>
   */
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
   * Routes {@code command} to the stripe owning its Venice partition and returns its durable future after
   * bounded admission. Never waits for the writer. A rejected admission (dispatcher stopped or kernel shutdown)
   * fails the command's submission and records a sticky failure.
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
      int partition = writer.getPartitionId(command.getKey());
      try {
        kernel.submit(partition, () -> execute(command));
      } catch (RuntimeException e) {
        recordSticky(e);
        runDurableCompletion(command.finishSubmission(e));
      }
      return command.getDurableFuture();
    } finally {
      admissionLock.readLock().unlock();
    }
  }

  /** Worker body: invoke the writer once, then finish submission (or fail it on a synchronous error). */
  private void execute(VeniceSystemProducerWriteCommand command) {
    try {
      command.submit(writer, (result, exception) -> onCallback(command, exception));
      handOffDurableCompletion(command.finishSubmission(null));
    } catch (RuntimeException e) {
      recordSticky(e);
      handOffDurableCompletion(command.finishSubmission(e));
    } catch (Error e) {
      // Complete submission/durable state with the failure so awaiters see it, then rethrow so the fatal
      // Error keeps its original identity on the worker thread rather than being swallowed. The durable
      // completion is handed off the worker so a caller continuation cannot run inline here.
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
   * Called once per accepted command by its stripe worker to settle any durable completion it still owes.
   *
   * <p>When {@code durableCompletion} is {@code null} nothing durable is owed on the worker (normal
   * asynchronous path — a later callback completes the durable future directly, see {@link #onCallback}). When a
   * durable completion <em>is</em> owed (a synchronous writer failure, or a callback that arrived synchronously
   * before submission returned) it is handed onto the VSP-owned {@link #completionExecutor} rather than run on
   * the worker: completing the durable future on the worker would run any caller continuation (e.g. a retry that
   * calls {@link #flush()} or {@link #stop()}) inline and self-deadlock.</p>
   */
  private void handOffDurableCompletion(Runnable durableCompletion) {
    if (durableCompletion != null) {
      completionExecutor.execute(durableCompletion);
    }
  }

  /**
   * Runs a durable completion directly. Used on the admission paths (durable future not yet returned to the
   * caller) and the asynchronous callback path (completing on a non-worker thread), where there is no stripe
   * worker to self-deadlock. A {@code null} completion is a no-op.
   */
  private static void runDurableCompletion(Runnable durableCompletion) {
    if (durableCompletion != null) {
      durableCompletion.run();
    }
  }

  /**
   * Lossless checkpoint fence with standard {@code producer.flush()} semantics: every write submitted before
   * the fence is durably handed to the writer before {@link AbstractVeniceWriter#flush()} runs; writes that
   * arrive after the fence may admit and execute concurrently and are not part of this checkpoint.
   *
   * <p>The admission write lock is held only long enough to place one marker per stripe <em>behind</em> all
   * pre-fence admissions, then released before awaiting the markers and flushing. Holding it across
   * {@code writer.flush()} would deadlock: a Kafka callback completion can run a retry continuation that
   * calls {@link #dispatch} and blocks on the admission read lock while {@code writer.flush()} waits for that
   * same callback.</p>
   */
  void flush() {
    int stripes = kernel.getStripeCount();
    CountDownLatch fence = new CountDownLatch(stripes);
    admissionLock.writeLock().lock();
    try {
      // Under the write lock, no new admission can interleave, so these markers land strictly behind every
      // already-admitted task on each stripe. Capturing the latch here lets us await it after unlocking.
      for (int stripe = 0; stripe < stripes; stripe++) {
        kernel.executeOnStripe(stripe, fence::countDown);
      }
    } finally {
      admissionLock.writeLock().unlock();
    }
    try {
      // Await + flush OUTSIDE the lock so callback-driven retry continuations can still admit.
      fence.await();
      writer.flush();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new VeniceException("Interrupted while flushing VeniceSystemProducer write dispatcher", e);
    }
    checkForFailure();
  }

  /**
   * Rejects new writes and drains the workers losslessly. Idempotent. Never closes the writer — the producer
   * owns that and only closes once no worker can still use it.
   *
   * <p>{@link PartitionStripedExecutor#shutdown()} wakes any submitter blocked on a full queue so it fails
   * fast, and lets every already-accepted task run to completion. The drain is intentionally unbounded: it
   * waits as long as necessary for the workers to finish rather than force-cancelling queued work, so a wedged
   * writer blocks {@code stop()} by design instead of dropping an accepted write.</p>
   *
   * <p>If interrupted while draining, it keeps draining and reports the interrupt via its return value
   * <em>without</em> re-asserting it, so the caller can perform its own interruptible writer/auxiliary cleanup
   * with the interrupt clear and restore it afterwards.</p>
   *
   * <p>Once the striped workers are fully drained no worker can hand off another durable completion, so the
   * VSP completion pool is shut down but intentionally <em>not</em> awaited: an in-flight durable completion may
   * be running a user continuation that itself called {@code stop()} or {@code flush()}, so awaiting it would
   * block on arbitrary user code and could deadlock. Its daemon threads never block JVM exit, and
   * {@code shutdown()} still lets already-queued completions finish.</p>
   *
   * @return {@code true} if the draining thread observed an interrupt at least once; a no-op idempotent call
   *         returns {@code false}
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
        // Keep draining: dropping queued writes here would be lossy. Remember the interrupt and continue.
        interrupted = true;
      }
    }
    // Workers are fully drained, so no further durable completion can be handed off. Shut the handoff pool down
    // but do not await it: a queued completion may be running a user continuation that called stop()/flush(),
    // and awaiting arbitrary user code could deadlock. Daemon threads let already-queued completions finish.
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
