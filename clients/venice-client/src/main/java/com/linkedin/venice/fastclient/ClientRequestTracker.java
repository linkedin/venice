package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Keeps track of in-flight work for one Fast Client so that {@link #close} can let it finish before
 * releasing the client's resources.
 *
 * <p>Each application request, transport attempt (including losing retries and dual reads), and
 * response-processing task holds a {@link Ticket} while it runs. On close, the tracker:
 * <ol>
 *   <li>rejects new application requests,</li>
 *   <li>waits up to {@code closeTimeoutMs} for outstanding tickets to be released,</li>
 *   <li>fails whatever is still outstanding with a {@link VeniceClientException}, and</li>
 *   <li>runs the cleanup that closes the client's resources.</li>
 * </ol>
 */
public final class ClientRequestTracker {
  private static final Logger LOGGER = LogManager.getLogger(ClientRequestTracker.class);
  private static final RedundantExceptionFilter REDUNDANT_EXCEPTION_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();
  /** Tickets not yet released. Guarded by {@code this}. */
  private final Set<Ticket> pending = new HashSet<>();
  /** Completes once cleanup has run; every {@link #close} caller waits on it. */
  private final CompletableFuture<Void> closed = new CompletableFuture<>();
  private final long closeTimeoutMs;
  /** Close has started: new application requests are rejected, but in-flight work may still start internal tasks. */
  private boolean closing;
  /** The drain is over: nothing new is accepted and late results are dropped. */
  private volatile boolean stopped;
  /** A {@link #close} caller was interrupted, so stop waiting and fail what is left. */
  private boolean interrupted;

  /**
   * @param closeTimeoutMs how long {@link #close} waits for in-flight work before failing it; {@code 0} means no wait
   */
  public ClientRequestTracker(long closeTimeoutMs) {
    this.closeTimeoutMs = closeTimeoutMs;
  }

  /**
   * Registers new work. The caller must {@link Ticket#close} the returned ticket when the work is done.
   *
   * @param applicationRequest {@code true} for a request made by the application, which is rejected as soon as
   *                           close starts; {@code false} for internal work, which is accepted until the drain ends
   * @param onShutdown called with the shutdown error if the work is still running when the drain ends; may be null
   * @throws VeniceClientException if the client is closing and this work is no longer accepted
   */
  synchronized Ticket acquire(boolean applicationRequest, Consumer<VeniceClientException> onShutdown) {
    if (stopped || (applicationRequest && closing)) {
      throw new VeniceClientException("Fast Client is closing; new requests are not accepted");
    }
    Ticket ticket = new Ticket(onShutdown);
    pending.add(ticket);
    return ticket;
  }

  /** Returns {@code true} once the drain is over; callers use this to drop late results. */
  boolean isStopped() {
    return stopped;
  }

  /**
   * Wraps a response-processing or retry task so close waits for it while it runs. If the drain is already over,
   * the task is skipped because the resources it needs may be closed.
   */
  Runnable resourceTask(Runnable task) {
    return () -> {
      Ticket ticket;
      try {
        ticket = acquire(false, null);
      } catch (VeniceClientException e) {
        String logMessage = "Skipping a background task because the Fast Client is closed: " + e.getMessage();
        if (!REDUNDANT_EXCEPTION_FILTER.isRedundantException(logMessage)) {
          LOGGER.info(logMessage);
        }
        return;
      }
      try {
        task.run();
      } finally {
        ticket.close();
      }
    };
  }

  /**
   * Starts shutdown on a separate thread and waits until {@code cleanup} has run. Only the first call starts shutdown
   * and runs {@code cleanup}; later calls just wait for it. Interrupting a caller ends the drain early (in-flight
   * work is failed right away), but the caller still waits for {@code cleanup}; the interrupt flag is restored
   * before returning.
   *
   * @throws VeniceClientException if {@code cleanup} fails
   */
  void close(Runnable cleanup) {
    synchronized (this) {
      if (!closing) {
        closing = true;
        long start = System.nanoTime();
        Thread closer = new Thread(() -> drainAndClose(start, cleanup), "venice-fast-client-close");
        closer.setDaemon(true);
        closer.start();
      }
    }
    boolean restoreInterrupt = false;
    try {
      while (true) {
        try {
          closed.get();
          return;
        } catch (InterruptedException e) {
          LOGGER.warn("Interrupted while waiting for the Fast Client to close; in-flight requests will be failed now");
          restoreInterrupt = true;
          synchronized (this) {
            interrupted = true;
            notifyAll();
          }
        } catch (ExecutionException e) {
          throw new VeniceClientException("Failed to close Fast Client", e.getCause());
        }
      }
    } finally {
      if (restoreInterrupt) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /** Runs on the close thread: waits for in-flight work, fails what is left, then runs {@code cleanup}. */
  private void drainAndClose(long start, Runnable cleanup) {
    try {
      ArrayList<Ticket> remaining;
      synchronized (this) {
        long budget = TimeUnit.MILLISECONDS.toNanos(closeTimeoutMs);
        while (!pending.isEmpty() && !interrupted) {
          long waitNs = budget - (System.nanoTime() - start);
          if (waitNs <= 0) {
            break;
          }
          TimeUnit.NANOSECONDS.timedWait(this, waitNs);
        }
        stopped = true;
        remaining = new ArrayList<>(pending);
        pending.clear();
      }
      VeniceClientException failure = new VeniceClientException(
          interrupted
              ? "Fast Client shutdown interrupted while draining requests"
              : "Fast Client shutdown timeout after " + closeTimeoutMs + " ms");
      for (Ticket ticket: remaining) {
        if (ticket.onShutdown != null) {
          try {
            ticket.onShutdown.accept(failure);
          } catch (Throwable e) {
            LOGGER.warn("Request completion callback failed during client shutdown", e);
          }
        }
      }
      cleanup.run();
      closed.complete(null);
    } catch (Throwable e) {
      LOGGER.error("Failed to close Fast Client", e);
      closed.completeExceptionally(e);
    }
  }

  /**
   * A handle for one unit of in-flight work. It starts with one reference; {@link #retain} adds one, and the work is
   * considered finished when every reference has been released with {@link #close}.
   */
  final class Ticket implements AutoCloseable {
    private final Consumer<VeniceClientException> onShutdown;
    private final AtomicInteger references = new AtomicInteger(1);

    private Ticket(Consumer<VeniceClientException> onShutdown) {
      this.onShutdown = onShutdown;
    }

    /** Adds a reference, for example so the ticket stays open until an async response arrives. */
    void retain() {
      references.incrementAndGet();
    }

    /** Releases one reference. Releasing the last one removes the ticket and wakes up a waiting close. */
    @Override
    public void close() {
      if (references.decrementAndGet() == 0) {
        synchronized (ClientRequestTracker.this) {
          pending.remove(this);
          ClientRequestTracker.this.notifyAll();
        }
      }
    }
  }
}
