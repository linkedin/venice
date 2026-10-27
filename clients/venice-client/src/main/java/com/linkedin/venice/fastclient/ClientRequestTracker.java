package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.exceptions.VeniceClientException;
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
 * Tracks requests and transport attempts until shutdown, including losing retries and dual reads.
 */
public final class ClientRequestTracker {
  private static final Logger LOGGER = LogManager.getLogger(ClientRequestTracker.class);
  private final Set<Ticket> pending = new HashSet<>();
  private final CompletableFuture<Void> closed = new CompletableFuture<>();
  private final long closeTimeoutMs;
  private boolean closing;
  private volatile boolean stopped;
  private boolean interrupted;

  public ClientRequestTracker(long closeTimeoutMs) {
    this.closeTimeoutMs = closeTimeoutMs;
  }

  synchronized Ticket acquire(boolean applicationRequest, Consumer<VeniceClientException> onShutdown) {
    if (stopped || (applicationRequest && closing)) {
      throw new VeniceClientException("Fast Client is closing; new requests are not accepted");
    }
    Ticket ticket = new Ticket(onShutdown);
    pending.add(ticket);
    return ticket;
  }

  boolean isStopped() {
    return stopped;
  }

  Runnable resourceTask(Runnable task) {
    return () -> {
      Ticket ticket;
      try {
        ticket = acquire(false, null);
      } catch (VeniceClientException ignored) {
        return;
      }
      try {
        task.run();
      } finally {
        ticket.close();
      }
    };
  }

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

  final class Ticket implements AutoCloseable {
    private final Consumer<VeniceClientException> onShutdown;
    private final AtomicInteger references = new AtomicInteger(1);

    private Ticket(Consumer<VeniceClientException> onShutdown) {
      this.onShutdown = onShutdown;
    }

    void retain() {
      references.incrementAndGet();
    }

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
