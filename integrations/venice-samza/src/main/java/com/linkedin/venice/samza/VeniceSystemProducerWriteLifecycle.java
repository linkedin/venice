package com.linkedin.venice.samza;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.writer.PartitionedVeniceWriteExecutor;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * SystemProducer-specific admission, fencing, failure, and completion-handoff lifecycle.
 */
final class VeniceSystemProducerWriteLifecycle {
  enum StopStatus {
    STARTED, ALREADY_STOPPED, FAILED
  }

  private final ReentrantReadWriteLock admissionLock = new ReentrantReadWriteLock(true);
  private final Lock fenceLock = new java.util.concurrent.locks.ReentrantLock(true);
  private final Executor handoffExecutor;
  private final Object admissions = new Object();
  private final Object handoffs = new Object();
  private final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
  private volatile boolean accepting = true;
  private volatile boolean stopped;
  private boolean stopFenceHeld;
  private boolean stopAdmissionHeld;
  private int pendingAdmissions;
  private int pendingHandoffs;

  VeniceSystemProducerWriteLifecycle(Executor handoffExecutor) {
    this.handoffExecutor = handoffExecutor;
  }

  void runFlushFence(Runnable action) {
    lockInterruptibly(fenceLock, "Interrupted before Venice SystemProducer flush");
    try {
      lockInterruptibly(admissionLock.writeLock(), "Interrupted before Venice SystemProducer flush");
      try {
        ensureAccepting();
        awaitPendingAdmissions();
        action.run();
      } finally {
        admissionLock.writeLock().unlock();
      }
    } finally {
      fenceLock.unlock();
    }
  }

  StopStatus beginStop(long deadlineNanos, AtomicBoolean restoreInterrupt) {
    if (!tryLockUntil(fenceLock, deadlineNanos, restoreInterrupt)) {
      accepting = false;
      recordFailure(new VeniceException("Timed out while waiting for an active Venice SystemProducer flush"));
      return StopStatus.FAILED;
    }
    stopFenceHeld = true;
    if (stopped) {
      stopFenceHeld = false;
      fenceLock.unlock();
      return StopStatus.ALREADY_STOPPED;
    }
    if (!accepting) {
      stopFenceHeld = false;
      fenceLock.unlock();
      checkForFailure();
      throw new VeniceException("Venice SystemProducer stop did not complete");
    }
    if (!tryLockUntil(admissionLock.writeLock(), deadlineNanos, restoreInterrupt)) {
      accepting = false;
      stopFenceHeld = false;
      fenceLock.unlock();
      recordFailure(new VeniceException("Timed out while closing Venice SystemProducer write admission"));
      return StopStatus.FAILED;
    }

    accepting = false;
    stopAdmissionHeld = true;
    try {
      if (!awaitPendingAdmissionsUntil(deadlineNanos, restoreInterrupt)) {
        recordFailure(new VeniceException("Timed out while draining Venice SystemProducer write admissions"));
        return StopStatus.FAILED;
      }
    } catch (Throwable throwable) {
      recordFailure(throwable);
      return StopStatus.FAILED;
    }
    return StopStatus.STARTED;
  }

  void releaseStopAdmission() {
    if (stopAdmissionHeld) {
      stopAdmissionHeld = false;
      admissionLock.writeLock().unlock();
    }
  }

  void markStopped() {
    stopped = true;
  }

  void finishStop() {
    releaseStopAdmission();
    if (stopFenceHeld) {
      stopFenceHeld = false;
      fenceLock.unlock();
    }
  }

  void beginAdmission() {
    lockInterruptibly(admissionLock.readLock(), "Interrupted before Venice write admission");
    try {
      ensureAccepting();
      synchronized (admissions) {
        checkForFailure();
        pendingAdmissions++;
      }
    } finally {
      admissionLock.readLock().unlock();
    }
  }

  void finishAdmission() {
    synchronized (admissions) {
      if (--pendingAdmissions == 0) {
        admissions.notifyAll();
      }
    }
  }

  void scheduleCompletionHandoff(PartitionedVeniceWriteExecutor executor, Runnable completion) {
    AtomicBoolean scheduled = new AtomicBoolean();
    Runnable schedule = () -> {
      if (scheduled.compareAndSet(false, true)) {
        handoff(completion);
      }
    };
    if (!executor.isCallbackExecutorEnabled() || !executor.tryExecuteCallback(schedule, ignored -> schedule.run())) {
      schedule.run();
    }
  }

  boolean awaitHandoffCompletions(long timeout, TimeUnit unit) {
    long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
    boolean interrupted = false;
    try {
      synchronized (handoffs) {
        while (pendingHandoffs > 0) {
          long remaining = remainingNanos(deadlineNanos);
          if (remaining == 0) {
            return false;
          }
          try {
            TimeUnit.NANOSECONDS.timedWait(handoffs, remaining);
          } catch (InterruptedException exception) {
            interrupted = true;
          }
        }
        return true;
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  void checkForFailure() {
    Throwable failure = firstFailure.get();
    if (failure != null) {
      throw new VeniceException("Venice SystemProducer observed a prior write failure", failure);
    }
  }

  void recordFailure(Throwable failure) {
    synchronized (admissions) {
      firstFailure.compareAndSet(null, failure);
      admissions.notifyAll();
    }
  }

  boolean isStopped() {
    return stopped;
  }

  int getPendingAdmissions() {
    synchronized (admissions) {
      return pendingAdmissions;
    }
  }

  private void handoff(Runnable completion) {
    synchronized (handoffs) {
      pendingHandoffs++;
    }
    handoffExecutor.execute(() -> {
      synchronized (handoffs) {
        pendingHandoffs--;
        handoffs.notifyAll();
      }
      try {
        completion.run();
      } catch (Throwable throwable) {
        recordFailure(throwable);
      }
    });
  }

  private void awaitPendingAdmissions() {
    try {
      synchronized (admissions) {
        while (pendingAdmissions > 0) {
          checkForFailure();
          admissions.wait();
        }
        checkForFailure();
      }
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new VeniceException("Interrupted while draining Venice SystemProducer write admissions", exception);
    }
  }

  private boolean awaitPendingAdmissionsUntil(long deadlineNanos, AtomicBoolean restoreInterrupt) {
    synchronized (admissions) {
      while (pendingAdmissions > 0) {
        checkForFailure();
        long remaining = remainingNanos(deadlineNanos);
        if (remaining == 0) {
          return false;
        }
        try {
          TimeUnit.NANOSECONDS.timedWait(admissions, remaining);
        } catch (InterruptedException exception) {
          restoreInterrupt.set(true);
          throw new VeniceException("Interrupted while draining Venice SystemProducer write admissions", exception);
        }
      }
      checkForFailure();
      return true;
    }
  }

  private void ensureAccepting() {
    if (!accepting) {
      throw new VeniceException("Venice SystemProducer is no longer accepting writes");
    }
  }

  private static void lockInterruptibly(Lock lock, String message) {
    try {
      lock.lockInterruptibly();
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new VeniceException(message, exception);
    }
  }

  static boolean tryLockUntil(Lock lock, long deadlineNanos, AtomicBoolean restoreInterrupt) {
    while (remainingNanos(deadlineNanos) > 0) {
      try {
        return lock.tryLock(remainingNanos(deadlineNanos), TimeUnit.NANOSECONDS);
      } catch (InterruptedException exception) {
        restoreInterrupt.set(true);
      }
    }
    return false;
  }

  static long remainingNanos(long deadlineNanos) {
    return Math.max(0, deadlineNanos - System.nanoTime());
  }

}
