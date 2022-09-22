package com.linkedin.alpini.base.registry;

import com.linkedin.alpini.base.misc.Preconditions;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A simplified implementation of a {@link ShutdownableResource}.
 *
 * @author Antony T Curtis &lt;acurtis@linkedin.com&gt;
 */
public abstract class AbstractShutdownableResource<R> implements ShutdownableResource {
  private final String _string;
  private final Thread _shutdown;
  private final CountDownLatch _shutdownInitiated;
  private boolean _shutdownStarted;
  private boolean _shutdownTerminated;

  protected AbstractShutdownableResource(R object) {
    _string = object.toString();
    _shutdown = new Thread(newShutdownRunnable(object), "shutdown-" + _string);
    _shutdownInitiated = new CountDownLatch(1);
  }

  private Logger getLogger() {
    return LogManager.getLogger(getClass());
  }

  private Runnable newShutdownRunnable(R object) {
    final Runnable runnable = constructShutdown(object);
    if (runnable == null) {
      throw new NullPointerException("constructShutdown() must not return null");
    }
    return () -> {
      try {
        runnable.run();
      } finally {
        synchronized (AbstractShutdownableResource.this) {
          if (_shutdownInitiated.getCount() != 0) {
            getLogger().warn("shutdownInitiated not signalled");
            _shutdownInitiated.countDown();
          }

          _shutdownTerminated = true;
        }
      }
    };
  }

  /**
   * Called by the Runnable to indicate that shutdown has started.
   */
  protected final void signalShutdownInitiated() {
    assert Thread.currentThread() == _shutdown;
    _shutdownInitiated.countDown();
  }

  /**
   * The constructed runnable must call {@link #signalShutdownInitiated()} method to indicate that shutdown has
   * started.
   *
   * @param object Object to be shutdown
   * @return Runnable instance
   */
  protected abstract Runnable constructShutdown(R object);

  /**
   * Returns <tt>true</tt> if this resource has been shut down.
   *
   * @return <tt>true</tt> if this resource has been shut down
   */
  @Override
  public final synchronized boolean isShutdown() {
    return _shutdownStarted;
  }

  /**
   * Returns <tt>true</tt> if the resource has completed shutting down.
   * Note that <tt>isTerminated</tt> is never <tt>true</tt> unless
   * <tt>shutdown</tt> was called first.
   *
   * @return <tt>true</tt> if the resource has completed shutting down.
   */
  @Override
  public final synchronized boolean isTerminated() {
    return _shutdownTerminated;
  }

  /**
   * Starts the shutdown process.
   * The Runnable returned by constructShutdown() must call {@link #signalShutdownInitiated()}.
   */
  @Override
  public final void shutdown() {
    synchronized (this) {
      _shutdown.start();
      _shutdownStarted = true;
    }
    try {
      _shutdownInitiated.await();
    } catch (InterruptedException e) {
      getLogger().warn("interrupted", e);
    }
  }

  /**
   * Waits for shutdown to complete
   *
   * @throws InterruptedException  when the wait is interrupted
   * @throws IllegalStateException when the method is invoked when the shutdown has yet to start
   */
  @Override
  public final void waitForShutdown() throws InterruptedException, IllegalStateException {
    synchronized (this) {
      Preconditions.checkState(_shutdownStarted);
    }
    _shutdown.join();
  }

  /**
   * Waits for shutdown to complete with a timeout
   *
   * @param timeoutInMs number of milliseconds to wait before throwing TimeoutException
   * @throws InterruptedException                     when the wait is interrupted
   * @throws IllegalStateException                    when the method is invoked when the shutdown has yet to start
   * @throws java.util.concurrent.TimeoutException    when the operation times out
   *
   */
  @Override
  public final void waitForShutdown(long timeoutInMs)
      throws InterruptedException, IllegalStateException, TimeoutException {
    synchronized (this) {
      Preconditions.checkState(_shutdownStarted);
    }
    _shutdown.join(timeoutInMs);
    if (_shutdown.isAlive()) {
      throw new TimeoutException();
    }
  }

  @Override
  public final String toString() {
    return _string;
  }
}
