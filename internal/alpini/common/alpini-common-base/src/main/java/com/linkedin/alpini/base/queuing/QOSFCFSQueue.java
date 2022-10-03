package com.linkedin.alpini.base.queuing;

import java.util.concurrent.ConcurrentLinkedQueue;
import javax.annotation.Nonnull;


/**
 * Simple wrapper over a ConcurrentLinkedQueue to expose FCFS Queue operations.
 *
 * @author aauradka
 *
 * @param <T>
 */
public class QOSFCFSQueue<T extends QOSBasedRequestRunnable> implements SimpleQueue<T> {
  private final ConcurrentLinkedQueue<T> _queue = new ConcurrentLinkedQueue<>();

  @Override
  public boolean add(@Nonnull T e) {
    return _queue.add(e);
  }

  @Override
  public T poll() {
    return _queue.poll();
  }

  @Override
  public int size() {
    return _queue.size();
  }

  @Override
  public boolean isEmpty() {
    return _queue.isEmpty();
  }
}
