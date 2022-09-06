package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.common.Measurable;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is a generic implementation of a memory bound blocking queue.
 *
 * This blocking queue is bounded by the memory usage of each {@link Measurable} object buffered inside.
 * To guarantee some kind of fairness, you need to choose suitable {@link #notifyDeltaInByte}
 * according to the max size of messages, which could be buffered.
 *
 * The reason behind this design:
 * Considering some thread could put various sizes of messages into the shared queue, {@link MemoryBoundBlockingQueue}
 * won't notify the waiting thread (the 'put' thread)
 * right away when some message gets processed until the freed memory hit the follow config: {@link #notifyDeltaInByte}.
 * The reason behind this design:
 * When the buffered queue is full, and the processing thread keeps processing small message, the bigger message won't
 * have chance to get queued into the buffer since the memory freed by the processed small message is not enough to
 * fit the bigger message.
 *
 * With this delta config, {@link MemoryBoundBlockingQueue} will guarantee some kind of fairness
 * among various sizes of messages when buffered queue is full.
 *
 * When tuning this config, we need to consider the following tradeoffs:
 * 1. {@link #notifyDeltaInByte} must be smaller than {@link #memoryCapacityInByte};
 * 2. If the delta is too big, it will waste some buffer space since it won't notify the waiting threads even there
 * are some memory available (less than the delta);
 * 3. If the delta is too small, the big message may not be able to get chance to be buffered when the queue is full;
 *
 * @param <T>
 */
public class MemoryBoundBlockingQueue<T extends Measurable> implements BlockingQueue<T> {
  private static final Logger LOGGER = LogManager.getLogger(MemoryBoundBlockingQueue.class);
  /**
   * Considering the node implementation: {@link java.util.LinkedList.Node}, the overhead
   * is three references, which could be about 24 bytes, and the 'Node' object type itself could take 24 bytes.
   * We can adjust this value later if necessary.
   */
  public static final int LINKED_QUEUE_NODE_OVERHEAD_IN_BYTE = 48;
  private final Queue<T> queue;
  private final long memoryCapacityInByte;
  private final long notifyDeltaInByte;
  private final AtomicLong remainingMemoryCapacityInByte;
  private final Lock memoryLock = new ReentrantLock();
  private final Condition hasEnoughMemory = memoryLock.newCondition();
  private final Condition notEmpty = memoryLock.newCondition();
  // Accumulated free memory since last notification
  private long currentFreedMemoryInBytes = 0;

  public MemoryBoundBlockingQueue(long memoryCapacityInByte, long notifyDeltaInByte) {
    if (notifyDeltaInByte > memoryCapacityInByte) {
      throw new IllegalArgumentException(
          "Param notifyDeltaInByte: " + notifyDeltaInByte + " should not be bigger than param memoryCapacityInByte: "
              + memoryCapacityInByte);
    }
    /**
     * There is no need to use any blocking queue here since it is using a lock for both
     * access control and memory throttling.
     */
    this.queue = new LinkedList<>();
    this.memoryCapacityInByte = memoryCapacityInByte;
    this.notifyDeltaInByte = notifyDeltaInByte;
    this.remainingMemoryCapacityInByte = new AtomicLong(this.memoryCapacityInByte);
  }

  public long getMemoryUsage() {
    return memoryCapacityInByte - remainingMemoryCapacityInByte();
  }

  public long remainingMemoryCapacityInByte() {
    return remainingMemoryCapacityInByte.get();
  }

  private int getRecordSize(T record) {
    return record.getSize() + LINKED_QUEUE_NODE_OVERHEAD_IN_BYTE;
  }

  @Override
  public void put(T record) throws InterruptedException {
    int recordSize = getRecordSize(record);
    if (recordSize > notifyDeltaInByte) {
      LOGGER.warn(
          "Record size of record: " + record + " is " + recordSize + ", which exceeds notifyDeltaInByte: "
              + notifyDeltaInByte + ", and it could potentially be blocked when the buffer is full.");
    }
    memoryLock.lock();
    try {
      while (remainingMemoryCapacityInByte() < recordSize) {
        hasEnoughMemory.await();
      }
      queue.add(record);
      remainingMemoryCapacityInByte.getAndAdd(-recordSize);
      notEmpty.signal();
    } finally {
      memoryLock.unlock();
    }
  }

  @Override
  public T take() throws InterruptedException {
    T record = null;

    this.memoryLock.lock();
    try {
      while ((record = this.queue.poll()) == null) {
        notEmpty.await();
      }
      int recordSize = getRecordSize(record);
      currentFreedMemoryInBytes += recordSize;
      /**
       * It won't notify the blocked {@link #put(Measurable)}  thread until the freed memory exceeds
       * pre-defined {@link #notifyDeltaInByte}.
       */
      if (currentFreedMemoryInBytes >= notifyDeltaInByte) {
        remainingMemoryCapacityInByte.getAndAdd(currentFreedMemoryInBytes);
        currentFreedMemoryInBytes = 0;
        hasEnoughMemory.signalAll();
      }
    } finally {
      memoryLock.unlock();
    }

    return record;
  }

  @Override
  public boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException {
    throw new VeniceException("Operation is not supported yet!");
  }

  @Override
  public boolean add(T t) {
    throw new VeniceException("Operation is not supported yet!");
  }

  @Override
  public boolean offer(T t) {
    throw new VeniceException("Operation is not supported yet!");
  }

  @Override
  public T remove() {
    throw new VeniceException("Operation is not supported yet!");
  }

  @Override
  public T poll() {
    throw new VeniceException("Operation is not supported yet!");
  }

  @Override
  public T element() {
    throw new VeniceException("Operation is not supported yet!");
  }

  @Override
  public T peek() {
    throw new VeniceException("Operation is not supported yet!");
  }

  @Override
  public T poll(long timeout, TimeUnit unit) throws InterruptedException {
    throw new VeniceException("Operation is not supported yet!");
  }

  @Override
  public int remainingCapacity() {
    throw new VeniceException("Operation is not supported yet!");
  }

  @Override
  public boolean remove(Object o) {
    throw new VeniceException("Operation is not supported yet!");
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return this.queue.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    throw new VeniceException("Operation is not supported yet!");
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new VeniceException("Operation is not supported yet!");
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new VeniceException("Operation is not supported yet!");
  }

  @Override
  public void clear() {
    throw new VeniceException("Operation is not supported yet!");
  }

  @Override
  public int size() {
    return this.queue.size();
  }

  @Override
  public boolean isEmpty() {
    return this.queue.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return this.queue.contains(o);
  }

  @Override
  public Iterator<T> iterator() {
    return this.queue.iterator();
  }

  @Override
  public Object[] toArray() {
    return this.queue.toArray();
  }

  @Override
  public <T1> T1[] toArray(T1[] a) {
    return this.queue.toArray(a);
  }

  @Override
  public int drainTo(Collection<? super T> c) {
    throw new VeniceException("Operation is not supported yet!");
  }

  @Override
  public int drainTo(Collection<? super T> c, int maxElements) {
    throw new VeniceException("Operation is not supported yet!");
  }
}
