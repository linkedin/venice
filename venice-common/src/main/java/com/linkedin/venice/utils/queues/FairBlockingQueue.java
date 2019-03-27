package com.linkedin.venice.utils.queues;

import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * The FairBlockingQueue is meant to work with Labeled objects instead of vanilla objects.
 * Each Labeled includes a label.  The FairBlockingQueue uses that label to categorize work items.  Internally
 * there is another queue for each category and work items go into the category-specific queue based on the label. Any
 * category-specific queue that is non-empty gets placed into a global work-queue.  When we pull an item from the
 * FairBlockingQueue, it internally pulls a category-specific queue from the work-queue, returns a work item from the
 * category-specific queue, and re-queues the category-specific queue if it is non-empty.
 *
 * Most locking is done on the category-specific queues to ensure we avoid a race condition that would double-queue
 * them into the work-queue. A few operations require a broader lock on the work queue.  Any operation that requires a
 * lock on both the category-specific queue and the work-queue must lock the work-queue first.  Never take a lock on
 * multiple category-specific queues.  This would risk deadlock.
 *
 * If a category queue is not-empty then it is in the work queue.  If a category queue is empty, then it is not in the
 * work queue.  This must always be true because we depend on only the work queue's behavior of blocking on empty.  If
 * we were to block on a category queue being empty then we would be stuck until an item was added to that category.
 * This means that when we remove an item from the queue with #remove(Object) then the category queue containing that item
 * may now be empty and the category queue must be removed from the work queue.  To prevent the work queue from grabbing
 * a handle on that category queue before it is removed from the work queue we must synchronize on the work queue during
 * every method that removes category queues from the work queue.
 */
public class FairBlockingQueue<T> implements BlockingQueue<T> {

  private final BlockingQueue<Queue<T>> workQueue = new LinkedBlockingQueue<>();
  private final Map<String, Queue<T>> categoryQueues = new VeniceConcurrentHashMap<>();

  public FairBlockingQueue(){
  }

  private boolean internalAdd(T t, BiFunction<BlockingQueue<Queue<T>>, Queue<T>, Boolean> enqueueFunction){
    Queue<T> categoryQueue = categoryQueues.computeIfAbsent(getCategory(t),
        label -> new ConcurrentLinkedQueue<>());
    synchronized (categoryQueue) {
      if (categoryQueue.isEmpty()) { //empty category queues are not in the work queue and must be enqueued
        categoryQueue.add(t);
        return enqueueFunction.apply(workQueue, categoryQueue);
      } else { //non-empty category queues are already in the work queue and must not be enqueued
        categoryQueue.add(t);
        return true;
      }
    }
  }

  // for #remove() and #poll() which don't block
  private synchronized T internalRemove(
      Function<BlockingQueue<Queue<T>>, Queue<T>> dequeueFunction){
    Queue<T> categoryQueue = dequeueFunction.apply(workQueue); //might return null, might throw
    return popRunnableFromCategoryQueue(categoryQueue);
  }

  // for #take() and #poll(timeout) which block
  private synchronized T internalRemoveInterruptable(
      InterruptableFunction<BlockingQueue<Queue<T>>, Queue<T>> dequeueFunction)
      throws InterruptedException {
    Queue<T> categoryQueue = dequeueFunction.apply(workQueue); //might block, might throw, might return null
    return popRunnableFromCategoryQueue(categoryQueue);
  }

  private T popRunnableFromCategoryQueue(Queue<T> categoryQueue){
    if (null == categoryQueue) { // because #poll() returns null if queue is empty
      return null;
    }
    synchronized (categoryQueue){
      T returnObj = categoryQueue.remove(); //should not throw exception because categoryQueue should not be empty
      if (!categoryQueue.isEmpty()){
        workQueue.add(categoryQueue); //should not throw an exception because work queue is not bounded
      }
      return returnObj;
    }
  }

  @Override
  public boolean add(T t) {
    return internalAdd(t, (bigQueue, smallQueue) -> bigQueue.add(smallQueue));
  }

  @Override
  public boolean offer(T t) {
    return internalAdd(t, (bigQueue, smallQueue) -> bigQueue.offer(smallQueue));
  }

  @Override
  public T remove() {
    return internalRemove(bigQueue -> bigQueue.remove());
  }

  @Override
  public T poll() {
    return internalRemove(bigQueue -> bigQueue.poll());
  }

  @Override
  public T element() {
    return workQueue.element().element();
  }

  @Override
  public T peek() {
    return workQueue.peek().peek();
  }

  @Override
  public void put(T t) throws InterruptedException {
    internalAdd(t, (bigQueue, smallQueue) -> bigQueue.add(smallQueue)); //work queue is unbounded, so this shouldn't throw
  }

  @Override
  public boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException {
    return offer(t); //work queue is unbounded, so this shouldn't block (or timeout)
  }

  @Override
  public T take() throws InterruptedException {
    return internalRemoveInterruptable(bigQueue -> bigQueue.take());
  }

  @Override
  public T poll(long timeout, TimeUnit unit) throws InterruptedException {
    return internalRemoveInterruptable(bigQueue -> bigQueue.poll(timeout, unit));
  }

  @Override
  public int remainingCapacity() {
    return Integer.MAX_VALUE;
  }

  @Override
  public synchronized boolean remove(Object o) {
    String category = getCategory(o);
    Queue<T> categoryQueue = categoryQueues.get(category);
    synchronized (categoryQueue) {
      boolean modified = categoryQueue.remove(o);
      if (modified && categoryQueue.isEmpty()){
        workQueue.remove(categoryQueue);
      }
      return modified;
    }
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    for (Object o : c){
      if (! contains(o)){
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    for (T t : c){
      add(t);
    }
    return true;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    boolean changed = false;
    for (Object o : c){
      changed |= remove(o);
    }
    return changed;
  }

  @Override
  public synchronized boolean retainAll(Collection<?> c) {
    boolean changed = false;
    for (Queue<T> queue : categoryQueues.values()){
      changed |= queue.retainAll(c);
    }
    return changed;
  }

  @Override
  public synchronized void clear() {
    workQueue.clear();
    categoryQueues.clear();
  }

  @Override
  public int size() {
    return categoryQueues.values().stream().mapToInt(q -> q.size()).reduce(0, (a,b) -> a + b);
  }

  @Override
  public boolean isEmpty() {
    return workQueue.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    String category = getCategory(o);
    return categoryQueues.get(category).contains(o);
  }

  @Override
  public Iterator<T> iterator() {
    return new Itr();
  }

  @Override
  public Object[] toArray() {
    List<Object[]> arrays = new LinkedList<>();
    int size = 0;
    for (Queue<T> q : categoryQueues.values()) {
      Object[] categoryArray = q.toArray();
      arrays.add(categoryArray);
      size += categoryArray.length;
    }
    Object[] outputArray = new Object[size];
    int currentSize = 0;
    for (Object[] array : arrays){
      int thisSize = array.length;
      System.arraycopy(array, 0, outputArray, currentSize, thisSize);
      currentSize += thisSize;
    }
    return outputArray;
  }

  @Override
  public <T> T[] toArray(T[] a) {
    List<Object[]> arrays = new LinkedList<>();
    int size = 0;
    for (Queue queue : categoryQueues.values()) {
      Object[] categoryArray = queue.toArray();
      arrays.add(categoryArray);
      size += categoryArray.length;
    }
    T[] outputArray;
    if (size <= a.length){
      outputArray = a;
    } else {
      outputArray = (T[]) new Object[size];
    }
    int currentSize = 0;
    for (Object[] array : arrays){
      int thisSize = array.length;
      System.arraycopy(array, 0, outputArray, currentSize, thisSize);
      currentSize += thisSize;
    }
    return outputArray;
  }

  @Override
  public int drainTo(Collection<? super T> c) {
    return drainTo(c, Integer.MAX_VALUE);
  }

  @Override
  public synchronized int drainTo(Collection<? super T> c, int maxElements) {
    if (c == null) {
      throw new NullPointerException();
    }
    if (c == this) {
      throw new IllegalArgumentException();
    }
    if (maxElements <= 0) {
      return 0;
    }
    int drainCount = 0;
    while (drainCount < maxElements){
      T t = peek();
      if (null == t){
        break;
      }
      try {
        c.add(t);
        drainCount += 1;
        remove(); //We have synchronized on the queue, so this removes the peeked object if c.add() didn't throw
      } catch (Exception e) {
        break;
      }
    }
    return drainCount;
  }

  @FunctionalInterface
  private interface InterruptableFunction<T1,R> {
    R apply(T1 t) throws InterruptedException;
  }

  private static String getCategory(Object o){
    String category = "";
    if (o instanceof Labeled){
      category = ((Labeled) o).getLabel();
    }
    return category;
  }

  private class Itr implements Iterator<T> {

    // Defer iterator behavior to the iterators on the category queues.  Run through them one at a time.
    // Note that this is a different order than the FairBlockingQueue will return.
    private LinkedList<Iterator<T>> listOfIterators = null;

    Itr(){
      synchronized (workQueue){
        Iterator<Queue<T>> workQueueIterator = workQueue.iterator();
        while (workQueueIterator.hasNext()){
          listOfIterators.add(workQueueIterator.next().iterator());
        }
      }
    }

    @Override
    public boolean hasNext() {
      while (! listOfIterators.isEmpty()) {
        if (listOfIterators.getFirst().hasNext()){
          return true;
        } else { //first iterator in the list doesn't #hasNext(), so it is empty.  Move on to the next one
          listOfIterators.removeFirst();
        }
      }
      return false; // We've gone through every iterator in the list, none #hasNext(), so we're empty
    }

    @Override
    public T next() {
      if (listOfIterators.isEmpty()){
        throw new NoSuchElementException();
      }
      return listOfIterators.getFirst().next();
    }
  }
}
