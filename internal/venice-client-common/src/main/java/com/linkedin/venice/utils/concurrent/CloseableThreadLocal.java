package com.linkedin.venice.utils.concurrent;

import java.util.Map;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A wrapper of {@link ThreadLocal} for {@link AutoCloseable} objects.
 * Java's {@link ThreadLocal} only dereferences the thread-local objects
 * when the {@link Thread} exits which triggers their garbage collection.
 * It does not {@code close} the {@link AutoCloseable} thread-local objects.
 * @param <T> The {@link AutoCloseable} type whose objects will be held by an object of this class.
 */
public class CloseableThreadLocal<T extends AutoCloseable> implements AutoCloseable {
  private static final Logger LOGGER = LogManager.getLogger(CloseableThreadLocal.class);
  private final Map<Thread, T> threadToValueMap = new VeniceConcurrentHashMap<>();
  private final ThreadLocal<T> threadLocal;

  /**
   * Creates a closeable thread local. The initial value of the
   * variable is determined by invoking the {@code get} method
   * on the {@code Supplier}.
   * @param initialValue the supplier to be used to determine
   *                     the initial value for each Thread
   */
  public CloseableThreadLocal(Supplier<T> initialValue) {
    this.threadLocal = ThreadLocal.withInitial(wrap(initialValue));
  }

  /**
   * Returns the value in the current thread's copy of this
   * thread-local variable.  If the variable has no value for the
   * current thread, it is first initialized to the value returned
   * by an invocation of the {@code initialValue} method.
   *
   * @return the current thread's value of this thread-local
   */
  public T get() {
    return threadLocal.get();
  }

  /**
   * Removes the current thread's value for this thread-local
   * variable. It also triggers {@code close} on the current
   * thread's value. If this thread-local variable is subsequently
   * {@linkplain #get read} by the current thread, its value will be
   * reinitialized by invoking its {@code initialValue} method,
   * unless its value is {@linkplain #set set} by the current thread
   * in the interim. This may result in multiple invocations of the
   * {@code initialValue} method in the current thread.
   */
  public void remove() {
    closeQuietly(Thread.currentThread());
    threadLocal.remove();
  }

  /**
   * Sets the current thread's copy of this thread-local variable
   * to the specified value. If the thread that is setting a value
   * had a previous value set, {@code close} will be triggered on
   * the previous value. Most subclasses will have no need to override
   * this method, relying solely on the {@code initialValue} method to
   * set the values of thread-locals.
   *
   * @param value the value to be stored in the current thread's copy of
   *        this thread-local.
   */
  public void set(T value) {
    Thread currentThread = Thread.currentThread();
    if (threadToValueMap.containsKey(currentThread)) {
      closeQuietly(currentThread);
    }

    threadLocal.set(value);
    threadToValueMap.put(currentThread, value);
  }

  /**
   * Clean up the resources held by this object. It triggers {@code close}
   * on each thread's value. It is the responsibility of the caller to ensure
   * that all threads have finished processing the thread-local objects.
   */
  @Override
  public void close() {
    threadToValueMap.keySet().parallelStream().forEach(this::closeQuietly);
  }

  private Supplier<T> wrap(Supplier<T> initialValue) {
    return () -> {
      T value = initialValue.get();
      threadToValueMap.put(Thread.currentThread(), value);
      return value;
    };
  }

  private void closeQuietly(Thread thread) {
    if (thread != null && threadToValueMap.containsKey(thread)) {
      try {
        threadToValueMap.remove(thread).close();
      } catch (Exception e) {
        LOGGER.error(e);
      }
    }
  }
}
