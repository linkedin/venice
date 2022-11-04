package com.linkedin.venice.utils.concurrent;

import com.linkedin.venice.utils.HashCodeComparator;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
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
  private final Set<T> valueSet = new ConcurrentSkipListSet<>(new HashCodeComparator<>());
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
   * Clean up the resources held by this object. It triggers {@code close}
   * on each thread's value. It is the responsibility of the caller to ensure
   * that all threads have finished processing the thread-local objects.
   */
  @Override
  public void close() {
    valueSet.parallelStream().forEach(this::closeQuietly);
    valueSet.clear();
  }

  private Supplier<T> wrap(Supplier<T> initialValue) {
    return () -> {
      T value = initialValue.get();
      valueSet.add(value);
      return value;
    };
  }

  private void closeQuietly(T value) {
    try {
      value.close();
    } catch (Exception e) {
      LOGGER.error(e);
    }
  }
}
