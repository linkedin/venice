package com.linkedin.alpini.base.misc;

import java.lang.ref.SoftReference;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import javax.annotation.Nonnull;


/**
 * This class provides thread-local variables with {@link SoftReference} semantics.
 *
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class SoftThreadLocal<T> implements Supplier<T> {
  private static final ThreadLocal<SoftReference<Map<SoftThreadLocal<?>, ?>>> THREAD_LOCAL =
      ThreadLocal.withInitial(SoftThreadLocal::makeMap);

  private static SoftReference<Map<SoftThreadLocal<?>, ?>> makeMap() {
    return new SoftReference<>(new IdentityHashMap<>());
  }

  /**
   * Creates a thread local variable. The initial value of the variable is
   * determined by invoking the {@code get} method on the {@code Supplier}.
   *
   * @param <S> the type of the thread local's value
   * @param supplier the supplier to be used to determine the initial value
   * @return a new thread local variable
   * @throws NullPointerException if the specified supplier is null
   * @since 1.8
   */
  public static @Nonnull <S> SoftThreadLocal<S> withInitial(@Nonnull Supplier<? extends S> supplier) {
    return new SuppliedSoftThreadLocal<>(supplier);
  }

  /**
   * Returns the current thread's "initial value" for this
   * thread-local variable.  This method will be invoked the first
   * time a thread accesses the variable with the {@link #get}
   * method, unless the thread previously invoked the {@link #set}
   * method, in which case the {@code initialValue} method will not
   * be invoked for the thread.  Normally, this method is invoked at
   * most once per thread, but it may be invoked again in case of
   * subsequent invocations of {@link #remove} followed by {@link #get}.
   *
   * <p>This implementation simply returns {@code null}; if the
   * programmer desires thread-local variables to have an initial
   * value other than {@code null}, {@code ThreadLocal} must be
   * subclassed, and this method overridden.  Typically, an
   * anonymous inner class will be used.
   *
   * @return the initial value for this thread-local
   */
  protected T initialValue() {
    return null;
  }

  @SuppressWarnings("unchecked")
  private Map<SoftThreadLocal<T>, T> getMap() {
    SoftReference<Map<SoftThreadLocal<?>, ?>> reference = THREAD_LOCAL.get();
    for (;;) {
      Map map = reference.get();
      if (map != null) {
        return (Map<SoftThreadLocal<T>, T>) map;
      }
      reference = makeMap();
      THREAD_LOCAL.set(reference);
    }
  }

  private T initialMapValue(SoftThreadLocal<T> ignored) {
    return initialValue();
  }

  /**
   * Returns the value in the current thread's copy of this
   * thread-local variable.  If the variable has no value for the
   * current thread, it is first initialized to the value returned
   * by an invocation of the {@link #initialValue} method.
   *
   * @return the current thread's value of this thread-local
   */
  @Override
  public T get() {
    return getMap().computeIfAbsent(this, this::initialMapValue);
  }

  /**
   * Sets the current thread's copy of this thread-local variable
   * to the specified value.  Most subclasses will have no need to
   * override this method, relying solely on the {@link #initialValue}
   * method to set the values of thread-locals.
   *
   * @param value the value to be stored in the current thread's copy of
   *        this thread-local.
   */
  public void set(T value) {
    getMap().put(this, value);
  }

  /**
   * Removes the current thread's value for this thread-local
   * variable.  If this thread-local variable is subsequently
   * {@linkplain #get read} by the current thread, its value will be
   * reinitialized by invoking its {@link #initialValue} method,
   * unless its value is {@linkplain #set set} by the current thread
   * in the interim.  This may result in multiple invocations of the
   * {@code initialValue} method in the current thread.
   */
  public void remove() {
    getMap().remove(this);
  }

  /**
   * An extension of SoftThreadLocal that obtains its initial value from
   * the specified {@code Supplier}.
   */
  static final class SuppliedSoftThreadLocal<T> extends SoftThreadLocal<T> {
    private final Supplier<? extends T> supplier;

    SuppliedSoftThreadLocal(Supplier<? extends T> supplier) {
      this.supplier = Objects.requireNonNull(supplier);
    }

    @Override
    protected T initialValue() {
      return supplier.get();
    }
  }
}
