package com.linkedin.venice.utils;

/**
 * A Supplier that throws checked exceptions.
 *
 * @param <T> result type
 */
@FunctionalInterface
public interface VeniceCheckedSupplier<T> {
  T get() throws Throwable;
}
