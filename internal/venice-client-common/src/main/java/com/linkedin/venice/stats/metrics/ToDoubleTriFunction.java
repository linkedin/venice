package com.linkedin.venice.stats.metrics;

/**
 * Three-argument specialization of {@link java.util.function.ToDoubleBiFunction}. Used by
 * {@link AsyncMetricEntityStateTwoEnums} to extract a {@code double} value from a resolved state
 * plus both enum dimensions without boxing.
 */
@FunctionalInterface
public interface ToDoubleTriFunction<T1, T2, T3> {
  double applyAsDouble(T1 t1, T2 t2, T3 t3);
}
