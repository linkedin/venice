package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * The two-callback (liveness + value) contract for async gauges: a live-state resolver returns the state to read, or
 * {@code null} to emit nothing, and a value resolver reads the value from non-null state.
 * Co-located in a single file because they are only meaningful together.
 */
public final class AsyncMetricResolvers {
  private AsyncMetricResolvers() {
  }

  /** Returns the state to read, or {@code null} to emit no sample. */
  @FunctionalInterface
  public interface LiveStateResolver<S> {
    @Nullable
    S resolve();
  }

  /**
   * Reads the value from non-null state. Long gauges record integral values exactly; {@code Double} and {@code Float}
   * values are recorded only when finite.
   */
  @FunctionalInterface
  public interface ValueResolver<S> {
    @Nonnull
    Number extractValue(@Nonnull S state);
  }

  /**
   * Resolves the backing state for one enum dimension value, or {@code null} when the combo is
   * dormant. The {@code null} return is the liveness signal used by
   * {@link AsyncMetricEntityStateOneEnum}: dormant combos are not observed by the SDK and
   * therefore do not count against the per-instrument cardinality cap.
   *
   * @param <E> the enum dimension type
   * @param <S> the backing state type (any reference type — never inspected beyond null-check)
   */
  @FunctionalInterface
  public interface LiveStateResolverOneEnum<E extends Enum<E> & VeniceDimensionInterface, S> {
    @Nullable
    S resolve(E enumValue);
  }

  /**
   * Resolves the backing state for an {@code (e1, e2)} dimension pair, or {@code null} when the
   * pair is dormant. The {@code null} return is the liveness signal used by
   * {@link AsyncMetricEntityStateTwoEnums}: dormant pairs are not observed by the SDK and
   * therefore do not count against the per-instrument cardinality cap.
   *
   * @param <E1> the first enum dimension type
   * @param <E2> the second enum dimension type
   * @param <S>  the backing state type (any reference type — never inspected beyond null-check)
   */
  @FunctionalInterface
  public interface LiveStateResolverTwoEnums<E1 extends Enum<E1> & VeniceDimensionInterface, E2 extends Enum<E2> & VeniceDimensionInterface, S> {
    @Nullable
    S resolve(E1 e1, E2 e2);
  }

  /**
   * Reads the value from a non-null state plus the enum dimension, recorded as described in {@link ValueResolver}.
   * Used by {@link AsyncMetricEntityStateOneEnum} on combos for which {@link LiveStateResolverOneEnum#resolve}
   * returned non-null.
   *
   * @param <S> the backing state type
   * @param <E> the enum dimension type
   */
  @FunctionalInterface
  public interface ValueResolverOneEnum<S, E extends Enum<E> & VeniceDimensionInterface> {
    @Nonnull
    Number extractValue(@Nonnull S state, E enumValue);
  }

  /**
   * Reads the value from a non-null state plus both enum dimensions, recorded as described in {@link ValueResolver}.
   * Used by {@link AsyncMetricEntityStateTwoEnums} on pairs for which {@link LiveStateResolverTwoEnums#resolve}
   * returned non-null.
   *
   * @param <S>  the backing state type
   * @param <E1> the first enum dimension type
   * @param <E2> the second enum dimension type
   */
  @FunctionalInterface
  public interface ValueResolverTwoEnums<S, E1 extends Enum<E1> & VeniceDimensionInterface, E2 extends Enum<E2> & VeniceDimensionInterface> {
    @Nonnull
    Number extractValue(@Nonnull S state, E1 e1, E2 e2);
  }
}
