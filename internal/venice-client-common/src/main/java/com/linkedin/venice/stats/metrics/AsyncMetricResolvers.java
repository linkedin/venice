package com.linkedin.venice.stats.metrics;

import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Holder for the four named functional interfaces that wire the two-callback (liveness + value)
 * contract used by {@link AsyncMetricEntityStateOneEnum} and {@link AsyncMetricEntityStateTwoEnums}.
 * Co-located in a single file because they are only meaningful together.
 */
public final class AsyncMetricResolvers {
  private AsyncMetricResolvers() {
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
   * Reads a {@code double} value from a non-null state plus the enum dimension. Used by
   * {@link AsyncMetricEntityStateOneEnum} on combos for which
   * {@link LiveStateResolverOneEnum#resolve} returned non-null.
   *
   * @param <S> the backing state type
   * @param <E> the enum dimension type
   */
  @FunctionalInterface
  public interface ValueResolverOneEnum<S, E extends Enum<E> & VeniceDimensionInterface> {
    double extractValue(@Nonnull S state, E enumValue);
  }

  /**
   * Reads a {@code double} value from a non-null state plus both enum dimensions. Used by
   * {@link AsyncMetricEntityStateTwoEnums} on pairs for which
   * {@link LiveStateResolverTwoEnums#resolve} returned non-null.
   *
   * @param <S>  the backing state type
   * @param <E1> the first enum dimension type
   * @param <E2> the second enum dimension type
   */
  @FunctionalInterface
  public interface ValueResolverTwoEnums<S, E1 extends Enum<E1> & VeniceDimensionInterface, E2 extends Enum<E2> & VeniceDimensionInterface> {
    double extractValue(@Nonnull S state, E1 e1, E2 e2);
  }
}
