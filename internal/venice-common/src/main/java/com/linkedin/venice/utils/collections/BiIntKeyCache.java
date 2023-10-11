package com.linkedin.venice.utils.collections;

import com.linkedin.venice.utils.BiIntFunction;
import com.linkedin.venice.utils.SparseConcurrentList;
import java.util.function.IntFunction;


/**
 * Read-through cache for items retrievable by two positive integer IDs.
 *
 * N.B.: This class uses a list of lists internally. Therefore, the memory footprint will be proportional to the
 *       highest integer ID used. As such, this class should only be used in cases where the range of valid IDs goes
 *       from zero to a low max.
 */
public class BiIntKeyCache<E> {
  /** The index within this list is the "first" param of the {@link BiIntKeyCache#get(int, int)} function.*/
  private final SparseConcurrentList<InnerList<E>> outerList = new SparseConcurrentList<>();

  /** Pre-allocated to minimize GC when calling {@link SparseConcurrentList#computeIfAbsent(int, IntFunction)} */
  private final IntFunction<InnerList<E>> innerListSupplier;

  public BiIntKeyCache(BiIntFunction<E> generator) {
    this.innerListSupplier = outerId -> new InnerList<>(outerId, generator);
  }

  /**
   * @return the element corresponding to the two int provided
   */
  public E get(int firstId, int secondId) {
    return this.outerList.computeIfAbsent(firstId, this.innerListSupplier).computeIfAbsent(secondId);
  }

  /**
   * The elements to be returned are held inside instances of this {@link InnerList} class. The index within this list
   * is the "second" param of the {@link BiIntKeyCache#get(int, int)} function.
   */
  private static class InnerList<E> extends SparseConcurrentList<E> {
    /** Needed because the parent class is serializable (this is also the reason for making fields transient). */
    private static final long serialVersionUID = 1L;

    /** Passed into {@link #generator} by {@link #generateElement(int)} to generate elements on cache misses. */
    private final transient int idWithinOuterList;

    /** Called by {@link #generateElement(int)} to generate elements on cache misses. */
    private final transient BiIntFunction<E> generator;

    private InnerList(int idWithinOuterList, BiIntFunction<E> generator) {
      this.generator = generator;
      this.idWithinOuterList = idWithinOuterList;
    }

    /** Get the requested element, or else generate it. */
    private E computeIfAbsent(int idWithinInnerList) {
      return super.computeIfAbsent(idWithinInnerList, this::generateElement);
    }

    /**
     * Pre-defined function so that it can be passed as is, without extra allocation, into
     * {@link SparseConcurrentList#computeIfAbsent(int, IntFunction)}
     */
    private E generateElement(int idWithinInnerList) {
      return this.generator.apply(this.idWithinOuterList, idWithinInnerList);
    }
  }
}
