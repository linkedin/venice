package com.linkedin.venice.utils.collections;

import static com.linkedin.venice.memory.ClassSizeEstimator.getClassOverhead;
import static com.linkedin.venice.memory.InstanceSizeEstimator.getByteArraySizeByLength;

import com.linkedin.venice.memory.Measurable;
import java.util.LinkedHashMap;


/**
 * A subclass of {@link LinkedHashMap} which does a best-effort attempt at guessing its size on heap + the size of its
 * values. Several assumptions are made which could make this imprecise in some contexts. See the usage in
 * {@link com.linkedin.venice.pubsub.api.PubSubMessageHeaders} for the usage it was originally intended for...
 */
public class MeasurableLinkedHashMap<K, V extends Measurable> extends LinkedHashMap<K, V> implements Measurable {
  private static final int SHALLOW_CLASS_OVERHEAD = getClassOverhead(MeasurableLinkedHashMap.class);
  private static final int ENTRY_CLASS_OVERHEAD = getClassOverhead(PseudoLinkedHashMapEntry.class);
  /**
   * The reason to have {@link PseudoLinkedValues} is that if we call {@link #getHeapSize()} then we will iterate over
   * the values and this causes a cached view of the map to be held, thus creating one more object...
   */
  private static final int LINKED_VALUES_SHALLOW_CLASS_OVERHEAD =
      getClassOverhead(new MeasurableLinkedHashMap().getPseudoLinkedValues().getClass());

  /**
   * The default initial capacity - MUST be a power of two.
   */
  private static final int DEFAULT_INITIAL_CAPACITY = 1 << 4; // aka 16

  /**
   * The maximum capacity, used if a higher value is implicitly specified
   * by either of the constructors with arguments.
   * MUST be a power of two <= 1<<30.
   */
  private static final int MAXIMUM_CAPACITY = 1 << 30;

  /**
   * The load factor used when none specified in constructor.
   */
  private static final float DEFAULT_LOAD_FACTOR = 0.75f;

  private int guessCurrentlyAllocatedTableSize() {
    if (isEmpty()) {
      /**
       * This is not necessarily true... if a map had entries in it, and then they got all removed, then the table would
       * be empty but its capacity would still be whatever it was when it had the most entries, since maps grow but
       * don't shrink. Tracking this accurately would require making this subclass stateful, which would add complexity
       * and may not be a good trade. So we take a naive approach instead.
       */
      return 0;
    }

    int assumedCapacity = DEFAULT_INITIAL_CAPACITY;
    int assumedThreshold = (int) (DEFAULT_LOAD_FACTOR * (float) assumedCapacity);
    while (assumedCapacity <= MAXIMUM_CAPACITY) {
      if (size() < assumedThreshold) {
        return assumedCapacity;
      }

      // double assumed capacity and threshold
      assumedCapacity = assumedCapacity << 1;
      assumedThreshold = assumedThreshold << 1;
    }
    return MAXIMUM_CAPACITY;
  }

  @Override
  public int getHeapSize() {
    int size = SHALLOW_CLASS_OVERHEAD;
    int tableSize = getByteArraySizeByLength(guessCurrentlyAllocatedTableSize());
    if (tableSize > 0) {
      size += tableSize;
      size += size() * ENTRY_CLASS_OVERHEAD;
      for (V value: values()) {
        /**
         * N.B.: We only consider the value size, and ignore the key size. In our initial use case for this class, this
         * is okay, though technically not correct.
         */
        size += value.getHeapSize();
      }
      /**
       * Unfortunately, the act of calling {@link #values()} ends up generating a new object, and storing it in a class
       * field, so we must now account for it...
       */
      size += LINKED_VALUES_SHALLOW_CLASS_OVERHEAD;
    }

    return size;
  }

  // The classes below are for the sake of measuring overhead, since the equivalent JDK classes have limited visibility
  static class PseudoHashMapNode {
    int hash;
    Object key, value, next;
  }

  static class PseudoLinkedHashMapEntry<K, V> extends PseudoHashMapNode {
    Object before, after;
  }

  private PseudoLinkedValues getPseudoLinkedValues() {
    return new PseudoLinkedValues();
  }

  final class PseudoLinkedValues {
  }
}
