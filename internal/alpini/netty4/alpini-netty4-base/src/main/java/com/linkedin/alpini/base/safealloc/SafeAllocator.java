package com.linkedin.alpini.base.safealloc;

import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufAllocatorMetric;
import io.netty.buffer.ByteBufAllocatorMetricProvider;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.internal.PlatformDependent;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * An instance of {@link SafeAllocator} may be used to wrap an existing {@linkplain ByteBufAllocator}
 * to provide insurance against slow memory leaks from exceptional conditions by providing a means
 * for the leaked {@linkplain ByteBuf} objects to be reclaimed.
 *
 * Makes use of a reference queue per thread in order to avoid some of the synchronization costs.
 */
public class SafeAllocator extends AbstractByteBufAllocator implements ByteBufAllocatorMetricProvider {
  private static final Logger LOG = LogManager.getLogger(SafeAllocator.class);

  public static final SafeAllocator POOLED_ALLOCATOR;

  public static final SafeAllocator UNPOOLED_ALLOCATOR;

  private static final ByteBufAllocatorMetric UNKNOWN_METRICS = new ByteBufAllocatorMetric() {
    @Override
    public long usedHeapMemory() {
      return -1;
    }

    @Override
    public long usedDirectMemory() {
      return -1;
    }
  };

  private final ByteBufAllocator _allocator;
  private final SafeAllocatorMetric _metric;
  private final ThreadLocal<FinalizableReferenceQueue> _referenceQueue;
  final ConcurrentMap<SafeReference, Boolean> _active;
  final LongAdder _leakCount;
  final LongAdder _referenceCount;
  final LongAdder _queuesCount;

  public SafeAllocator(@Nonnull ByteBufAllocator allocator) {
    super(PlatformDependent.directBufferPreferred());
    _allocator = allocator;
    _referenceQueue = ThreadLocal.withInitial(FinalizableReferenceQueue::new);
    _active = new ConcurrentHashMap<>(256, 0.75f, 48);
    _leakCount = new LongAdder();
    _referenceCount = new LongAdder();
    _queuesCount = new LongAdder();

    _metric = new SafeAllocatorMetric(
        this,
        allocator instanceof ByteBufAllocatorMetricProvider
            ? ((ByteBufAllocatorMetricProvider) allocator).metric()
            : UNKNOWN_METRICS);
  }

  final FinalizableReferenceQueue referenceQueue() {
    return _referenceQueue.get();
  }

  private FinalizableReferenceQueue checkReferenceQueue() {
    return referenceQueue().check();
  }

  final SafeReference makeReference(SafeByteBuf referent, ReferenceQueue<SafeByteBuf> queue, ByteBuf buf) {
    SafeReference ref = new SafeReference(referent, queue, buf);
    _referenceCount.increment();
    _active.put(ref, Boolean.TRUE);
    return ref;
  }

  private ByteBuf newBuffer(FinalizableReferenceQueue queue, ByteBuf source) {
    ByteBuf buf = new DerivedMutableByteBuf(new SafeByteBuf(this, queue, source));
    buf.setIndex(0, 0);
    buf.markReaderIndex();
    buf.markWriterIndex();
    return buf.touch(this);
  }

  @Override
  protected final ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
    return newBuffer(checkReferenceQueue(), _allocator.heapBuffer(initialCapacity, maxCapacity));
  }

  @Override
  protected final ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
    return newBuffer(checkReferenceQueue(), _allocator.directBuffer(initialCapacity, maxCapacity));
  }

  @Override
  public final boolean isDirectBufferPooled() {
    return _allocator.isDirectBufferPooled();
  }

  @Override
  public SafeAllocatorMetric metric() {
    return _metric;
  }

  protected void reportLeak(String message, int capacity, ByteBuf buf, Object hint) {
    LOG.error(message, capacity, buf, hint);
  }

  @Override
  public String toString() {
    return "SafeAllocator{" + "_allocator=" + _allocator + '}';
  }

  private class FinalizableReferenceQueue extends ReferenceQueue<SafeByteBuf> {
    FinalizableReferenceQueue() {
      _queuesCount.increment();
    }

    FinalizableReferenceQueue check() {
      Reference<? extends SafeByteBuf> ref;
      do {
        ref = poll();
        if (ref instanceof SafeReference) {
          SafeReference reference = (SafeReference) ref;
          _leakCount.increment();
          int capacity = reference.capacity();
          String message;
          if (!_active.remove(reference)) {
            message = "Missing reference, cap=(), {}, last touched by {}";
          } else if (reference.release()) {
            message = "Leak recovered, cap={}, {}, last touched by {}";
          } else {
            message = "Unrecovered leak, cap={}, {} last touched by {}";
          }
          reportLeak(message, capacity, reference.store(), reference._hint);
        }
      } while (ref != null);
      return this;
    }

    @Override
    protected void finalize() throws Throwable {
      check();
      super.finalize();
    }
  }

  static {
    POOLED_ALLOCATOR = new SafeAllocator(PooledByteBufAllocator.DEFAULT);
    UNPOOLED_ALLOCATOR = new SafeAllocator(UnpooledByteBufAllocator.DEFAULT);
  }
}
