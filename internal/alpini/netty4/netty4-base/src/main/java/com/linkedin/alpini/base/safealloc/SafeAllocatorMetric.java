package com.linkedin.alpini.base.safealloc;

import io.netty.buffer.ByteBufAllocatorMetric;


/**
 * Metrics regarding the {@link SafeAllocator}
 */
public final class SafeAllocatorMetric implements ByteBufAllocatorMetric {
  private final SafeAllocator _allocator;
  private final ByteBufAllocatorMetric _metric;

  SafeAllocatorMetric(SafeAllocator allocator, ByteBufAllocatorMetric metric) {
    _allocator = allocator;
    _metric = metric;
  }

  public long totalReferences() {
    return _allocator._referenceCount.longValue();
  }

  public long totalQueues() {
    return _allocator._queuesCount.longValue();
  }

  public long activeAllocations() {
    return _allocator._active.size();
  }

  public long leakedAllocations() {
    return _allocator._leakCount.longValue();
  }

  @Override
  public long usedHeapMemory() {
    return _metric.usedHeapMemory();
  }

  @Override
  public long usedDirectMemory() {
    return _metric.usedDirectMemory();
  }
}
