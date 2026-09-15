package com.linkedin.davinci.blobtransfer;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocatorMetric;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Supplies the {@link PooledByteBufAllocator} that blob transfer channels allocate their buffers from.
 * <p>
 * Netty channels allocate from the process-wide {@link PooledByteBufAllocator#DEFAULT} unless a channel is
 * explicitly given its own, so blob transfer shares one pool with the read path, the write path and every
 * other Netty user in the process. That makes the reported heap and direct memory usage an aggregate which
 * cannot be attributed to any single component. Giving blob transfer its own pool makes
 * {@code allocator.metric().usedDirectMemory()} report the memory charged to that pool alone, whichever
 * pipeline stage happens to be holding the buffers. It covers only the buffers that come from this allocator,
 * so the plain {@code byte[]} and {@code Unpooled} buffers the metadata path builds are not counted.
 */
public final class BlobTransferPooledByteBufAllocator {
  private static final Logger LOGGER = LogManager.getLogger(BlobTransferPooledByteBufAllocator.class);

  private BlobTransferPooledByteBufAllocator() {
  }

  /**
   * @param role identifies which end of blob transfer owns the allocator, so the chunk size logged below can be
   *        attributed to the sender or the receiver.
   * @param dedicatedAllocatorEnabled when {@code false}, the returned allocator is
   *        {@link PooledByteBufAllocator#DEFAULT}, which is what Netty channels use when no allocator is
   *        configured. Installing it on a channel is therefore indistinguishable from not configuring an
   *        allocator at all.
   * @param eventLoopThreadCount the size of the event loop pool the allocator serves. Both arena counts are set
   *        to it so the pool scales with blob transfer's small event loop pool instead of Netty's process-wide
   *        {@code cores * 2} default. Netty gives a thread the least used arena rather than one arena each, so
   *        sizing the arenas to the threads spreads them without reserving arenas no thread will ever use.
   */
  public static PooledByteBufAllocator create(
      String role,
      boolean dedicatedAllocatorEnabled,
      int eventLoopThreadCount) {
    if (!dedicatedAllocatorEnabled) {
      return PooledByteBufAllocator.DEFAULT;
    }

    /**
     * Both arena counts must stay above zero. Netty degrades to unpooled allocation for a buffer type whose
     * arena count is zero, silently and without an error, and blob transfer needs both types pooled: the
     * sender allocates heap buffers, because {@code ChunkedFile#readChunk} and the wrap path of a JDK-backed
     * {@code SslHandler} both call {@code ByteBufAllocator#heapBuffer}, and Netty then converts those heap
     * buffers to direct ones in {@code AbstractNioByteChannel#filterOutboundMessage} before the socket write.
     */
    int arenaCount = Math.max(1, eventLoopThreadCount);

    /**
     * Page size and max order are copied from the defaults so that this allocator's chunk size matches
     * {@link PooledByteBufAllocator#DEFAULT}. Both ends of blob transfer keep the pooling boundary and the
     * huge-allocation threshold they have today, including when the process narrows the chunk through
     * {@code -Dio.netty.allocator.maxOrder}, so enabling a dedicated allocator changes where buffers are
     * allocated from without changing which allocations are poolable.
     */
    PooledByteBufAllocator allocator = new PooledByteBufAllocator(
        PooledByteBufAllocator.defaultPreferDirect(),
        arenaCount,
        arenaCount,
        PooledByteBufAllocator.defaultPageSize(),
        PooledByteBufAllocator.defaultMaxOrder());

    /**
     * The chunk size is fixed for the life of the allocator, so it is logged once here rather than reported as
     * a metric. It is the only way to confirm which pooling boundary a host actually ended up with, since
     * {@code -Dio.netty.allocator.maxOrder} is set per deployment and is easy to leave out of one of them.
     */
    LOGGER.info(
        "Blob transfer {} created a dedicated allocator with {} heap and direct arenas, page size {} B, max order {}, chunk size {} B.",
        role,
        arenaCount,
        PooledByteBufAllocator.defaultPageSize(),
        PooledByteBufAllocator.defaultMaxOrder(),
        allocator.metric().chunkSize());
    return allocator;
  }

  /**
   * Renders the memory charged to {@code allocator} at the moment of the call, or {@code null} when it is not a
   * dedicated blob transfer allocator. {@link PooledByteBufAllocator#DEFAULT} serves every Netty user in the
   * process, so its totals describe the process rather than blob transfer and are not worth logging against a
   * transfer.
   * <p>
   * {@code usedDirectMemory} and {@code usedHeapMemory} count whole reserved chunks rather than the bytes
   * currently in use, which is the figure that matters against {@code -XX:MaxDirectMemorySize}: a chunk occupies
   * its full size for as long as the pool holds it, however little of it is filled.
   * <p>
   * A buffer larger than the chunk size is allocated outside the pool and leaves those two fields as soon as it
   * is released. Blob transfer's payload buffers are larger than the chunk size on a deployment that narrows the
   * chunk through {@code -Dio.netty.allocator.maxOrder}, so a sample taken once a transfer has settled can read
   * close to the baseline even though that transfer allocated steadily while it ran. The huge counters are what
   * record that those allocations happened; the memory fields describe the instant they are read, not a peak.
   */
  public static String describeUsage(ByteBufAllocator allocator) {
    if (!(allocator instanceof PooledByteBufAllocator) || allocator == PooledByteBufAllocator.DEFAULT) {
      return null;
    }
    PooledByteBufAllocatorMetric metric = ((PooledByteBufAllocator) allocator).metric();
    /**
     * The huge counters are cumulative totals of the allocations too large to come from a pool chunk, so a
     * burst of them between two transfers is still visible afterwards. They record that oversized allocations
     * happened rather than how many are outstanding now, and Netty increments them only once the reservation
     * has succeeded, so the kind of allocation that failed for want of memory in production never reaches them.
     */
    return String.format(
        "usedDirectMemory=%d, usedHeapMemory=%d, directHugeAllocationsTotal=%d, heapHugeAllocationsTotal=%d",
        metric.usedDirectMemory(),
        metric.usedHeapMemory(),
        sumHugeAllocations(metric.directArenas()),
        sumHugeAllocations(metric.heapArenas()));
  }

  private static long sumHugeAllocations(List<PoolArenaMetric> arenas) {
    long total = 0;
    for (PoolArenaMetric arena: arenas) {
      total += arena.numHugeAllocations();
    }
    return total;
  }
}
