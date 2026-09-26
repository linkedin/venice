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
 * Netty channels allocate from the process-wide {@link PooledByteBufAllocator#DEFAULT} unless given their own, so
 * blob transfer's usage is otherwise indistinguishable from every other Netty user in the process. A dedicated
 * pool makes {@code allocator.metric().usedDirectMemory()} attributable to blob transfer alone. It covers only
 * buffers that come from this allocator, so the metadata path's {@code byte[]} and {@code Unpooled} buffers are
 * not counted.
 */
public final class BlobTransferPooledByteBufAllocator {
  private static final Logger LOGGER = LogManager.getLogger(BlobTransferPooledByteBufAllocator.class);

  private BlobTransferPooledByteBufAllocator() {
  }

  /**
   * @param role which end of blob transfer owns the allocator, so the chunk size logged below is attributable.
   * @param dedicatedAllocatorEnabled when {@code false}, returns {@link PooledByteBufAllocator#DEFAULT}, which is
   *        what a channel uses when no allocator is configured, so installing it changes nothing.
   * @param eventLoopThreadCount the size of the event loop pool the allocator serves. Both arena counts are set to
   *        it so the pool scales with blob transfer's event loop pool rather than Netty's process-wide
   *        {@code cores * 2}. Netty hands a thread the least used arena rather than one arena each, so sizing the
   *        arenas to the threads spreads them without reserving arenas no thread will use.
   */
  public static PooledByteBufAllocator create(
      String role,
      boolean dedicatedAllocatorEnabled,
      int eventLoopThreadCount) {
    if (!dedicatedAllocatorEnabled) {
      return PooledByteBufAllocator.DEFAULT;
    }

    // Both arena counts must stay above zero: Netty silently degrades a buffer type with zero arenas to unpooled
    // allocation, and blob transfer needs both pooled. The sender allocates heap buffers, because ChunkedFile#readChunk
    // and a JDK-backed SslHandler's wrap path both call heapBuffer(), which Netty then converts to direct buffers in
    // AbstractNioByteChannel#filterOutboundMessage before the socket write.
    int arenaCount = Math.max(1, eventLoopThreadCount);

    // Page size and max order are copied from the defaults so this allocator's chunk size, and therefore which
    // allocations are poolable, matches PooledByteBufAllocator.DEFAULT, including when the process narrows the chunk
    // through -Dio.netty.allocator.maxOrder.
    PooledByteBufAllocator allocator = new PooledByteBufAllocator(
        PooledByteBufAllocator.defaultPreferDirect(),
        arenaCount,
        arenaCount,
        PooledByteBufAllocator.defaultPageSize(),
        PooledByteBufAllocator.defaultMaxOrder());

    // The chunk size is fixed for the life of the allocator, and logging it is the only way to confirm which pooling
    // boundary a host ended up with, since -Dio.netty.allocator.maxOrder is set per deployment.
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
   * Whether {@code allocator} is a pool this class created rather than {@link PooledByteBufAllocator#DEFAULT}, which
   * serves every Netty user in the process and whose totals therefore say nothing about blob transfer. Anything that
   * attributes a reading to blob transfer has to ask this first.
   */
  public static boolean isDedicated(ByteBufAllocator allocator) {
    return allocator instanceof PooledByteBufAllocator && allocator != PooledByteBufAllocator.DEFAULT;
  }

  /**
   * The direct memory charged to {@code allocator} at the moment of the call, or zero when it is not a dedicated blob
   * transfer allocator and the figure would describe the whole process rather than blob transfer.
   * <p>
   * This is the same reading {@link #describeUsage} renders, exposed as a number for callers that compare it against
   * a budget instead of logging it.
   */
  public static long usedDirectMemoryBytes(ByteBufAllocator allocator) {
    return isDedicated(allocator) ? ((PooledByteBufAllocator) allocator).metric().usedDirectMemory() : 0L;
  }

  /**
   * Renders the memory charged to {@code allocator} at the moment of the call, or {@code null} when it is not a
   * dedicated blob transfer allocator, since {@link PooledByteBufAllocator#DEFAULT} describes the whole process.
   * <p>
   * {@code usedDirectMemory} and {@code usedHeapMemory} count whole reserved chunks rather than the bytes in use,
   * which is the figure that matters against {@code -XX:MaxDirectMemorySize}. A buffer larger than the chunk size is
   * allocated outside the pool and leaves both fields as soon as it is released, so on a deployment that narrows the
   * chunk through {@code -Dio.netty.allocator.maxOrder} a sample taken once a transfer has settled can read close to
   * the baseline: these are instantaneous samples, not peaks.
   */
  public static String describeUsage(ByteBufAllocator allocator) {
    if (!isDedicated(allocator)) {
      return null;
    }
    PooledByteBufAllocatorMetric metric = ((PooledByteBufAllocator) allocator).metric();
    // The huge counters are cumulative, so they record that oversized allocations happened rather than how many are
    // outstanding. Netty increments them only once the reservation has succeeded, so an allocation that failed for
    // want of memory never reaches them.
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
