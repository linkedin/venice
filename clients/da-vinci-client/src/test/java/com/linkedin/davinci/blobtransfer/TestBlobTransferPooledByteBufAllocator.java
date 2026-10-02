package com.linkedin.davinci.blobtransfer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.testng.annotations.Test;


public class TestBlobTransferPooledByteBufAllocator {
  @Test
  public void testDisabledReturnsTheProcessWideDefault() {
    assertSame(
        BlobTransferPooledByteBufAllocator.create("sender", false, 8),
        PooledByteBufAllocator.DEFAULT,
        "When disabled the allocator must be the same instance Netty channels use when none is configured, so that "
            + "installing it is indistinguishable from leaving the channel unconfigured.");
  }

  @Test
  public void testEnabledReturnsAnAllocatorThatIsNotSharedWithTheRestOfTheProcess() {
    PooledByteBufAllocator allocator = BlobTransferPooledByteBufAllocator.create("sender", true, 8);
    assertNotSame(allocator, PooledByteBufAllocator.DEFAULT);
    assertNotSame(
        allocator,
        BlobTransferPooledByteBufAllocator.create("sender", true, 8),
        "Each call must produce a separate pool so the sender and the receiver are reported separately.");
  }

  @Test
  public void testEnabledKeepsTheSameChunkSizeAsTheDefault() {
    assertEquals(
        BlobTransferPooledByteBufAllocator.create("sender", true, 8).metric().chunkSize(),
        PooledByteBufAllocator.DEFAULT.metric().chunkSize(),
        "A different chunk size would move the boundary above which allocations stop being poolable, which would "
            + "change behavior rather than only changing which pool the buffers come from.");
  }

  @Test
  public void testEnabledPoolsBothHeapAndDirectBuffers() {
    PooledByteBufAllocator allocator = BlobTransferPooledByteBufAllocator.create("sender", true, 4);
    assertEquals(allocator.metric().numHeapArenas(), 4);
    assertEquals(allocator.metric().numDirectArenas(), 4);

    // Netty silently falls back to unpooled allocation for a buffer type with no arenas, and blob transfer needs both:
    // the sender allocates its chunks on the heap and Netty converts them to direct buffers before the socket write.
    int pooledSize = allocator.metric().chunkSize() / 2;
    ByteBuf heapBuffer = allocator.heapBuffer(pooledSize);
    ByteBuf directBuffer = allocator.directBuffer(pooledSize);
    try {
      assertTrue(
          pooledImplementationOf(heapBuffer).startsWith("Pooled"),
          "Expected a pooled heap buffer but got " + pooledImplementationOf(heapBuffer));
      assertTrue(
          pooledImplementationOf(directBuffer).startsWith("Pooled"),
          "Expected a pooled direct buffer but got " + pooledImplementationOf(directBuffer));
      assertTrue(allocator.metric().usedHeapMemory() > 0);
      assertTrue(allocator.metric().usedDirectMemory() > 0);
    } finally {
      heapBuffer.release();
      directBuffer.release();
    }
  }

  /**
   * Netty's leak detector samples a fraction of allocations and hands back a {@code SimpleLeakAwareByteBuf} wrapper,
   * so the concrete type of an allocation is only visible underneath any wrapper. Which allocations get sampled
   * depends on how many allocations the JVM has already served, making the unwrapped type order-dependent across
   * test classes.
   */
  private static String pooledImplementationOf(ByteBuf buffer) {
    ByteBuf unwrapped = buffer.unwrap();
    return (unwrapped == null ? buffer : unwrapped).getClass().getSimpleName();
  }

  @Test
  public void testEnabledDoesNotReportAllocationsMadeElsewhereInTheProcess() {
    PooledByteBufAllocator allocator = BlobTransferPooledByteBufAllocator.create("sender", true, 2);
    ByteBuf otherBuffer = PooledByteBufAllocator.DEFAULT.directBuffer(allocator.metric().chunkSize() / 2);
    try {
      assertEquals(
          allocator.metric().usedDirectMemory(),
          0,
          "A dedicated allocator is only useful for attribution if buffers allocated by other Netty users in the "
              + "process are not counted against it.");
    } finally {
      otherBuffer.release();
    }
  }

  @Test
  public void testArenaCountIsNeverZero() {
    // A zero arena count would make Netty degrade the allocator to unpooled, defeating both the pooling and the
    // reporting, so an unset or nonsensical event loop thread count must still yield a usable pool.
    for (int eventLoopThreadCount: new int[] { 0, -1 }) {
      PooledByteBufAllocator allocator =
          BlobTransferPooledByteBufAllocator.create("sender", true, eventLoopThreadCount);
      assertEquals(allocator.metric().numHeapArenas(), 1);
      assertEquals(allocator.metric().numDirectArenas(), 1);

      ByteBuf buffer = allocator.directBuffer(allocator.metric().chunkSize() / 2);
      try {
        assertFalse(buffer.getClass().getSimpleName().startsWith("Unpooled"));
      } finally {
        buffer.release();
      }
    }
  }

  @Test
  public void testDescribeUsageIsSilentForAllocatorsThatAreNotBlobTransferSpecific() {
    assertNull(
        BlobTransferPooledByteBufAllocator.describeUsage(PooledByteBufAllocator.DEFAULT),
        "The process-wide default serves every Netty user, so logging its totals against a blob transfer would "
            + "attribute the whole process to blob transfer.");
    assertNull(
        BlobTransferPooledByteBufAllocator.describeUsage(UnpooledByteBufAllocator.DEFAULT),
        "An unpooled allocator keeps no pool metrics to report.");
    assertNull(
        BlobTransferPooledByteBufAllocator.describeUsage(BlobTransferPooledByteBufAllocator.create("sender", false, 8)),
        "A disabled dedicated allocator is the process-wide default, so it must be treated the same way.");
  }

  @Test
  public void testDescribeUsageReportsWhatThisAllocatorHolds() {
    PooledByteBufAllocator allocator = BlobTransferPooledByteBufAllocator.create("receiver", true, 2);
    assertTrue(BlobTransferPooledByteBufAllocator.describeUsage(allocator).contains("usedDirectMemory=0"));

    // Larger than a chunk, so the pool cannot serve it and Netty falls back to a dedicated allocation. This is the
    // shape of the reservation that failed in production, so it has to be distinguishable from pooled usage.
    ByteBuf hugeBuffer = allocator.directBuffer(allocator.metric().chunkSize() * 2);
    try {
      String usage = BlobTransferPooledByteBufAllocator.describeUsage(allocator);
      assertTrue(usage.contains("directHugeAllocationsTotal=1"), usage);
      assertTrue(usage.contains("heapHugeAllocationsTotal=0"), usage);
      assertFalse(
          usage.contains("usedDirectMemory=0"),
          "A huge allocation must still count against the total: " + usage);
    } finally {
      hugeBuffer.release();
    }

    // The huge counter is cumulative, so it keeps reporting the allocation after the memory itself is gone. Pinning
    // that here stops anyone reading it as a count of what the allocator is holding right now.
    String usageAfterRelease = BlobTransferPooledByteBufAllocator.describeUsage(allocator);
    assertTrue(usageAfterRelease.contains("directHugeAllocationsTotal=1"), usageAfterRelease);
    assertTrue(usageAfterRelease.contains("usedDirectMemory=0"), usageAfterRelease);
  }

  @Test
  public void testUsedDirectMemoryBytesIsAttributableToBlobTransferAlone() {
    ByteBuf processWideBuffer = PooledByteBufAllocator.DEFAULT.directBuffer(1024);
    try {
      assertEquals(
          BlobTransferPooledByteBufAllocator.usedDirectMemoryBytes(PooledByteBufAllocator.DEFAULT),
          0L,
          "The process-wide pool serves the read path too, so returning its total would let a blob transfer budget "
              + "be compared against memory blob transfer never allocated.");
      assertEquals(
          BlobTransferPooledByteBufAllocator.usedDirectMemoryBytes(UnpooledByteBufAllocator.DEFAULT),
          0L,
          "An unpooled allocator keeps no pool metrics to read.");
    } finally {
      processWideBuffer.release();
    }

    PooledByteBufAllocator allocator = BlobTransferPooledByteBufAllocator.create("receiver", true, 2);
    ByteBuf buffer = allocator.directBuffer(allocator.metric().chunkSize() * 2);
    try {
      long usedBytes = BlobTransferPooledByteBufAllocator.usedDirectMemoryBytes(allocator);
      assertTrue(usedBytes > 0, "A dedicated allocator holding a buffer must report it, or nothing would decline.");
      // The number a caller compares against a budget and the string a caller logs have to be one reading, or a
      // decline would end up explained by a figure that did not cause it.
      assertTrue(
          BlobTransferPooledByteBufAllocator.describeUsage(allocator).contains("usedDirectMemory=" + usedBytes),
          BlobTransferPooledByteBufAllocator.describeUsage(allocator));
    } finally {
      buffer.release();
    }
  }
}
