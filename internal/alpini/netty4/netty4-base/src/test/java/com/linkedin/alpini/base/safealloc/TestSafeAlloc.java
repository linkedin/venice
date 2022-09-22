package com.linkedin.alpini.base.safealloc;

import com.linkedin.alpini.base.misc.Time;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.internal.PlatformDependent;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestSafeAlloc {
  public void testAlloc(SafeAllocator allocator, String text, int active, long leaked, boolean release) {
    ByteBuf buf = ByteBufUtil.writeAscii(allocator, text);

    if (PlatformDependent.directBufferPreferred()) {
      Assert.assertEquals(allocator.metric().usedDirectMemory(), 16777216);
      Assert.assertEquals(allocator.metric().usedHeapMemory(), 0);
    } else {
      Assert.assertEquals(allocator.metric().usedDirectMemory(), 0);
      Assert.assertEquals(allocator.metric().usedHeapMemory(), 16777216);
    }
    Assert.assertEquals(allocator.metric().activeAllocations(), active);
    Assert.assertEquals(allocator.metric().leakedAllocations(), leaked);

    if (release) {
      buf.release();
    } else {
      buf.retain(100);
      Assert.assertEquals(buf.refCnt(), 101);
    }
  }

  @Test(groups = "unit")
  public void testBasic() throws InterruptedException {

    PooledByteBufAllocator store = new PooledByteBufAllocator(false);
    SafeAllocator allocator = new SafeAllocator(store);

    testAlloc(allocator, "Hello world", 1, 0, false);
    Assert.assertEquals(allocator.metric().totalReferences(), 1);

    System.gc();
    Time.sleep(1000L);
    System.gc();

    testAlloc(allocator, "Goodbye world", 1, 1, true);
    Assert.assertEquals(allocator.metric().totalReferences(), 2);

    System.gc();
    Time.sleep(1000L);
    System.gc();

    Assert.assertEquals(allocator.metric().activeAllocations(), 0);
    Assert.assertEquals(allocator.metric().leakedAllocations(), 1);
    Assert.assertEquals(allocator.metric().totalReferences(), 2);
    Assert.assertEquals(allocator.metric().totalQueues(), 1);
  }

}
