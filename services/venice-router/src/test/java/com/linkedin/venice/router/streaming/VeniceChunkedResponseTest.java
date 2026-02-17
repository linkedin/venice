package com.linkedin.venice.router.streaming;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import org.testng.Assert;
import org.testng.annotations.Test;


public class VeniceChunkedResponseTest {
  @Test
  public void whetherToSkipMessage() {
    VeniceChunkedWriteHandler chunkedWriteHandler = new VeniceChunkedWriteHandler();
    /** The {@link VeniceChunkedResponse} is only instantiated so that it registers its callback into the handler */
    new VeniceChunkedResponse(
        "storeName",
        RequestType.MULTI_GET_STREAMING,
        mock(ChannelHandlerContext.class),
        chunkedWriteHandler,
        mock(RouterStats.class),
        null);
    assertThrows(
        // This used to throw a NPE as part of a previous regression, we want a better error message
        VeniceException.class,
        () -> chunkedWriteHandler.write(mock(ChannelHandlerContext.class), null, mock(ChannelPromise.class)));
  }

  /**
   * Test that ChunkDispenser.readChunk() retains the buffer before passing to Netty.
   * This ensures that both Netty and resolveChunk() can safely release the buffer without
   * causing IllegalReferenceCountException during client disconnects.
   *
   * The flow is:
   * 1. Buffer starts with refCnt=1
   * 2. readChunk() calls retain() -> refCnt=2
   * 3. Netty releases after write complete/fail -> refCnt=1
   * 4. resolveChunk() releases -> refCnt=0
   */
  @Test
  public void testReadChunkRetainsBufferBeforePassingToNetty() throws Exception {
    // Setup mocks
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    Channel mockChannel = mock(Channel.class);
    ChannelProgressivePromise mockPromise = mock(ChannelProgressivePromise.class);
    RouterStats<AggRouterHttpRequestStats> mockRouterStats = mock(RouterStats.class);
    AggRouterHttpRequestStats mockStats = mock(AggRouterHttpRequestStats.class);

    when(mockCtx.channel()).thenReturn(mockChannel);
    when(mockChannel.isOpen()).thenReturn(true);
    when(mockCtx.newProgressivePromise()).thenReturn(mockPromise);
    when(mockRouterStats.getStatsByType(RequestType.MULTI_GET_STREAMING)).thenReturn(mockStats);

    VeniceChunkedWriteHandler chunkedWriteHandler = new VeniceChunkedWriteHandler();
    VeniceChunkedResponse response = new VeniceChunkedResponse(
        "testStore",
        RequestType.MULTI_GET_STREAMING,
        mockCtx,
        chunkedWriteHandler,
        mockRouterStats,
        null);

    // Create a buffer
    ByteBuf buffer = Unpooled.wrappedBuffer(new byte[] { 1, 2, 3 });
    Assert.assertEquals(buffer.refCnt(), 1, "Buffer should have refCnt=1 initially");

    // Use reflection to access the private Chunk class and create an instance
    Class<?> chunkClass = Class.forName("com.linkedin.venice.router.streaming.VeniceChunkedResponse$Chunk");
    Constructor<?> chunkConstructor = chunkClass
        .getDeclaredConstructor(VeniceChunkedResponse.class, ByteBuf.class, boolean.class, StreamingCallback.class);
    chunkConstructor.setAccessible(true);
    Object chunk = chunkConstructor.newInstance(response, buffer, false, null);

    // Access the chunksToWrite queue and add the chunk
    java.lang.reflect.Field chunksToWriteField = VeniceChunkedResponse.class.getDeclaredField("chunksToWrite");
    chunksToWriteField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Queue<Object> chunksToWrite = (java.util.Queue<Object>) chunksToWriteField.get(response);
    chunksToWrite.add(chunk);

    // Access the ChunkDispenser class and call readChunk
    Class<?> chunkDispenserClass =
        Class.forName("com.linkedin.venice.router.streaming.VeniceChunkedResponse$ChunkDispenser");
    Constructor<?> dispenserConstructor = chunkDispenserClass.getDeclaredConstructor(VeniceChunkedResponse.class);
    dispenserConstructor.setAccessible(true);
    Object dispenser = dispenserConstructor.newInstance(response);

    Method readChunkMethod = chunkDispenserClass.getDeclaredMethod("readChunk", io.netty.buffer.ByteBufAllocator.class);
    readChunkMethod.setAccessible(true);

    // Call readChunk - this should retain the buffer
    Object httpContent = readChunkMethod.invoke(dispenser, io.netty.buffer.UnpooledByteBufAllocator.DEFAULT);
    Assert.assertNotNull(httpContent, "HttpContent should not be null");

    // After readChunk, buffer should have refCnt=2 (original + retained for Netty)
    Assert.assertEquals(buffer.refCnt(), 2, "Buffer should have refCnt=2 after readChunk retains it");

    // Simulate Netty releasing the buffer after write
    buffer.release();
    Assert.assertEquals(buffer.refCnt(), 1, "Buffer should have refCnt=1 after Netty releases");

    // Now resolveChunk should be able to release safely
    Method resolveChunkMethod = chunkClass.getDeclaredMethod("resolveChunk", Exception.class);
    resolveChunkMethod.setAccessible(true);
    resolveChunkMethod.invoke(chunk, (Exception) null);

    // Buffer should be fully released now
    Assert.assertEquals(buffer.refCnt(), 0, "Buffer should have refCnt=0 after resolveChunk releases");
  }

  /**
   * Test that when a chunk is skipped (not passed to Netty), the buffer is correctly released
   * without going through the retain() path.
   *
   * This scenario occurs when maybeAddChunk() is called after responseCompleteCalled is true.
   * In this case:
   * 1. Buffer starts with refCnt=1
   * 2. readChunk() is never called, so no retain()
   * 3. resolveChunk() is called directly in maybeAddChunk() -> releases buffer -> refCnt=0
   *
   * This verifies that skipped chunks don't leak memory and don't cause double-release issues.
   */
  @Test
  public void testSkippedChunkReleasesBufferCorrectly() throws Exception {
    // Setup mocks
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    Channel mockChannel = mock(Channel.class);
    ChannelProgressivePromise mockPromise = mock(ChannelProgressivePromise.class);
    RouterStats<AggRouterHttpRequestStats> mockRouterStats = mock(RouterStats.class);
    AggRouterHttpRequestStats mockStats = mock(AggRouterHttpRequestStats.class);

    when(mockCtx.channel()).thenReturn(mockChannel);
    when(mockChannel.isOpen()).thenReturn(true);
    when(mockCtx.newProgressivePromise()).thenReturn(mockPromise);
    when(mockRouterStats.getStatsByType(RequestType.MULTI_GET_STREAMING)).thenReturn(mockStats);

    VeniceChunkedWriteHandler chunkedWriteHandler = new VeniceChunkedWriteHandler();
    VeniceChunkedResponse response = new VeniceChunkedResponse(
        "testStore",
        RequestType.MULTI_GET_STREAMING,
        mockCtx,
        chunkedWriteHandler,
        mockRouterStats,
        null);

    // Set responseCompleteCalled to true using reflection to simulate the scenario
    // where the response has already been completed
    java.lang.reflect.Field responseCompleteField =
        VeniceChunkedResponse.class.getDeclaredField("responseCompleteCalled");
    responseCompleteField.setAccessible(true);
    responseCompleteField.set(response, true);

    // Create a buffer
    ByteBuf buffer = Unpooled.wrappedBuffer(new byte[] { 1, 2, 3 });
    Assert.assertEquals(buffer.refCnt(), 1, "Buffer should have refCnt=1 initially");

    // Use reflection to access the private Chunk class and create an instance
    Class<?> chunkClass = Class.forName("com.linkedin.venice.router.streaming.VeniceChunkedResponse$Chunk");
    Constructor<?> chunkConstructor = chunkClass
        .getDeclaredConstructor(VeniceChunkedResponse.class, ByteBuf.class, boolean.class, StreamingCallback.class);
    chunkConstructor.setAccessible(true);
    Object chunk = chunkConstructor.newInstance(response, buffer, false, null);

    // Call maybeAddChunk - since responseCompleteCalled is true, it should skip the chunk
    // and call resolveChunk() directly, which releases the buffer
    Method maybeAddChunkMethod = VeniceChunkedResponse.class.getDeclaredMethod("maybeAddChunk", chunkClass);
    maybeAddChunkMethod.setAccessible(true);
    boolean added = (boolean) maybeAddChunkMethod.invoke(response, chunk);

    // Verify chunk was skipped (not added to queue)
    Assert.assertFalse(added, "Chunk should be skipped when responseCompleteCalled is true");

    // Buffer should be released by resolveChunk() called in maybeAddChunk()
    // Since readChunk() was never called, no retain() happened, so only one release is needed
    Assert.assertEquals(buffer.refCnt(), 0, "Buffer should have refCnt=0 after skipped chunk is resolved");

    // Verify the chunksToWrite queue is empty (chunk was not added)
    java.lang.reflect.Field chunksToWriteField = VeniceChunkedResponse.class.getDeclaredField("chunksToWrite");
    chunksToWriteField.setAccessible(true);
    @SuppressWarnings("unchecked")
    java.util.Queue<Object> chunksToWrite = (java.util.Queue<Object>) chunksToWriteField.get(response);
    Assert.assertTrue(chunksToWrite.isEmpty(), "chunksToWrite should be empty when chunk is skipped");
  }
}
