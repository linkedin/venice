package com.linkedin.venice.listener.response;

import com.linkedin.davinci.listener.response.ReadResponseStats;
import com.linkedin.venice.listener.response.stats.ReadResponseStatsRecorder;
import com.linkedin.venice.stats.ServerHttpRequestStats;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.function.IntFunction;


public class ParallelMultiKeyResponseWrapper<T extends MultiKeyResponseWrapper> extends AbstractReadResponse {
  private final T[] chunks;

  private ParallelMultiKeyResponseWrapper(
      int chunkCount,
      int chunkSize,
      T[] chunks,
      IntFunction<T> multiGetResponseProvider) {
    this.chunks = chunks;
    for (int i = 0; i < chunkCount; i++) {
      this.chunks[i] = multiGetResponseProvider.apply(chunkSize);
    }
  }

  public static ParallelMultiKeyResponseWrapper<MultiGetResponseWrapper> multiGet(
      int chunkCount,
      int chunkSize,
      IntFunction<MultiGetResponseWrapper> responseProvider) {
    return new ParallelMultiKeyResponseWrapper<>(
        chunkCount,
        chunkSize,
        new MultiGetResponseWrapper[chunkCount],
        responseProvider);
  }

  public static ParallelMultiKeyResponseWrapper<ComputeResponseWrapper> compute(
      int chunkCount,
      int chunkSize,
      IntFunction<ComputeResponseWrapper> responseProvider) {
    return new ParallelMultiKeyResponseWrapper<>(
        chunkCount,
        chunkSize,
        new ComputeResponseWrapper[chunkCount],
        responseProvider);
  }

  public T getChunk(int chunkIndex) {
    return this.chunks[chunkIndex];
  }

  /**
   * N.B.: Only the individual chunks should be passed into code paths that require this API. If some refactoring causes
   *       that assumption to change, then we have a regression.
   */
  @Override
  public ReadResponseStats getStats() {
    throw new IllegalStateException(this.getClass().getSimpleName() + " does not support accumulating metrics.");
  }

  @Override
  public ReadResponseStatsRecorder getStatsRecorder() {
    return new CompositeReadResponseStatsRecorder(this.chunks);
  }

  @Override
  public ByteBuf getResponseBody() {
    ByteBuf[] byteBufChunks = new ByteBuf[chunks.length];
    for (int i = 0; i < chunks.length; i++) {
      byteBufChunks[i] = this.chunks[i].getResponseBody();
    }
    return Unpooled.wrappedBuffer(byteBufChunks);
  }

  @Override
  public int getResponseSchemaIdHeader() {
    return this.chunks[0].getResponseSchemaIdHeader();
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "(" + this.chunks.length + " " + this.chunks[0].getClass().getSimpleName()
        + " chunks)";
  }

  private static final class CompositeReadResponseStatsRecorder implements ReadResponseStatsRecorder {
    /** The aggregated stats of all the chunks (for stats which can be aggregated). */
    private final ReadResponseStatsRecorder mergedStats;

    /**
     * This array references the {@link ReadResponseStatsRecorder} of all response chunks beyond the 1st one (for stats
     * which cannot be aggregated).
     */
    private final ReadResponseStatsRecorder[] statsRecorders;

    CompositeReadResponseStatsRecorder(MultiKeyResponseWrapper[] responseChunks) {
      this.mergedStats = responseChunks[0].getStatsRecorder();

      /**
       * This array can be one element shorter than {@param responseChunks} because the first chunk's storage exec sub
       * wait time will be recorded as part of the {@link mergedStats}.
       */
      this.statsRecorders = new ReadResponseStatsRecorder[responseChunks.length - 1];
      ReadResponseStatsRecorder statsRecorder;
      for (int i = 1; i < responseChunks.length; i++) {
        statsRecorder = responseChunks[i].getStatsRecorder();
        // We merge the stats of all chunks from the 2nd one to the last one into the stats of the 1st chunk.
        this.mergedStats.merge(statsRecorder);

        /**
         * We also keep a handle to each {@link ReadResponseStatsRecorder} beyond the 1st so that later on we can call
         * {@link #recordUnmergedMetrics(ServerHttpRequestStats)} on them. */
        this.statsRecorders[i - 1] = statsRecorder;
      }
    }

    @Override
    public void recordMetrics(ServerHttpRequestStats stats) {
      this.mergedStats.recordMetrics(stats);
      recordUnmergedMetrics(stats);
    }

    @Override
    public void recordUnmergedMetrics(ServerHttpRequestStats stats) {
      for (int i = 0; i < this.statsRecorders.length; i++) {
        this.statsRecorders[i].recordUnmergedMetrics(stats);
      }
    }

    @Override
    public void merge(ReadResponseStatsRecorder other) {
    }
  }
}
