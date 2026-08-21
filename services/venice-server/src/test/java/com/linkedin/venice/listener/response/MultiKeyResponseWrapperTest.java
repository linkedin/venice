package com.linkedin.venice.listener.response;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.compute.protocol.response.ComputeResponseRecordV1;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class MultiKeyResponseWrapperTest {
  private static final int RECORD_COUNT = 10;
  private static final int CHUNK_COUNT = 5;
  private static final int CHUNK_SIZE = RECORD_COUNT / CHUNK_COUNT;
  /** We always use the same value but it does not matter for the sake of this test... */
  private static final ByteBuffer SERIALIZED_VALUE = ByteBuffer.wrap(new byte[] { 0, 1, 2, 3 });

  /**
   * There are 2 dimensions of 2 possibilities each:
   * - Batch get and compute
   * - Sequential and parallel
   *
   * This leads to 2^2 = 4 permutations.
   */
  @DataProvider(name = "responseWrapperProvider")
  public static Object[][] responseWrapperProvider() {
    MultiGetResponseWrapper multiGetResponseWrapper = new MultiGetResponseWrapper(RECORD_COUNT);
    ComputeResponseWrapper computeResponseWrapper = new ComputeResponseWrapper(RECORD_COUNT);
    ParallelMultiKeyResponseWrapper parallelMultiGetResponseWrapper =
        ParallelMultiKeyResponseWrapper.multiGet(CHUNK_COUNT, CHUNK_SIZE, s -> new MultiGetResponseWrapper(s));
    ParallelMultiKeyResponseWrapper parallelComputeResponseWrapper =
        ParallelMultiKeyResponseWrapper.compute(CHUNK_COUNT, CHUNK_SIZE, s -> new ComputeResponseWrapper(s));
    int multiGetSerializedSize = 0;
    int computeSerializedSize = 0;
    for (int i = 0; i < RECORD_COUNT; i++) {
      MultiGetResponseRecordV1 multiGetResponseRecord = new MultiGetResponseRecordV1(i, SERIALIZED_VALUE, 1);
      ComputeResponseRecordV1 computeResponseRecord = new ComputeResponseRecordV1(i, SERIALIZED_VALUE);
      multiGetSerializedSize += MultiGetResponseWrapper.SERIALIZER.serialize(multiGetResponseRecord).length;
      computeSerializedSize += ComputeResponseWrapper.SERIALIZER.serialize(computeResponseRecord).length;
      multiGetResponseWrapper.addRecord(multiGetResponseRecord);
      computeResponseWrapper.addRecord(computeResponseRecord);
      int chunkIndex = i % CHUNK_COUNT;
      parallelMultiGetResponseWrapper.getChunk(chunkIndex).addRecord(multiGetResponseRecord);
      parallelComputeResponseWrapper.getChunk(chunkIndex).addRecord(computeResponseRecord);
    }

    return new Object[][] {
        /** {@link MultiGetResponseWrapper} */
        { multiGetResponseWrapper, multiGetSerializedSize },
        /** {@link ComputeResponseWrapper} */
        { computeResponseWrapper, computeSerializedSize },
        /** {@link ParallelMultiKeyResponseWrapper} containing {@link MultiGetResponseWrapper} chunks */
        { parallelMultiGetResponseWrapper, multiGetSerializedSize },
        /** {@link ParallelMultiKeyResponseWrapper} containing {@link ComputeResponseWrapper} chunks */
        { parallelComputeResponseWrapper, computeSerializedSize } };
  }

  /**
   * N.B.: Depending on which {@link io.netty.buffer.CompositeByteBuf} factory method is used, the
   * {@link ByteBuf#readableBytes()} may be incorrect. This unit test ensures we do it right.
   */
  @Test(dataProvider = "responseWrapperProvider")
  public void testResponseBodySize(AbstractReadResponse responseWrapper, int expectedSerializedSize) {
    assertNotNull(responseWrapper);
    ByteBuf responseBody = responseWrapper.getResponseBody();
    assertNotNull(responseBody);
    assertEquals(responseBody.readableBytes(), expectedSerializedSize);
  }
}
