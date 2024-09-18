package com.linkedin.venice.listener.response;

import com.linkedin.venice.response.VeniceReadResponseStatus;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;


public class BinaryResponseTest {
  @Test
  public void testBinaryResponseBodyAndStatus() {
    // Case 1: Null ByteBuf should default to EMPTY_BUFFER and status KEY_NOT_FOUND
    BinaryResponse responseWithNullByteBuf = new BinaryResponse((ByteBuf) null);
    Assert.assertEquals(responseWithNullByteBuf.getBody(), Unpooled.EMPTY_BUFFER);
    Assert.assertEquals(responseWithNullByteBuf.getStatus(), VeniceReadResponseStatus.KEY_NOT_FOUND);

    // Case 2: Non-null ByteBuf should preserve body and set status to OK
    ByteBuf nonNullByteBuf = Unpooled.wrappedBuffer(new byte[] { 1, 2, 3 });
    BinaryResponse responseWithNonNullByteBuf = new BinaryResponse(nonNullByteBuf);
    Assert.assertEquals(responseWithNonNullByteBuf.getBody(), nonNullByteBuf);
    Assert.assertEquals(responseWithNonNullByteBuf.getStatus(), VeniceReadResponseStatus.OK);

    // Case 3: Null ByteBuffer should default to EMPTY_BUFFER and status KEY_NOT_FOUND
    BinaryResponse responseWithNullByteBuffer = new BinaryResponse((ByteBuffer) null);
    Assert.assertEquals(responseWithNullByteBuffer.getBody(), Unpooled.EMPTY_BUFFER);
    Assert.assertEquals(responseWithNullByteBuffer.getStatus(), VeniceReadResponseStatus.KEY_NOT_FOUND);

    // Case 4: Non-null ByteBuffer should wrap into ByteBuf and set status to OK
    ByteBuffer nonNullByteBuffer = ByteBuffer.wrap(new byte[] { 4, 5, 6 });
    BinaryResponse responseWithNonNullByteBuffer = new BinaryResponse(nonNullByteBuffer);
    Assert.assertEquals(responseWithNonNullByteBuffer.getBody(), Unpooled.wrappedBuffer(nonNullByteBuffer));
    Assert.assertEquals(responseWithNonNullByteBuffer.getStatus(), VeniceReadResponseStatus.OK);
  }
}
