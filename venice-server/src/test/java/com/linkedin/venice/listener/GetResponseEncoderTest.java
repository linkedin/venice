package com.linkedin.venice.listener;

import com.linkedin.venice.message.GetResponseObject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GetResponseEncoderTest {

  @Test
  public static void encoderShouldEncode()
      throws Exception {
    ByteBuf out = Unpooled.buffer();
    byte[] value = "myvalue".getBytes();
    GetResponseEncoder encoder = new GetResponseEncoder();
    encoder.encode(null, value, out);
    byte[] r = new byte[out.readableBytes()];
    out.readBytes(r);
    GetResponseObject response = GetResponseObject.deserialize(r);
    Assert.assertEquals(response.getValue(), value);
  }

}
