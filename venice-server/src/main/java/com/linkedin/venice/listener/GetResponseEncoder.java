package com.linkedin.venice.listener;

import com.linkedin.venice.message.GetResponseObject;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Takes a raw bytearray of the value from the storage engine,
 * wraps it in a GetResponseObject and serializes it for the wire
 */
public class GetResponseEncoder extends MessageToByteEncoder<byte[]> {

  @Override
  protected void encode(ChannelHandlerContext ctx, byte[] value, ByteBuf out) throws Exception {
    byte[] response = new GetResponseObject(value).serialize();
    out.writeBytes(response);
  }
}
