package com.linkedin.venice.listener;

import com.linkedin.venice.message.GetRequestObject;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;


/**
 * Monitors the stream, when it gets enough bytes that form a genuine object,
 * it deserializes the object and passes it along the stack.
 *
 * TODO: add a magic byte or something to prevent a random payload size byte from triggering a wait for a very long frame.
 */
public class GetRequestDecoder extends ByteToMessageDecoder {

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    if (in.readableBytes() < GetRequestObject.HEADER_SIZE){
      return;
    }
    byte[] header = new byte[GetRequestObject.HEADER_SIZE];
    in.getBytes(in.readerIndex(), header);
    int payloadSize = GetRequestObject.parseSize(header);
    if (in.readableBytes() >= payloadSize) {
      ByteBuf completeFrame = in.readBytes(payloadSize);
      byte[] payload = new byte[payloadSize];
      completeFrame.getBytes(completeFrame.readerIndex(), payload);
      out.add(GetRequestObject.deserialize(payload));
    }
  }


}
