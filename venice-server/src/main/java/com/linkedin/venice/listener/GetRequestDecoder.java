package com.linkedin.venice.listener;

import com.linkedin.venice.message.GetRequestObject;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;


/**
 * Monitors the stream, when it gets enough bytes that form a genuine object,
 * it deserialized the object and passes it along the stack.
 *
 * TODO: add a magic byte or something to prevent a random payload size byte from triggering a wait for a very long frame.
 */
public class GetRequestDecoder extends FrameDecoder {
  @Override
  protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer)
      throws Exception {
    if (buffer.readableBytes() < GetRequestObject.HEADER_SIZE){
      return null;
    }
    byte[] header = new byte[GetRequestObject.HEADER_SIZE];
    buffer.getBytes(buffer.readerIndex(), header);
    int payloadSize = GetRequestObject.parseSize(header);
    if (buffer.readableBytes() < payloadSize){
      return null;
    } else {
      ChannelBuffer completeFrame = buffer.readBytes(payloadSize);
      byte[] payload = new byte[payloadSize];
      completeFrame.getBytes(completeFrame.readerIndex(), payload);
      return GetRequestObject.deserialize(payload);
    }

  }
}
