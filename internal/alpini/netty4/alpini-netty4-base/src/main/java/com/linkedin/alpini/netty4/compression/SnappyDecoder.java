package com.linkedin.alpini.netty4.compression;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.compression.Snappy;
import java.util.List;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class SnappyDecoder extends ByteToMessageDecoder {
  private final Snappy _snappy = new Snappy();
  private CompositeByteBuf _buffer;

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
    if (_buffer == null) {
      _buffer = ctx.alloc().compositeBuffer();
    }

    _buffer.addComponents(true, in.readRetainedSlice(in.readableBytes()));

    // does nothing. Allow it to accumulate the whole message
    out.add(Unpooled.EMPTY_BUFFER);
  }

  @Override
  protected void decodeLast(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
    ByteBuf input;

    if (_buffer == null) {
      input = in.readRetainedSlice(in.readableBytes());
    } else {
      _buffer.addComponents(true, in.readRetainedSlice(in.readableBytes()));
      input = _buffer;
      _buffer = null;
    }

    CompositeByteBuf buffer = ctx.alloc().compositeHeapBuffer();
    try {
      if (input.isReadable()) {
        _snappy.decode(input, buffer);
        out.add(buffer.retain());
      }
    } finally {
      _snappy.reset();
      buffer.release();
      input.release();
    }
  }
}
