package com.linkedin.alpini.netty4.compression;

import com.linkedin.alpini.netty4.handlers.ReverseHandler;
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
public class SnappyEncoder extends ReverseHandler {
  public SnappyEncoder() {
    this(new Snappy());
  }

  private SnappyEncoder(Snappy snappy) {
    super(new ByteToMessageDecoder() {
      private CompositeByteBuf _buffer;

      @Override
      protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (_buffer == null) {
          _buffer = ctx.alloc().compositeBuffer();
        }

        _buffer.addComponents(true, in.readRetainedSlice(in.readableBytes()));

        // Does nothing to accumulate
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

        CompositeByteBuf buffer = ctx.alloc().compositeBuffer();
        try {
          if (input.isReadable()) {
            snappy.encode(input, buffer, input.readableBytes());
            out.add(buffer.retain());
          }
        } finally {
          snappy.reset();
          buffer.release();
          input.release();
        }
      }
    });
  }
}
