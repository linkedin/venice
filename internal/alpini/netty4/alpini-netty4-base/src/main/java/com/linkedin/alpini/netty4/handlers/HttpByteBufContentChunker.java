package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.netty4.misc.HttpMultiPart;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import java.util.List;


/**
 * Created by acurtis on 3/23/17.
 */
public class HttpByteBufContentChunker extends MessageToMessageEncoder<Object> {
  private int _maxChunkSize;
  private int _chunkSize;
  int _chunkOn;

  public HttpByteBufContentChunker(int maxChunkSize, int chunkSize) {
    _maxChunkSize = maxChunkSize;
    _chunkSize = chunkSize;

    if (_maxChunkSize < _chunkSize) {
      throw new IllegalArgumentException("maxChunkSize should be greater than chunkSize");
    }
    if (_chunkSize < 128) {
      throw new IllegalArgumentException("chunkSize should be greater than 128");
    }
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    boolean last = false;
    if (msg instanceof LastHttpContent) {
      last = !(msg instanceof FullHttpMessage || msg instanceof HttpMultiPart);
    } else if (msg instanceof HttpMessage) {
      if (!HttpUtil.isContentLengthSet((HttpMessage) msg) && !(msg instanceof HttpMultiPart)) {
        HttpUtil.setTransferEncodingChunked((HttpMessage) msg, true);
      }
      _chunkOn++;
    }
    try {
      super.write(ctx, msg, promise);
    } finally {
      if (last) {
        _chunkOn--;
      }
    }
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
    if (_chunkOn > 0) {
      if (msg instanceof ByteBuf && encode0(ctx, (ByteBuf) msg, out)) {
        return;
      } else if (msg instanceof FullHttpMessage) {
        //
      } else if (msg instanceof LastHttpContent && encode0(ctx, ((LastHttpContent) msg).content(), out)) {
        out.add(((LastHttpContent) msg).replace(Unpooled.EMPTY_BUFFER));
        return;
      } else if (msg instanceof HttpContent && encode0(ctx, ((HttpContent) msg).content(), out)) {
        return;
      }
    }
    out.add(ReferenceCountUtil.retain(msg));
  }

  private boolean encode0(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    if (in.readableBytes() > _maxChunkSize) {
      do {
        out.add(new DefaultHttpContent(in.readBytes(_chunkSize)));
      } while (in.readableBytes() > _maxChunkSize);
      if (in.isReadable()) {
        out.add(new DefaultHttpContent(in.readBytes(in.readableBytes())));
      } else if (out.isEmpty()) {
        out.add(Unpooled.EMPTY_BUFFER);
      }
      return true;
    }
    return false;
  }
}
