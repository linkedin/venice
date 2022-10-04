package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.Msg;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import java.util.function.IntSupplier;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.StringBuilderFormattable;


/**
 * In order to dump the HTTP headers when there is a decode failure, surround the HTTP decode handler with these
 * handles...example:
 *
 * <pre>
 *   InboundContentDebugHandler debugContentHandler = new InboundContentDebugHandler(24 * 1024);
 *
 *   ...
 *
 *   pipeline
 *       .addLast(debugContentHandler)
 *       .addLast(new HttpServerCodec())
 *       .addLast(InboundContentDebugHandler.HttpDecodeResult.INSTANCE)
 *
 *
 * </pre>
 *
 */
@ChannelHandler.Sharable
public class InboundContentDebugHandler extends ChannelInboundHandlerAdapter {
  static final Logger LOG = LogManager.getLogger(InboundContentDebugHandler.class);
  static final AttributeKey<ByteBuf> BUFFER_KEY = AttributeKey.valueOf(InboundContentDebugHandler.class, "buffer");

  private IntSupplier _maxWindowSize;

  public InboundContentDebugHandler(int maxWindowSize) {
    this(() -> maxWindowSize);
  }

  public InboundContentDebugHandler(@Nonnull IntSupplier maxWindowSize) {
    _maxWindowSize = maxWindowSize;
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    if (ctx.channel().hasAttr(BUFFER_KEY)) {
      clearBuffer(ctx.channel());
    }

    super.channelInactive(ctx);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    CompositeByteBuf composite;
    int maxWindowSize;
    if (msg instanceof ByteBuf && (maxWindowSize = _maxWindowSize.getAsInt()) > 0) { // SUPPRESS CHECKSTYLE
                                                                                     // InnerAssignment
      ByteBuf incoming = (ByteBuf) msg;
      Attribute<ByteBuf> attr = ctx.channel().attr(BUFFER_KEY);
      if (attr.get() instanceof CompositeByteBuf) {
        composite = (CompositeByteBuf) attr.get();
      } else {
        composite = ctx.alloc().compositeBuffer();
        ReferenceCountUtil.release(attr.getAndSet(composite));
      }

      composite.addComponent(true, incoming.retain());
      msg = incoming.duplicate();
    } else {
      composite = null;
      maxWindowSize = 0; // Unnecessary but the compiler isn't smart enough.
    }

    super.channelRead(ctx, msg);

    if (composite != null) {
      if (composite.refCnt() > 0 && composite.readableBytes() > maxWindowSize) {
        int index = 0;
        do {
          ByteBuf component = composite.internalComponent(index++);
          if (component.refCnt() > 0 && component.isReadable()) {
            composite.skipBytes(component.readableBytes());
          }
        } while (composite.readableBytes() > maxWindowSize);
        composite.discardSomeReadBytes();
      }
    }
  }

  public static void clearBuffer(Channel channel) {
    ReferenceCountUtil.release(channel.attr(BUFFER_KEY).getAndSet(Unpooled.EMPTY_BUFFER));
  }

  public static ByteBuf fetchLastBytesOf(Channel channel, int length) {
    if (channel.hasAttr(BUFFER_KEY)) {
      ByteBuf buffer = channel.attr(BUFFER_KEY).get();
      if (buffer.isReadable()) {
        if (buffer.readableBytes() < length) {
          return buffer.duplicate();
        } else {
          return buffer.slice(buffer.writerIndex() - length, length);
        }
      }
    }
    return Unpooled.EMPTY_BUFFER;
  }

  @Sharable
  public static class HttpDecodeResult extends ChannelInboundHandlerAdapter {
    public static final HttpDecodeResult INSTANCE = new HttpDecodeResult();

    private HttpDecodeResult() {
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (msg instanceof HttpMessage) {
        HttpMessage message = (HttpMessage) msg;
        // Assume that the client isn't HTTP pipelining requests and that we can clear the buffer...
        if (message.decoderResult().isFailure()) {
          ByteBuf buf = fetchLastBytesOf(ctx.channel(), Integer.MAX_VALUE);
          if (buf.isReadable()) {
            LOG.info(
                "HTTP decode failure: {}",
                Msg.make(buf, InboundContentDebugHandler::formatReadableBytes),
                message.decoderResult().cause());
          }
        }
      }
      super.channelRead(ctx, msg);
      if ((msg instanceof LastHttpContent || msg instanceof HttpMessage)
          && ((HttpObject) msg).decoderResult().isSuccess()) {
        clearBuffer(ctx.channel());
      }
    }

    /*@Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      if (cause instanceof io.netty.handler.codec.TooLongFrameException && ctx.channel().hasAttr(BUFFER_KEY)) {
        ByteBuf lastBytes = ctx.channel().attr(BUFFER_KEY).get();
        if (lastBytes.isReadable()) {
          LOG.info("Exception caught: {}",
              Msg.make(lastBytes.duplicate(), InboundContentDebugHandler::formatReadableBytes),
              cause);
        }
      }
      super.exceptionCaught(ctx, cause);
    }*/
  }

  private static StringBuilderFormattable formatReadableBytes(ByteBuf bytes) {
    return buffer -> {
      buffer.append(bytes.readableBytes()).append(" readable bytes\n");
      ByteBufUtil.appendPrettyHexDump(buffer, bytes);
    };
  }
}
