package com.linkedin.alpini.netty4.handlers;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;


/**
 * SSL uses 16kb application message packets. This handler aims to combine the ByteBuf into packet-sized chunks,
 * This is due to limitations in the SSL implementation which prefers contiguous messages to process so we must
 * break down composite ByteBuf objects.
 *
 * This Aggregator includes a constructor which accepts a allocator to avoid memory fragmentation.
 * Since we always use 16KB buffers.
 */
public final class OutboundByteBufAggregator extends ChannelOutboundHandlerAdapter {
  public static final int SSL_PACKET_SIZE = 16384;

  private final int _packetSize;
  private final ByteBufAllocator _alloc;
  private final Deque<ByteBuf> _byteBufs = new ArrayDeque<>();
  private final Deque<ChannelPromise> _promises = new ArrayDeque<>();
  private int _accumulatedLength;

  public OutboundByteBufAggregator() {
    this(SSL_PACKET_SIZE);
  }

  public OutboundByteBufAggregator(int packetSize) {
    this(packetSize, null);
  }

  public OutboundByteBufAggregator(int packetSize, ByteBufAllocator alloc) {
    _packetSize = packetSize;
    _alloc = alloc;
  }

  private ByteBufAllocator alloc(ChannelHandlerContext ctx) {
    return _alloc == null ? ctx.alloc() : _alloc;
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    if (msg instanceof ByteBuf) {
      write0(ctx, (ByteBuf) msg, promise);
    } else {
      flush0(ctx, false);
      ctx.write(msg, promise);
    }
  }

  private void write0(ChannelHandlerContext ctx, ByteBuf msg, ChannelPromise promise) throws Exception {
    if (!_byteBufs.isEmpty() || msg instanceof CompositeByteBuf || msg.readableBytes() < _packetSize) {
      _byteBufs.add(msg);
      if (!promise.isVoid() || _promises.stream().noneMatch(ChannelPromise::isVoid)) {
        _promises.add(promise);
      }
      _accumulatedLength += msg.readableBytes();
    } else {
      ctx.write(msg, promise);
    }
    if (_accumulatedLength >= _packetSize) {
      flush0(ctx, false);
    }
  }

  private void compose(ChannelHandlerContext ctx, ByteBuf buf, List<ByteBuf> out) {
    if (out.isEmpty()) {
      out.add(alloc(ctx).buffer(_packetSize));
    }
    ByteBuf last = out.get(out.size() - 1);
    if (last.isWritable(buf.readableBytes())) {
      last.writeBytes(buf);
    } else {
      if (last.isWritable()) {
        last.writeBytes(buf, last.writableBytes());
      }
      if (buf.isReadable()) {
        if (buf.readableBytes() >= _packetSize) {
          out.add(buf.retainedDuplicate().asReadOnly());
        } else {
          ByteBuf next = alloc(ctx).buffer(_packetSize);
          out.add(next);
          next.writeBytes(buf);
        }
      }
    }
    buf.release();
  }

  private boolean isComposite(ByteBuf buf) {
    return buf != null && (buf instanceof CompositeByteBuf || isComposite(buf.unwrap()));
  }

  private void flush0(ChannelHandlerContext ctx, boolean channelFlush) {
    if (_byteBufs.isEmpty()) {
      return;
    }

    if (_byteBufs.size() == 1 && !isComposite(_byteBufs.getFirst())) {
      _accumulatedLength = 0;
      if (channelFlush) {
        ctx.writeAndFlush(_byteBufs.remove(), _promises.remove());
      } else {
        ctx.write(_byteBufs.remove(), _promises.remove());
      }
      return;
    }

    List<ByteBuf> buffers = new ArrayList<>(_byteBufs.size());

    do {
      compose(ctx, _byteBufs.remove(), buffers);
    } while (!_byteBufs.isEmpty());

    ChannelPromise[] promises = _promises.toArray(new ChannelPromise[0]);
    _promises.clear();

    ByteBuf out;
    if (buffers.size() == 1) {
      out = buffers.remove(0);
    } else {
      out = alloc(ctx).compositeBuffer(buffers.size()).addComponents(true, buffers);
    }

    ChannelFuture writeFuture;

    if (channelFlush) {
      if (promises.length == 1) {
        ctx.writeAndFlush(out, promises[0]);
        return;
      }

      writeFuture = ctx.writeAndFlush(out);
    } else {
      if (promises.length == 1) {
        ctx.write(out, promises[0]);
        return;
      }

      writeFuture = ctx.write(out);
    }

    writeFuture.addListener(future -> {
      if (future.isSuccess()) {
        for (ChannelPromise promise: promises) {
          promise.trySuccess();
        }
      } else {
        for (ChannelPromise promise: promises) {
          promise.tryFailure(future.cause());
        }
      }
    });
  }

  @Override
  public void flush(ChannelHandlerContext ctx) throws Exception {
    if (_byteBufs.isEmpty()) {
      super.flush(ctx);
    } else {
      flush0(ctx, true);
    }
  }
}
