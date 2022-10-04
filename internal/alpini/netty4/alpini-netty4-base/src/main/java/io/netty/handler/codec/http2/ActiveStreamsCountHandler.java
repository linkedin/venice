package io.netty.handler.codec.http2;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;
import java.util.concurrent.atomic.LongAdder;


/**
 * Counts the number of the active HTTP/2 Streams
 *
 * @author Abhishek Andhavarapu
 */
@ChannelHandler.Sharable
public class ActiveStreamsCountHandler extends ChannelInboundHandlerAdapter {
  private final AttributeKey<Count> _countKey = AttributeKey.newInstance(toString() + "key");

  static final class Count {
    private int streams;

    public void increment() {
      streams++;
    }

    public void decrement() {
      streams--;
    }

    public int intValue() {
      return streams;
    }
  }

  private final LongAdder _activeCount = new LongAdder();

  /**
   * Return the current active streams.
   * @return number of streams.
   */
  public int getActiveStreamsCount() {
    return _activeCount.intValue();
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof Http2FrameStreamEvent) {
      Http2FrameStreamEvent event = (Http2FrameStreamEvent) evt;
      Http2FrameCodec.DefaultHttp2FrameStream stream = (Http2FrameCodec.DefaultHttp2FrameStream) event.stream();
      Count count;
      if (event.type() == Http2FrameStreamEvent.Type.State && (count = ctx.channel().attr(_countKey).get()) != null) { // SUPPRESS
                                                                                                                       // CHECKSTYLE
                                                                                                                       // InnerAssignment
        switch (stream.state()) {
          case OPEN:
            count.increment();
            _activeCount.increment();
            break;
          case CLOSED:
            count.decrement();
            _activeCount.decrement();
            break;
          default:
            // ignore for now
            break;
        }
      }
    }
    ctx.fireUserEventTriggered(evt);
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    super.handlerAdded(ctx);
    ctx.channel().attr(_countKey).set(new Count());
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
    Count count = ctx.channel().attr(_countKey).getAndSet(null);
    if (count != null) {
      _activeCount.add(-count.intValue());
    }
    super.handlerRemoved(ctx);
  }
}
