package com.linkedin.alpini.netty4.handlers;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.PrematureChannelClosureException;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.util.ReferenceCountUtil;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Created by acurtis on 3/21/17.
 */
public class HttpClientResponseHandler extends ChannelDuplexHandler {
  private static final Logger LOG = LogManager.getLogger(HttpClientResponseHandler.class);

  private final BooleanSupplier _idleAutoReadDisable;

  private Queue<Consumer<Object>> _responseConsumers = new LinkedList<>();

  public HttpClientResponseHandler() {
    this(Boolean.FALSE::booleanValue);
  }

  public HttpClientResponseHandler(BooleanSupplier idleAutoReadDisable) {
    assert !isSharable() : "Not sharable";
    _idleAutoReadDisable = Objects.requireNonNull(idleAutoReadDisable, "idleAutoReadDisable");
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    if (msg instanceof HttpRequest) {
      LOG.debug("wrote a {}", msg);
      if (msg instanceof ResponseConsumer) {
        _responseConsumers.add(Objects.requireNonNull(((ResponseConsumer) msg).responseConsumer()));
      } else {
        Exception ex = new IllegalStateException("message does not implement ResponseConsumer");
        LOG.error("bad", ex);
        promise.setFailure(ex);
        return;
      }
      if (promise.isVoid()) {
        promise = ctx.newPromise();
      }
      promise.addListener(writeFuture -> {
        if (writeFuture.isSuccess()) {
          ctx.channel().config().setAutoRead(true);
        }
      });
    }
    super.write(ctx, msg, promise);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    boolean closeChannel = true;
    try {
      // TODO : Uncomment after logging is fixed
      // LOG.debug("received a {}", msg);
      Optional.ofNullable(_responseConsumers.peek()).orElseThrow(() -> {
        LOG.error("Received an unexpected message from {} : {}", ctx.channel().remoteAddress(), msg);
        return new NoSuchElementException("Received an unexpected message: " + msg);
      }).accept(msg);
      closeChannel = false;
    } finally {
      ReferenceCountUtil.release(msg);
      if (msg instanceof LastHttpContent) {
        if (_responseConsumers.poll() == null) {
          closeChannel = true;
        }
      }
      if (closeChannel && ctx.channel().isOpen()) {
        ctx.channel().close().addListener(closeFuture -> {
          if (!closeFuture.isSuccess()) {
            Throwable ex = closeFuture.cause();
            LOG.debug("failure closing connection to {}", ctx.channel().remoteAddress(), ex);
          }
        });
      } else if (_responseConsumers.isEmpty()) {
        // If there are no in-flight requests, we may as well disable auto-read from the connection.
        // It would help to reduce the number of active selector which netty would have to juggle.
        if (_idleAutoReadDisable.getAsBoolean()) {
          ctx.channel().config().setAutoRead(false);
        }
      }
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    List<Consumer<Object>> drain = new ArrayList<>(_responseConsumers.size());
    for (Consumer<Object> consumer = _responseConsumers.poll(); consumer != null; consumer =
        _responseConsumers.poll()) {
      drain.add(consumer);
    }
    try {
      if (ctx.channel().isOpen()) {
        ctx.channel().close();
      }
    } finally {
      drain.forEach(consumer -> {
        try {
          consumer.accept(cause);
        } catch (Throwable ex) {
          LOG.error("consumer threw exception", ex);
        }
      });
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    if (!_responseConsumers.isEmpty()) {
      String msg = ctx.channel() instanceof Http2StreamChannel ? "Closed stream channel" : "Closed parent channel";
      PrematureChannelClosureException ex = new PrematureChannelClosureException(msg);
      do {
        _responseConsumers.remove().accept(ex);
      } while (!_responseConsumers.isEmpty());
    }
    super.channelInactive(ctx);
  }

  public interface ResponseConsumer {
    Consumer<Object> responseConsumer();
  }
}
