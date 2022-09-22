package com.linkedin.alpini.netty4.misc;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.concurrency.AsyncPromise;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 4/28/17.
 */
public class TestChannelTaskSerializer {
  @Test(groups = "unit")
  public void basicTest() {

    EmbeddedChannel channel = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
      ChannelTaskSerializer _serializer;

      @Override
      public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        _serializer = new ChannelTaskSerializer(ctx);
        super.handlerAdded(ctx);
      }

      @Override
      public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        _serializer.executeTask(done -> {
          ChannelPromise innerPromise = ctx.newPromise();
          innerPromise.addListener(future -> {
            if (future.isSuccess()) {
              done.setSuccess();
            } else {
              done.setFailure(future.cause());
            }
          });
          try {
            super.write(ctx, msg, innerPromise);
          } catch (Exception e) {
            innerPromise.setFailure(e);
          }
        }, future -> {
          if (future.isSuccess()) {
            promise.setSuccess();
          } else {
            promise.setFailure(future.cause());
          }
        });
      }
    });

    Object msg = Mockito.mock(Object.class);
    ChannelPromise promise = Mockito.mock(ChannelPromise.class);
    Mockito.when(promise.channel()).thenReturn(channel);

    channel.writeOneOutbound(msg, promise);

    Mockito.verify(promise).isDone();
    Mockito.verify(promise).channel();

    Mockito.verifyNoMoreInteractions(msg, promise);

    Mockito.reset(msg, promise);

    channel.flushOutbound();

    Assert.assertSame(channel.readOutbound(), msg);

    Mockito.verify(promise).setSuccess();

    Mockito.verifyNoMoreInteractions(msg, promise);

  }

  @Test(groups = "unit")
  public void basicTest2() {

    AsyncPromise<Void> paws = AsyncFuture.deferred(false);

    EmbeddedChannel channel = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
      ChannelTaskSerializer _serializer;
      ChannelFuture _afterWrite;

      @Override
      public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        _serializer = new ChannelTaskSerializer(ctx);
        _afterWrite = ctx.newSucceededFuture();
        super.handlerAdded(ctx);
      }

      @Override
      public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        ChannelPromise afterWrite = ctx.newPromise();
        _serializer.executeTask(done -> {
          ChannelPromise innerPromise = ctx.newPromise();
          innerPromise.addListener(future -> {
            if (future.isSuccess()) {
              done.setSuccess();
            } else {
              done.setFailure(future.cause());
            }
          });
          paws.addListener(ignore -> {
            try {
              super.write(ctx, msg, innerPromise);
              super.flush(ctx);
            } catch (Exception e) {
              innerPromise.setFailure(e);
            } finally {
              afterWrite.setSuccess();
            }
          });
        }, future -> {
          if (future.isSuccess()) {
            promise.setSuccess();
          } else {
            promise.setFailure(future.cause());
          }
        });
        _afterWrite = afterWrite.isDone() ? ctx.newSucceededFuture() : afterWrite;
      }

      @Override
      public void flush(ChannelHandlerContext ctx) throws Exception {
        if (_afterWrite.isDone()) {
          super.flush(ctx);
        } else {
          _afterWrite.addListener(ignored -> super.flush(ctx));
        }
      }
    });

    Object msg1 = Mockito.mock(Object.class);
    ChannelPromise promise1 = Mockito.mock(ChannelPromise.class);
    Mockito.when(promise1.channel()).thenReturn(channel);

    Object msg2 = Mockito.mock(Object.class);
    ChannelPromise promise2 = Mockito.mock(ChannelPromise.class);
    Mockito.when(promise2.channel()).thenReturn(channel);

    channel.writeOneOutbound(msg1, promise1);
    channel.writeOneOutbound(msg2, promise2);

    Mockito.verify(promise1).isDone();
    Mockito.verify(promise1).channel();
    Mockito.verify(promise2).isDone();
    Mockito.verify(promise2).channel();

    Mockito.verifyNoMoreInteractions(msg1, promise1, msg2, promise2);

    Mockito.reset(msg1, promise1, msg2, promise2);

    channel.flushOutbound();

    Assert.assertNull(channel.readOutbound());

    paws.setSuccess(null);

    Assert.assertSame(channel.readOutbound(), msg1);
    Assert.assertSame(channel.readOutbound(), msg2);

    Mockito.verify(promise1).setSuccess();
    Mockito.verify(promise2).setSuccess();

    Mockito.verifyNoMoreInteractions(msg1, promise1, msg2, promise2);

  }
}
