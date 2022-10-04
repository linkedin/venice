package com.linkedin.alpini.netty4.handlers;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 4/23/18.
 */
public class TestLog4J2LoggingHandler {
  private final Logger _mockLogger = Mockito.mock(Logger.class);

  @Test(groups = "unit")
  public void testConstruct() throws Exception {
    Assert.assertNotNull(new Log4J2LoggingHandler());
  }

  @Test(groups = "unit")
  public void testChannelRegistered() throws Exception {
    Mockito.reset(_mockLogger);

    class Harness extends Log4J2LoggingHandler {
      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    Log4J2LoggingHandler handler = new Harness();

    Channel ch = Mockito.mock(Channel.class, "mockChannel");
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Mockito.when(ctx.channel()).thenReturn(ch);

    handler.channelRegistered(ctx);

    Mockito.verify(_mockLogger).isEnabled(Mockito.eq(Level.DEBUG));
    ArgumentCaptor<Object> message = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(_mockLogger).log(Mockito.eq(Level.DEBUG), message.capture());
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Assert.assertEquals(String.valueOf(message.getValue()), "mockChannel REGISTERED");
    Mockito.verify(ctx).channel();
    Mockito.verify(ctx).fireChannelRegistered();
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testChannelUnregistered() throws Exception {
    Mockito.reset(_mockLogger);

    class Harness extends Log4J2LoggingHandler {
      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    Log4J2LoggingHandler handler = new Harness();

    Channel ch = Mockito.mock(Channel.class, "mockChannel");
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Mockito.when(ctx.channel()).thenReturn(ch);

    handler.channelUnregistered(ctx);

    Mockito.verify(_mockLogger).isEnabled(Mockito.eq(Level.DEBUG));
    ArgumentCaptor<Object> message = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(_mockLogger).log(Mockito.eq(Level.DEBUG), message.capture());
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Assert.assertEquals(String.valueOf(message.getValue()), "mockChannel UNREGISTERED");
    Mockito.verify(ctx).channel();
    Mockito.verify(ctx).fireChannelUnregistered();
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testChannelActive() throws Exception {
    Mockito.reset(_mockLogger);

    class Harness extends Log4J2LoggingHandler {
      private Harness() {
        super(TestLog4J2LoggingHandler.class);
      }

      @Override
      Logger setupLogger(Logger logger) {
        Assert.assertEquals(logger.getName(), TestLog4J2LoggingHandler.class.getName());
        return _mockLogger;
      }
    }

    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    Log4J2LoggingHandler handler = new Harness();

    Channel ch = Mockito.mock(Channel.class, "mockChannel");
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Mockito.when(ctx.channel()).thenReturn(ch);

    handler.channelActive(ctx);

    Mockito.verify(_mockLogger).isEnabled(Mockito.eq(Level.DEBUG));
    ArgumentCaptor<Object> message = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(_mockLogger).log(Mockito.eq(Level.DEBUG), message.capture());
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Assert.assertEquals(String.valueOf(message.getValue()), "mockChannel ACTIVE");
    Mockito.verify(ctx).channel();
    Mockito.verify(ctx).fireChannelActive();
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testChannelInactive() throws Exception {
    Mockito.reset(_mockLogger);

    class Harness extends Log4J2LoggingHandler {
      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    Log4J2LoggingHandler handler = new Harness();

    Channel ch = Mockito.mock(Channel.class, "mockChannel");
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Mockito.when(ctx.channel()).thenReturn(ch);

    handler.channelInactive(ctx);

    Mockito.verify(_mockLogger).isEnabled(Mockito.eq(Level.DEBUG));
    ArgumentCaptor<Object> message = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(_mockLogger).log(Mockito.eq(Level.DEBUG), message.capture());
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Assert.assertEquals(String.valueOf(message.getValue()), "mockChannel INACTIVE");
    Mockito.verify(ctx).channel();
    Mockito.verify(ctx).fireChannelInactive();
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testExceptionCaught() throws Exception {
    Mockito.reset(_mockLogger);

    class Harness extends Log4J2LoggingHandler {
      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    class TestEx extends Error {
    }

    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    Log4J2LoggingHandler handler = new Harness();

    Channel ch = Mockito.mock(Channel.class, "mockChannel");
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Mockito.when(ctx.channel()).thenReturn(ch);

    handler.exceptionCaught(ctx, new TestEx());

    Mockito.verify(_mockLogger).isEnabled(Mockito.eq(Level.DEBUG));
    ArgumentCaptor<Object> message = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(_mockLogger).log(Mockito.eq(Level.DEBUG), message.capture(), Mockito.notNull(TestEx.class));
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Assert.assertEquals(
        String.valueOf(message.getValue()),
        "mockChannel EXCEPTION: com.linkedin.alpini.netty4.handlers.TestLog4J2LoggingHandler$1TestEx");
    Mockito.verify(ctx).channel();
    Mockito.verify(ctx).fireExceptionCaught(Mockito.notNull(TestEx.class));
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testUserEvent() throws Exception {
    Mockito.reset(_mockLogger);

    class Harness extends Log4J2LoggingHandler {
      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    class TestEvent {
      @Override
      public String toString() {
        return "TestEvent{}";
      }
    }

    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    Log4J2LoggingHandler handler = new Harness();

    Channel ch = Mockito.mock(Channel.class, "mockChannel");
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Mockito.when(ctx.channel()).thenReturn(ch);

    handler.userEventTriggered(ctx, new TestEvent());

    Mockito.verify(_mockLogger).isEnabled(Mockito.eq(Level.DEBUG));
    ArgumentCaptor<Object> message = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(_mockLogger).log(Mockito.eq(Level.DEBUG), message.capture());
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Assert.assertEquals(String.valueOf(message.getValue()), "mockChannel USER_EVENT: TestEvent{}");
    Mockito.verify(ctx).channel();
    Mockito.verify(ctx).fireUserEventTriggered(Mockito.notNull(TestEvent.class));
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testBind() throws Exception {
    Mockito.reset(_mockLogger);

    class Harness extends Log4J2LoggingHandler {
      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    Log4J2LoggingHandler handler = new Harness();

    Channel ch = Mockito.mock(Channel.class, "mockChannel");
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Mockito.when(ctx.channel()).thenReturn(ch);

    SocketAddress localAddress = Mockito.mock(SocketAddress.class, "localAddress");
    ChannelPromise promise = Mockito.mock(ChannelPromise.class);

    handler.bind(ctx, localAddress, promise);

    Mockito.verify(_mockLogger).isEnabled(Mockito.eq(Level.DEBUG));
    ArgumentCaptor<Object> message = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(_mockLogger).log(Mockito.eq(Level.DEBUG), message.capture());
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Assert.assertEquals(String.valueOf(message.getValue()), "mockChannel BIND: localAddress");
    Mockito.verify(ctx).channel();
    Mockito.verify(ctx).bind(Mockito.eq(localAddress), Mockito.eq(promise));
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testConnect2() throws Exception {
    Mockito.reset(_mockLogger);

    class Harness extends Log4J2LoggingHandler {
      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    Log4J2LoggingHandler handler = new Harness();

    Channel ch = Mockito.mock(Channel.class, "mockChannel");
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Mockito.when(ctx.channel()).thenReturn(ch);

    SocketAddress localAddress = Mockito.mock(SocketAddress.class, "localAddress");
    SocketAddress remoteAddress = Mockito.mock(SocketAddress.class, "remoteAddress");
    ChannelPromise promise = Mockito.mock(ChannelPromise.class);

    handler.connect(ctx, remoteAddress, localAddress, promise);

    Mockito.verify(_mockLogger).isEnabled(Mockito.eq(Level.DEBUG));
    ArgumentCaptor<Object> message = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(_mockLogger).log(Mockito.eq(Level.DEBUG), message.capture());
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Assert.assertEquals(String.valueOf(message.getValue()), "mockChannel CONNECT: remoteAddress, localAddress");
    Mockito.verify(ctx).channel();
    Mockito.verify(ctx).connect(Mockito.eq(remoteAddress), Mockito.eq(localAddress), Mockito.eq(promise));
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testConnect1() throws Exception {
    Mockito.reset(_mockLogger);

    class Harness extends Log4J2LoggingHandler {
      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    Log4J2LoggingHandler handler = new Harness();

    Channel ch = Mockito.mock(Channel.class, "mockChannel");
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Mockito.when(ctx.channel()).thenReturn(ch);

    SocketAddress remoteAddress = Mockito.mock(SocketAddress.class, "remoteAddress");
    ChannelPromise promise = Mockito.mock(ChannelPromise.class);

    handler.connect(ctx, remoteAddress, null, promise);

    Mockito.verify(_mockLogger).isEnabled(Mockito.eq(Level.DEBUG));
    ArgumentCaptor<Object> message = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(_mockLogger).log(Mockito.eq(Level.DEBUG), message.capture());
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Assert.assertEquals(String.valueOf(message.getValue()), "mockChannel CONNECT: remoteAddress");
    Mockito.verify(ctx).channel();
    Mockito.verify(ctx).connect(Mockito.eq(remoteAddress), Mockito.isNull(SocketAddress.class), Mockito.eq(promise));
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testDisconnect() throws Exception {
    Mockito.reset(_mockLogger);

    class Harness extends Log4J2LoggingHandler {
      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    Log4J2LoggingHandler handler = new Harness();

    Channel ch = Mockito.mock(Channel.class, "mockChannel");
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Mockito.when(ctx.channel()).thenReturn(ch);
    ChannelPromise promise = Mockito.mock(ChannelPromise.class);

    handler.disconnect(ctx, promise);

    Mockito.verify(_mockLogger).isEnabled(Mockito.eq(Level.DEBUG));
    ArgumentCaptor<Object> message = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(_mockLogger).log(Mockito.eq(Level.DEBUG), message.capture());
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Assert.assertEquals(String.valueOf(message.getValue()), "mockChannel DISCONNECT");
    Mockito.verify(ctx).channel();
    Mockito.verify(ctx).disconnect(Mockito.eq(promise));
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testClose() throws Exception {
    Mockito.reset(_mockLogger);

    class Harness extends Log4J2LoggingHandler {
      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    Log4J2LoggingHandler handler = new Harness();

    Channel ch = Mockito.mock(Channel.class, "mockChannel");
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Mockito.when(ctx.channel()).thenReturn(ch);
    ChannelPromise promise = Mockito.mock(ChannelPromise.class);

    handler.close(ctx, promise);

    Mockito.verify(_mockLogger).isEnabled(Mockito.eq(Level.DEBUG));
    ArgumentCaptor<Object> message = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(_mockLogger).log(Mockito.eq(Level.DEBUG), message.capture());
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Assert.assertEquals(String.valueOf(message.getValue()), "mockChannel CLOSE");
    Mockito.verify(ctx).channel();
    Mockito.verify(ctx).close(Mockito.eq(promise));
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testDeregister() throws Exception {
    Mockito.reset(_mockLogger);

    class Harness extends Log4J2LoggingHandler {
      private Harness() {
        super("Foo");
      }

      @Override
      Logger setupLogger(Logger logger) {
        Assert.assertEquals(logger.getName(), "Foo");
        return _mockLogger;
      }
    }

    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    Log4J2LoggingHandler handler = new Harness();

    Channel ch = Mockito.mock(Channel.class, "mockChannel");
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Mockito.when(ctx.channel()).thenReturn(ch);
    ChannelPromise promise = Mockito.mock(ChannelPromise.class);

    handler.deregister(ctx, promise);

    Mockito.verify(_mockLogger).isEnabled(Mockito.eq(Level.DEBUG));
    ArgumentCaptor<Object> message = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(_mockLogger).log(Mockito.eq(Level.DEBUG), message.capture());
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Assert.assertEquals(String.valueOf(message.getValue()), "mockChannel DEREGISTER");
    Mockito.verify(ctx).channel();
    Mockito.verify(ctx).deregister(Mockito.eq(promise));
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testChannelReadComplete() throws Exception {
    Mockito.reset(_mockLogger);

    class Harness extends Log4J2LoggingHandler {
      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    Log4J2LoggingHandler handler = new Harness();

    Channel ch = Mockito.mock(Channel.class, "mockChannel");
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Mockito.when(ctx.channel()).thenReturn(ch);

    handler.channelReadComplete(ctx);

    Mockito.verify(_mockLogger).isEnabled(Mockito.eq(Level.DEBUG));
    ArgumentCaptor<Object> message = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(_mockLogger).log(Mockito.eq(Level.DEBUG), message.capture());
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Assert.assertEquals(String.valueOf(message.getValue()), "mockChannel READ COMPLETE");
    Mockito.verify(ctx).channel();
    Mockito.verify(ctx).fireChannelReadComplete();
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testChannelRead() throws Exception {
    Mockito.reset(_mockLogger);

    class Harness extends Log4J2LoggingHandler {
      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    Log4J2LoggingHandler handler = new Harness();

    Channel ch = Mockito.mock(Channel.class, "mockChannel");
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Mockito.when(ctx.channel()).thenReturn(ch);
    Object msg = Mockito.mock(Object.class, "Msg");

    handler.channelRead(ctx, msg);

    Mockito.verify(_mockLogger).isEnabled(Mockito.eq(Level.DEBUG));
    ArgumentCaptor<Object> message = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(_mockLogger).log(Mockito.eq(Level.DEBUG), message.capture());
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Assert.assertEquals(String.valueOf(message.getValue()), "mockChannel READ: Msg");
    Mockito.verify(ctx).channel();
    Mockito.verify(ctx).fireChannelRead(Mockito.eq(msg));
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testChannelReadByteBufHolder0() throws Exception {
    Mockito.reset(_mockLogger);

    class Harness extends Log4J2LoggingHandler {
      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    Log4J2LoggingHandler handler = new Harness();

    Channel ch = Mockito.mock(Channel.class, "mockChannel");
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Mockito.when(ctx.channel()).thenReturn(ch);
    ByteBuf byteBuf = Mockito.mock(ByteBuf.class, "Msg");
    ByteBufHolder msg = Mockito.mock(ByteBufHolder.class, "Holder0");
    Mockito.when(msg.content()).thenReturn(byteBuf);

    handler.channelRead(ctx, msg);

    Mockito.verify(_mockLogger).isEnabled(Mockito.eq(Level.DEBUG));
    ArgumentCaptor<Object> message = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(_mockLogger).log(Mockito.eq(Level.DEBUG), message.capture());
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Assert.assertEquals(String.valueOf(message.getValue()), "mockChannel READ, Holder0, 0B");
    Mockito.verify(ctx).channel();
    Mockito.verify(ctx).fireChannelRead(Mockito.eq(msg));
    Mockito.verifyNoMoreInteractions(ctx);

    Mockito.verify(msg).content();
    Mockito.verify(byteBuf).readableBytes();
  }

  @Test(groups = "unit")
  public void testChannelReadByteBufHolderHello() throws Exception {
    Mockito.reset(_mockLogger);

    class Harness extends Log4J2LoggingHandler {
      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    Log4J2LoggingHandler handler = new Harness();

    Channel ch = Mockito.mock(Channel.class, "mockChannel");
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Mockito.when(ctx.channel()).thenReturn(ch);
    ByteBuf byteBuf = Unpooled.copiedBuffer("Hello world", StandardCharsets.US_ASCII);
    ByteBufHolder msg = Mockito.mock(ByteBufHolder.class, "Holder1");
    Mockito.when(msg.content()).thenReturn(byteBuf);

    handler.channelRead(ctx, msg);

    Mockito.verify(_mockLogger).isEnabled(Mockito.eq(Level.DEBUG));
    ArgumentCaptor<Object> message = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(_mockLogger).log(Mockito.eq(Level.DEBUG), message.capture());
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Assert.assertEquals(
        String.valueOf(message.getValue()),
        "mockChannel READ: Holder1, 11B\n" + "         +-------------------------------------------------+\n"
            + "         |  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f |\n"
            + "+--------+-------------------------------------------------+----------------+\n"
            + "|00000000| 48 65 6c 6c 6f 20 77 6f 72 6c 64                |Hello world     |\n"
            + "+--------+-------------------------------------------------+----------------+");
    Mockito.verify(ctx).channel();
    Mockito.verify(ctx).fireChannelRead(Mockito.eq(msg));
    Mockito.verifyNoMoreInteractions(ctx);

    Mockito.verify(msg).content();
  }

  @Test(groups = "unit")
  public void testWrite() throws Exception {
    Mockito.reset(_mockLogger);

    class Harness extends Log4J2LoggingHandler {
      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    Log4J2LoggingHandler handler = new Harness();

    Channel ch = Mockito.mock(Channel.class, "mockChannel");
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Mockito.when(ctx.channel()).thenReturn(ch);
    ChannelPromise promise = Mockito.mock(ChannelPromise.class);
    Object msg = Mockito.mock(Object.class, "Msg");

    handler.write(ctx, msg, promise);

    Mockito.verify(_mockLogger).isEnabled(Mockito.eq(Level.DEBUG));
    ArgumentCaptor<Object> message = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(_mockLogger).log(Mockito.eq(Level.DEBUG), message.capture());
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Assert.assertEquals(String.valueOf(message.getValue()), "mockChannel WRITE: Msg");
    Mockito.verify(ctx).channel();
    Mockito.verify(ctx).write(Mockito.eq(msg), Mockito.eq(promise));
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testWriteByteBuf0() throws Exception {
    Mockito.reset(_mockLogger);

    class Harness extends Log4J2LoggingHandler {
      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    Log4J2LoggingHandler handler = new Harness();

    Channel ch = Mockito.mock(Channel.class, "mockChannel");
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Mockito.when(ctx.channel()).thenReturn(ch);
    ChannelPromise promise = Mockito.mock(ChannelPromise.class);
    ByteBuf msg = Mockito.mock(ByteBuf.class, "Msg");

    handler.write(ctx, msg, promise);

    Mockito.verify(_mockLogger).isEnabled(Mockito.eq(Level.DEBUG));
    ArgumentCaptor<Object> message = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(_mockLogger).log(Mockito.eq(Level.DEBUG), message.capture());
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Assert.assertEquals(String.valueOf(message.getValue()), "mockChannel WRITE: 0B");
    Mockito.verify(ctx).channel();
    Mockito.verify(ctx).write(Mockito.eq(msg), Mockito.eq(promise));
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testWriteByteBufHello() throws Exception {
    Mockito.reset(_mockLogger);

    class Harness extends Log4J2LoggingHandler {
      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    Log4J2LoggingHandler handler = new Harness();

    Channel ch = Mockito.mock(Channel.class, "mockChannel");
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Mockito.when(ctx.channel()).thenReturn(ch);
    ChannelPromise promise = Mockito.mock(ChannelPromise.class);
    ByteBuf msg = Unpooled.copiedBuffer("Hello World", StandardCharsets.US_ASCII);

    handler.write(ctx, msg, promise);

    Mockito.verify(_mockLogger).isEnabled(Mockito.eq(Level.DEBUG));
    ArgumentCaptor<Object> message = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(_mockLogger).log(Mockito.eq(Level.DEBUG), message.capture());
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Assert.assertEquals(
        String.valueOf(message.getValue()),
        "mockChannel WRITE: 11B\n" + "         +-------------------------------------------------+\n"
            + "         |  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f |\n"
            + "+--------+-------------------------------------------------+----------------+\n"
            + "|00000000| 48 65 6c 6c 6f 20 57 6f 72 6c 64                |Hello World     |\n"
            + "+--------+-------------------------------------------------+----------------+");
    Mockito.verify(ctx).channel();
    Mockito.verify(ctx).write(Mockito.eq(msg), Mockito.eq(promise));
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testChannelWritabilityChanged() throws Exception {
    Mockito.reset(_mockLogger);

    class Harness extends Log4J2LoggingHandler {
      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    Log4J2LoggingHandler handler = new Harness();

    Channel ch = Mockito.mock(Channel.class, "mockChannel");
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Mockito.when(ctx.channel()).thenReturn(ch);

    handler.channelWritabilityChanged(ctx);

    Mockito.verify(_mockLogger).isEnabled(Mockito.eq(Level.DEBUG));
    ArgumentCaptor<Object> message = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(_mockLogger).log(Mockito.eq(Level.DEBUG), message.capture());
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Assert.assertEquals(String.valueOf(message.getValue()), "mockChannel WRITABILITY CHANGED");
    Mockito.verify(ctx).channel();
    Mockito.verify(ctx).fireChannelWritabilityChanged();
    Mockito.verifyNoMoreInteractions(ctx);
  }

  @Test(groups = "unit")
  public void testFlush() throws Exception {
    Mockito.reset(_mockLogger);

    class Harness extends Log4J2LoggingHandler {
      @Override
      Logger setupLogger(Logger logger) {
        return _mockLogger;
      }
    }

    Mockito.when(_mockLogger.isEnabled(Mockito.eq(Level.DEBUG))).thenReturn(true);

    Log4J2LoggingHandler handler = new Harness();

    Channel ch = Mockito.mock(Channel.class, "mockChannel");
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Mockito.when(ctx.channel()).thenReturn(ch);

    handler.flush(ctx);

    Mockito.verify(_mockLogger).isEnabled(Mockito.eq(Level.DEBUG));
    ArgumentCaptor<Object> message = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(_mockLogger).log(Mockito.eq(Level.DEBUG), message.capture());
    Mockito.verifyNoMoreInteractions(_mockLogger);

    Assert.assertEquals(String.valueOf(message.getValue()), "mockChannel FLUSH");
    Mockito.verify(ctx).channel();
    Mockito.verify(ctx).flush();
    Mockito.verifyNoMoreInteractions(ctx);
  }
}
