package com.linkedin.alpini.netty4.handlers;

import static io.netty.buffer.ByteBufUtil.appendPrettyHexDump;
import static io.netty.util.internal.StringUtil.NEWLINE;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.internal.logging.InternalLogLevel;
import java.net.SocketAddress;
import java.util.EnumMap;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.StringBuilderFormattable;


/**
 * Created by acurtis on 4/20/18.
 */
public class Log4J2LoggingHandler extends LoggingHandler {
  private static final EnumMap<InternalLogLevel, Level> LEVEL_ENUM_MAP = new EnumMap<>(InternalLogLevel.class);

  static {
    LEVEL_ENUM_MAP.put(InternalLogLevel.DEBUG, Level.DEBUG);
    LEVEL_ENUM_MAP.put(InternalLogLevel.TRACE, Level.TRACE);
    LEVEL_ENUM_MAP.put(InternalLogLevel.INFO, Level.INFO);
    LEVEL_ENUM_MAP.put(InternalLogLevel.WARN, Level.WARN);
    LEVEL_ENUM_MAP.put(InternalLogLevel.ERROR, Level.ERROR);
  }

  private static final LogLevel DEFAULT_LEVEL = LogLevel.DEBUG;

  protected final Logger logger;
  protected final Level internalLevel;

  /**
   * Creates a new instance whose logger name is the fully qualified class
   * name of the instance with hex dump enabled.
   */
  public Log4J2LoggingHandler() {
    this(DEFAULT_LEVEL);
  }

  /**
   * Creates a new instance whose logger name is the fully qualified class
   * name of the instance.
   *
   * @param level the log level
   */
  public Log4J2LoggingHandler(LogLevel level) {
    super(level);

    logger = setupLogger(LogManager.getLogger(getClass()));
    internalLevel = LEVEL_ENUM_MAP.get(level().toInternalLevel());
  }

  /**
   * Creates a new instance with the specified logger name and with hex dump
   * enabled.
   *
   * @param clazz the class type to generate the logger for
   */
  public Log4J2LoggingHandler(Class<?> clazz) {
    this(clazz, DEFAULT_LEVEL);
  }

  /**
   * Creates a new instance with the specified logger name.
   *
   * @param clazz the class type to generate the logger for
   * @param level the log level
   */
  public Log4J2LoggingHandler(Class<?> clazz, LogLevel level) {
    super(clazz, level);

    logger = setupLogger(LogManager.getLogger(clazz));
    internalLevel = LEVEL_ENUM_MAP.get(level().toInternalLevel());
  }

  /**
   * Creates a new instance with the specified logger name using the default log level.
   *
   * @param name the name of the class to use for the logger
   */
  public Log4J2LoggingHandler(String name) {
    this(name, DEFAULT_LEVEL);
  }

  /**
   * Creates a new instance with the specified logger name.
   *
   * @param name the name of the class to use for the logger
   * @param level the log level
   */
  public Log4J2LoggingHandler(String name, LogLevel level) {
    super(name, level);

    logger = setupLogger(LogManager.getLogger(name));
    internalLevel = LEVEL_ENUM_MAP.get(level().toInternalLevel());
  }

  Logger setupLogger(Logger logger) {
    return logger;
  }

  @Override
  public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
    if (logger.isEnabled(internalLevel)) {
      logger.log(internalLevel, format0(ctx, "REGISTERED"));
    }
    ctx.fireChannelRegistered();
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    if (logger.isEnabled(internalLevel)) {
      logger.log(internalLevel, format0(ctx, "UNREGISTERED"));
    }
    ctx.fireChannelUnregistered();
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    if (logger.isEnabled(internalLevel)) {
      logger.log(internalLevel, format0(ctx, "ACTIVE"));
    }
    ctx.fireChannelActive();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    if (logger.isEnabled(internalLevel)) {
      logger.log(internalLevel, format0(ctx, "INACTIVE"));
    }
    ctx.fireChannelInactive();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    if (logger.isEnabled(internalLevel)) {
      logger.log(internalLevel, format0(ctx, "EXCEPTION", cause), cause);
    }
    ctx.fireExceptionCaught(cause);
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (logger.isEnabled(internalLevel)) {
      logger.log(internalLevel, format0(ctx, "USER_EVENT", evt));
    }
    ctx.fireUserEventTriggered(evt);
  }

  @Override
  public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise promise) throws Exception {
    if (logger.isEnabled(internalLevel)) {
      logger.log(internalLevel, format0(ctx, "BIND", localAddress));
    }
    ctx.bind(localAddress, promise);
  }

  @Override
  public void connect(
      ChannelHandlerContext ctx,
      SocketAddress remoteAddress,
      SocketAddress localAddress,
      ChannelPromise promise) throws Exception {
    if (logger.isEnabled(internalLevel)) {
      logger.log(internalLevel, format0(ctx, "CONNECT", remoteAddress, localAddress));
    }
    ctx.connect(remoteAddress, localAddress, promise);
  }

  @Override
  public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
    if (logger.isEnabled(internalLevel)) {
      logger.log(internalLevel, format0(ctx, "DISCONNECT"));
    }
    ctx.disconnect(promise);
  }

  @Override
  public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
    if (logger.isEnabled(internalLevel)) {
      logger.log(internalLevel, format0(ctx, "CLOSE"));
    }
    ctx.close(promise);
  }

  @Override
  public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
    if (logger.isEnabled(internalLevel)) {
      logger.log(internalLevel, format0(ctx, "DEREGISTER"));
    }
    ctx.deregister(promise);
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    if (logger.isEnabled(internalLevel)) {
      logger.log(internalLevel, format0(ctx, "READ COMPLETE"));
    }
    ctx.fireChannelReadComplete();
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (logger.isEnabled(internalLevel)) {
      logger.log(internalLevel, format0(ctx, "READ", msg));
    }
    ctx.fireChannelRead(msg);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    if (logger.isEnabled(internalLevel)) {
      logger.log(internalLevel, format0(ctx, "WRITE", msg));
    }
    ctx.write(msg, promise);
  }

  @Override
  public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
    if (logger.isEnabled(internalLevel)) {
      logger.log(internalLevel, format0(ctx, "WRITABILITY CHANGED"));
    }
    ctx.fireChannelWritabilityChanged();
  }

  @Override
  public void flush(ChannelHandlerContext ctx) throws Exception {
    if (logger.isEnabled(internalLevel)) {
      logger.log(internalLevel, format0(ctx, "FLUSH"));
    }
    ctx.flush();
  }

  /**
   * Formats an event and returns the formatted message.
   *
   * @param eventName the name of the event
   */
  private static Object format0(ChannelHandlerContext ctx, String eventName) {
    return new Formattable() {
      @Override
      public void formatTo(StringBuilder buffer) {
        buffer.append(ctx.channel()).append(' ').append(eventName);
      }
    };
  }

  /**
   * Formats an event and returns the formatted message.
   *
   * @param eventName the name of the event
   * @param arg       the argument of the event
   */
  private static Object format0(ChannelHandlerContext ctx, String eventName, Object arg) {
    if (arg instanceof ByteBuf) {
      return formatByteBuf(ctx, eventName, (ByteBuf) arg);
    } else if (arg instanceof ByteBufHolder) {
      return formatByteBufHolder(ctx, eventName, (ByteBufHolder) arg);
    } else {
      return formatSimple(ctx, eventName, arg);
    }
  }

  /**
   * Formats an event and returns the formatted message.  This method is currently only used for formatting
   * {@link io.netty.channel.ChannelOutboundHandler#connect(ChannelHandlerContext, SocketAddress, SocketAddress, ChannelPromise)}.
   *
   * @param eventName the name of the event
   * @param firstArg  the first argument of the event
   * @param secondArg the second argument of the event
   */
  private static Object format0(ChannelHandlerContext ctx, String eventName, Object firstArg, Object secondArg) {
    if (secondArg == null) {
      return formatSimple(ctx, eventName, firstArg);
    }
    return new Formattable() {
      @Override
      public void formatTo(StringBuilder buffer) {
        buffer.append(ctx.channel())
            .append(' ')
            .append(eventName)
            .append(": ")
            .append(firstArg)
            .append(", ")
            .append(secondArg);
      }
    };
  }

  /**
   * Generates the default log message of the specified event whose argument is a {@link ByteBuf}.
   */
  private static Object formatByteBuf(ChannelHandlerContext ctx, String eventName, ByteBuf msg) {
    int length = msg.readableBytes();
    if (length == 0) {
      return new Formattable() {
        @Override
        public void formatTo(StringBuilder buffer) {
          buffer.append(ctx.channel()).append(' ').append(eventName).append(": 0B");
        }
      };
    } else {
      ByteBuf msgCopy = Unpooled.copiedBuffer(msg);
      return new Formattable() {
        @Override
        public void formatTo(StringBuilder buffer) {
          buffer.append(ctx.channel())
              .append(' ')
              .append(eventName)
              .append(": ")
              .append(length)
              .append('B')
              .append(NEWLINE);
          appendPrettyHexDump(buffer, msgCopy);
        }
      };
    }
  }

  /**
   * Generates the default log message of the specified event whose argument is a {@link ByteBufHolder}.
   */
  private static Object formatByteBufHolder(ChannelHandlerContext ctx, String eventName, ByteBufHolder msg) {
    ByteBuf content = msg.content();
    int length = content.readableBytes();
    if (length == 0) {
      return new Formattable() {
        @Override
        public void formatTo(StringBuilder buffer) {
          buffer.append(ctx.channel()).append(' ').append(eventName).append(", ").append(msg).append(", 0B");
        }
      };
    } else {
      assert content.refCnt() > 0;
      ByteBuf contentCopy = Unpooled.copiedBuffer(content);
      return new Formattable() {
        @Override
        public void formatTo(StringBuilder buffer) {
          buffer.append(ctx.channel())
              .append(' ')
              .append(eventName)
              .append(": ")
              .append(msg)
              .append(", ")
              .append(length)
              .append('B')
              .append(NEWLINE);
          appendPrettyHexDump(buffer, contentCopy);
        }
      };
    }
  }

  /**
   * Generates the default log message of the specified event whose argument is an arbitrary object.
   */
  private static Object formatSimple(ChannelHandlerContext ctx, String eventName, Object msg) {
    return new Formattable() {
      @Override
      public void formatTo(StringBuilder buffer) {
        buffer.append(ctx.channel()).append(' ').append(eventName).append(": ").append(msg);
      }
    };
  }

  private static abstract class Formattable implements StringBuilderFormattable {
    @Override
    public final String toString() {
      StringBuilder buf = new StringBuilder();
      formatTo(buf);
      return buf.toString();
    }
  }
}
