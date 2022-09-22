package com.linkedin.alpini.netty4.misc;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.AttributeKey;
import io.netty.util.AttributeMap;
import io.netty.util.concurrent.EventExecutorGroup;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import javax.annotation.Nonnull;


/**
 * Created by acurtis on 3/30/17.
 */
public class NettyUtils {
  private NettyUtils() {
    throw new UnsupportedOperationException("Never instantiated");
  }

  public enum Mode {
    NIO {
      @Override
      public Class<NioSocketChannel> socketChannel() {
        return NioSocketChannel.class;
      }

      @Override
      public Class<NioServerSocketChannel> serverSocketChannel() {
        return NioServerSocketChannel.class;
      }

      @Override
      public Class<NioDatagramChannel> datagramChannel() {
        return NioDatagramChannel.class;
      }

      @Override
      public NioEventLoopGroup newEventLoopGroup(int nThreads, ThreadFactory threadFactory) {
        return new NioEventLoopGroup(nThreads, threadFactory);
      }

      @Override
      public NioEventLoopGroup newEventLoopGroup(int nThreads, Executor executor) {
        return new NioEventLoopGroup(nThreads, executor);
      }
    },

    EPOLL {
      @Override
      public Class<EpollSocketChannel> socketChannel() {
        return EpollSocketChannel.class;
      }

      @Override
      public Class<EpollServerSocketChannel> serverSocketChannel() {
        return EpollServerSocketChannel.class;
      }

      @Override
      public Class<EpollDatagramChannel> datagramChannel() {
        return EpollDatagramChannel.class;
      }

      @Override
      public EpollEventLoopGroup newEventLoopGroup(int nThreads, ThreadFactory threadFactory) {
        return new EpollEventLoopGroup(nThreads, threadFactory);
      }

      @Override
      public EpollEventLoopGroup newEventLoopGroup(int nThreads, Executor executor) {
        return new EpollEventLoopGroup(nThreads, executor);
      }
    };

    public abstract Class<? extends SocketChannel> socketChannel();

    public abstract Class<? extends ServerSocketChannel> serverSocketChannel();

    public abstract Class<? extends DatagramChannel> datagramChannel();

    public abstract EventLoopGroup newEventLoopGroup(int nThreads, ThreadFactory threadFactory);

    public abstract EventLoopGroup newEventLoopGroup(int nThreads, Executor executor);
  }

  public enum ReadMode {
    COPY {
      @Override
      public ByteBuf read(ByteBuf in, int length) {
        return in.readBytes(length);
      }
    },
    SLICE {
      @Override
      public ByteBuf read(ByteBuf in, int length) {
        return in.readRetainedSlice(length);
      }
    };

    public abstract ByteBuf read(ByteBuf in, int length);
  }

  @Nonnull
  private static Mode _mode = Mode.NIO;

  @Nonnull
  private static ReadMode _readMode = ReadMode.COPY;

  public static void setMode(String mode) {
    setMode(Mode.valueOf(mode));
  }

  public static void setMode(Mode mode) {
    _mode = Objects.requireNonNull(mode);
  }

  @Nonnull
  public static Mode mode() {
    return _mode;
  }

  public static void setReadMode(String mode) {
    setReadMode(ReadMode.valueOf(mode));
  }

  public static void setReadMode(ReadMode mode) {
    _readMode = Objects.requireNonNull(mode);
  }

  @Nonnull
  public static ReadMode readMode() {
    return _readMode;
  }

  public static ByteBuf read(ByteBuf in, int length) {
    return readMode().read(in, length);
  }

  public static EventLoopGroup newEventLoopGroup(int nThreads, ThreadFactory threadFactory) {
    return mode().newEventLoopGroup(nThreads, threadFactory);
  }

  public static EventLoopGroup newEventLoopGroup(int nThreads, Executor executor) {
    return mode().newEventLoopGroup(nThreads, executor);
  }

  public static Class<? extends SocketChannel> socketChannel() {
    return mode().socketChannel();
  }

  public static Class<? extends ServerSocketChannel> serverSocketChannel() {
    return mode().serverSocketChannel();
  }

  public static Class<? extends DatagramChannel> datagramChannel() {
    return mode().datagramChannel();
  }

  public static final AttributeKey<EventExecutorGroup> EXECUTOR_GROUP_ATTRIBUTE_KEY =
      AttributeKey.valueOf(NettyUtils.class, EventExecutorGroup.class.getSimpleName());

  public static EventExecutorGroup executorGroup(Channel channel) {
    return channel.hasAttr(EXECUTOR_GROUP_ATTRIBUTE_KEY) ? channel.attr(EXECUTOR_GROUP_ATTRIBUTE_KEY).get() : null;
  }

  public static EventExecutorGroup executorGroup(ChannelPipeline pipeline) {
    return executorGroup(pipeline.channel());
  }

  public static boolean isTrue(AttributeMap map, AttributeKey<Boolean> key) {
    return map.hasAttr(key) && Boolean.TRUE.equals(map.attr(key).get()); // guards against null
  }
}
