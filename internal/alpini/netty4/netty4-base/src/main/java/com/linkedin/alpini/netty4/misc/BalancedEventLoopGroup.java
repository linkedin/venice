package com.linkedin.alpini.netty4.misc;

import com.linkedin.alpini.netty4.handlers.AllChannelsHandler;
import io.netty.channel.AbstractEventLoopGroup;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;


/**
 * An {@link EventLoopGroup} instance which may be passed to an instance of {@link io.netty.bootstrap.Bootstrap}
 * to ensure that there is a reasonably fair balance of connections for the supplied {@link AllChannelsHandler}.
 *
 * <P>This can be used to ensure that incoming and outgoing connections are balanced.
 *
 * <P>Example:
 * <pre>
 *
 *   AllChannelsHandler allChannelsHandler = new AllChannelsHandler();
 *
 *   Bootstrap bootstrap = new Bootstrap()
 *       .group(new BalancedEventLoopGroup(_eventLoopGroup, allChannelsHandler))
 *       ...
 *       .handler(new ChannelInitializer&lt;Channel&gt;() {
 *         protected void initChannel(Channel ch) {
 *           ch.pipeline().addLast(allChannelsHandler);
 *           ...
 *         }
 *       }
 *
 *
 * </pre>
 * @author acurtis
 */
public class BalancedEventLoopGroup extends AbstractEventLoopGroup {
  private final EventLoopGroup _eventLoopGroup;
  private final AllChannelsHandler _allChannels;
  private final int _eventLoopCount;
  private final boolean _tightBalance;

  public BalancedEventLoopGroup(EventLoopGroup eventLoopGroup, AllChannelsHandler allChannels) {
    this(eventLoopGroup, allChannels, true);
  }

  public BalancedEventLoopGroup(EventLoopGroup eventLoopGroup, AllChannelsHandler allChannels, boolean tightBalance) {
    _eventLoopGroup = eventLoopGroup;
    _allChannels = allChannels;
    _eventLoopCount = (int) StreamSupport.stream(_eventLoopGroup.spliterator(), false).count();
    _tightBalance = tightBalance;
  }

  @Override
  public EventLoop next() {
    return _eventLoopGroup.next();
  }

  @Override
  public Iterator<EventExecutor> iterator() {
    return _eventLoopGroup.iterator();
  }

  private int averageChannelsPerEventLoop() {
    int channels = _allChannels.size();
    return (channels + _eventLoopCount - 1) / _eventLoopCount;
  }

  private int countChannelsInLoop(EventLoop eventLoop) {
    return _allChannels.sizeOf(eventLoop);
  }

  private boolean isUnbalanced(EventLoop eventLoop, int average) {
    int channelsInLoop = countChannelsInLoop(eventLoop);
    return _tightBalance ? channelsInLoop >= average : channelsInLoop > average;
  }

  private EventLoop selectLoop() {
    int average = averageChannelsPerEventLoop();
    EventLoop loop = next();
    if (_eventLoopCount > 1 && isUnbalanced(loop, average)) {
      ArrayList<EventLoop> list = new ArrayList<>(_eventLoopCount);
      _eventLoopGroup.forEach(eventExecutor -> list.add((EventLoop) eventExecutor));
      Collections.shuffle(list, ThreadLocalRandom.current());
      Iterator<EventLoop> it = list.iterator();
      do {
        loop = it.next();
      } while (it.hasNext() && isUnbalanced(loop, average));
    }
    return loop;
  }

  @Override
  public ChannelFuture register(Channel channel) {
    return selectLoop().register(channel);
  }

  @Override
  public ChannelFuture register(ChannelPromise promise) {
    return selectLoop().register(promise);
  }

  @Deprecated
  @Override
  public ChannelFuture register(Channel channel, ChannelPromise promise) {
    return selectLoop().register(channel, promise);
  }

  @Override
  public boolean isShuttingDown() {
    return _eventLoopGroup.isShuttingDown();
  }

  @Override
  public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
    return _eventLoopGroup.shutdownGracefully(quietPeriod, timeout, unit);
  }

  @Override
  public Future<?> terminationFuture() {
    return _eventLoopGroup.terminationFuture();
  }

  @Deprecated
  @Override
  public void shutdown() {
    _eventLoopGroup.shutdown();
  }

  @Override
  public boolean isShutdown() {
    return _eventLoopGroup.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return _eventLoopGroup.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return _eventLoopGroup.awaitTermination(timeout, unit);
  }
}
