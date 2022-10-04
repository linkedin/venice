package com.linkedin.alpini.netty4.handlers;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


@ChannelHandler.Sharable
public class AllChannelsHandler extends ChannelHandlerAdapter implements Set<Channel> {
  private final ConcurrentMap<EventLoop, ChannelGroup> _maps = new ConcurrentHashMap<>();

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) {
    EventLoop eventLoop = ctx.channel().eventLoop();
    ChannelGroup group = _maps.get(eventLoop);
    if (group == null) {
      group = new DefaultChannelGroup(getClass().getSimpleName(), eventLoop);
      _maps.put(eventLoop, group);
      ctx.channel().eventLoop().terminationFuture().addListener(future -> _maps.remove(eventLoop));
    }
    // Channel lifecycle is managed by ChannelGroup
    group.add(ctx.channel());
  }

  public int sizeOf(EventLoop loop) {
    ChannelGroup group = _maps.get(loop);
    return group != null ? group.size() : 0;
  }

  @Override
  public int size() {
    return _maps.values().stream().mapToInt(ChannelGroup::size).sum();
  }

  @Override
  public boolean isEmpty() {
    return _maps.values().stream().allMatch(ChannelGroup::isEmpty);
  }

  @Override
  public boolean contains(Object o) {
    if (o instanceof Channel) {
      EventLoop loop = ((Channel) o).eventLoop();
      ChannelGroup group = _maps.get(loop);
      return group != null && group.contains(o);
    }
    return false;
  }

  @Nonnull
  @Override
  public Iterator<Channel> iterator() {
    return _maps.values().stream().flatMap(ChannelGroup::stream).iterator();
  }

  @Override
  public Object[] toArray() {
    return _maps.values().stream().flatMap(ChannelGroup::stream).toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return _maps.values().stream().flatMap(ChannelGroup::stream).collect(Collectors.toList()).toArray(a);
  }

  @Override
  public boolean add(Channel channel) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return c.stream().allMatch(this::contains);
  }

  @Override
  public boolean addAll(Collection<? extends Channel> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }
}
