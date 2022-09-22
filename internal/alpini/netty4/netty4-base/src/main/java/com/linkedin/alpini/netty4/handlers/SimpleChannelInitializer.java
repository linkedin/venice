package com.linkedin.alpini.netty4.handlers;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.util.concurrent.EventExecutorGroup;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.WeakHashMap;


/**
 * Created by acurtis on 4/27/18.
 */
public abstract class SimpleChannelInitializer<C extends Channel> extends ChannelInitializer<C> {
  private static final WeakHashMap<String, List<String>> NAME_CACHE = new WeakHashMap<>();

  protected void addAfter(C ch, String name, ChannelHandler handler) {
    addAfter(name, handler);
  }

  protected void addAfter(C ch, EventExecutorGroup group, String name, ChannelHandler handler) {
    addAfter(group, name, handler);
  }

  protected void addAfter(C ch, ChannelHandler... handlers) {
    addAfter(handlers);
  }

  protected Initializer addAfter(String name, ChannelHandler handler) {
    return initalizer().addAfter(name, handler);
  }

  protected Initializer addAfter(EventExecutorGroup group, String name, ChannelHandler handler) {
    return initalizer().addAfter(group, name, handler);
  }

  protected Initializer addAfter(ChannelHandler... handlers) {
    return initalizer().addAfter(handlers);
  }

  protected Initializer addAfter(EventExecutorGroup group, ChannelHandler... handlers) {
    return initalizer().addAfter(group, handlers);
  }

  private Initializer initalizer() {
    ChannelHandlerContext ctx = Objects.requireNonNull(currentContext());
    ChannelPipeline pipeline = ctx.pipeline();

    return new Initializer() {
      private String _position = ctx.name();
      private int _ordinal = 0;

      @Override
      public Initializer addAfter(String name, ChannelHandler handler) {
        if (name == null) {
          return addAfter(handler);
        }

        return addAfter(ctx.executor(), name, handler);
      }

      @Override
      public Initializer addAfter(EventExecutorGroup group, String name, ChannelHandler handler) {
        if (group == null) {
          return addAfter(name, handler);
        }

        if (name == null) {
          return addAfter(group, handler);
        }

        pipeline.addAfter(group, _position, name, handler);
        _position = name;

        return this;
      }

      @Override
      public Initializer addAfter(ChannelHandler... handlers) {
        return addAfter(ctx.executor(), handlers);
      }

      @Override
      public Initializer addAfter(EventExecutorGroup group, ChannelHandler... handlers) {
        if (group == null) {
          return addAfter(handlers);
        }

        for (ChannelHandler handler: handlers) {
          String name;
          do {
            name = nextGeneratedName();
            // check for name conflict
          } while (pipeline.context(name) != null);

          pipeline.addAfter(group, _position, name, handler);
          _position = name;
        }

        return this;
      }

      private String nextGeneratedName() {
        int ordinal = ++_ordinal;
        String name = ctx.name();
        // WeakHashMap needs to be accessed via synchronized section
        synchronized (NAME_CACHE) {
          List<String> names = NAME_CACHE.computeIfAbsent(name, k -> new ArrayList<>());
          while (names.size() < ordinal) {
            names.add(name + "-" + (names.size() + 1));
          }
          return names.get(ordinal - 1);
        }
      }
    };
  }

  interface Initializer {
    Initializer addAfter(String name, ChannelHandler handler);

    Initializer addAfter(EventExecutorGroup group, String name, ChannelHandler handler);

    Initializer addAfter(ChannelHandler... handlers);

    Initializer addAfter(EventExecutorGroup group, ChannelHandler... handlers);
  }
}
