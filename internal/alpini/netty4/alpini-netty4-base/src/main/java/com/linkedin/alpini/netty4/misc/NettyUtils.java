package com.linkedin.alpini.netty4.misc;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.util.AttributeKey;
import io.netty.util.AttributeMap;
import io.netty.util.concurrent.EventExecutorGroup;


/**
 * Created by acurtis on 3/30/17.
 */
public class NettyUtils {
  private NettyUtils() {
    throw new UnsupportedOperationException("Never instantiated");
  }

  public static ByteBuf read(ByteBuf in, int length) {
    return in.readBytes(length);
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
