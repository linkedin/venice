package com.linkedin.alpini.netty4.misc;

import io.netty.channel.EventLoopGroup;
import javax.annotation.Nonnull;


/**
 * Created by acurtis on 12/22/17.
 */
public interface EventGroupLoopSupplier {
  @Nonnull
  EventLoopGroup getEventLoopGroup();
}
