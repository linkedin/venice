package com.linkedin.venice.utils;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class SimpleServerChannelInitializer extends ChannelInitializer<SocketChannel> {
  private static final AtomicInteger counter = new AtomicInteger();

  private static final Logger LOGGER = LogManager.getLogger();

  private final SimpleServer simpleServer;
  private final int id = counter.getAndIncrement();

  public SimpleServerChannelInitializer(SimpleServer simpleServer) {
    this.simpleServer = simpleServer;
    LOGGER.info("SimpleServerChannelInitializer created for channel index={}", id);
  }

  @Override
  protected void initChannel(SocketChannel ch) {
    ch.pipeline().addLast(new HttpRequestDecoder());
    // Set the maximum allowed request size to 100MB as the initial metric report size is fairly large.
    ch.pipeline().addLast(new HttpObjectAggregator(1024 * 1024 * 100));
    ch.pipeline().addLast(new HttpResponseEncoder());
    ch.pipeline().addLast(new SimpleServerHandler(simpleServer));
    LOGGER.info("SimpleServerChannelInitializer initialized for channel index={}", id);
  }
}
