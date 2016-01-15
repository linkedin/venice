package com.linkedin.venice.listener;

import com.linkedin.venice.server.StoreRepository;
import java.util.concurrent.Executor;
import org.jboss.netty.channel.ChannelDownstreamHandler;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.util.ExternalResourceReleasable;


/**
 * Created by mwise on 1/14/16.
 */
public class StorageExecutionHandler implements ChannelUpstreamHandler, ExternalResourceReleasable {

  private final Executor executor;
  private StoreRepository storeRepository;


  public StorageExecutionHandler(Executor executor, StoreRepository storeRepository) {
    if (executor == null) {
      throw new NullPointerException("StorageExecutionHandler created with null executor");
    }
    this.executor = executor;
    this.storeRepository = storeRepository;
  }

  @Override
  public void handleUpstream(ChannelHandlerContext context, ChannelEvent channelEvent)
      throws Exception {
    if(channelEvent instanceof MessageEvent) {
      getExecutor().execute(new StorageWorkerThread((MessageEvent) channelEvent, storeRepository));
    }
  }

  public Executor getExecutor() {
    return executor;
  }

  @Override
  public void releaseExternalResources() {

  }
}
