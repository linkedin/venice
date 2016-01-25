package com.linkedin.venice.listener;

import com.linkedin.venice.message.GetRequestObject;
import com.linkedin.venice.server.StoreRepository;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.concurrent.ThreadPoolExecutor;


@ChannelHandler.Sharable
public class StorageExecutionHandler extends ChannelInboundHandlerAdapter {

  private final ThreadPoolExecutor executor;
  private StoreRepository storeRepository;

  public StorageExecutionHandler(ThreadPoolExecutor executor, StoreRepository storeRepository) {
    if (executor == null) {
      throw new IllegalArgumentException("StorageExecutionHandler created with null executor");
    }
    this.executor = executor;
    this.storeRepository = storeRepository;
  }

  @Override
  public void channelRead(ChannelHandlerContext context, Object message) throws Exception {
    if(message instanceof GetRequestObject) {
      executor.execute(new StorageWorkerThread(context, (GetRequestObject) message, storeRepository));
    }
  }

}
