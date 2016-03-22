package com.linkedin.venice.listener;

import com.linkedin.venice.message.GetRequestObject;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.server.StoreRepository;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.concurrent.ExecutorService;
import javax.validation.constraints.NotNull;


/***
 * Expects a GetRequestObject which has store name, key, and partition
 * Queries the local store for the associated value
 * writes the value (as a byte[]) back down the stack
 */
@ChannelHandler.Sharable
public class StorageExecutionHandler extends ChannelInboundHandlerAdapter {

  private final ExecutorService executor;
  private StoreRepository storeRepository;
  private final OffsetManager offsetManager;

  public StorageExecutionHandler(
      @NotNull ExecutorService executor,
      @NotNull StoreRepository storeRepository,
      @NotNull OffsetManager offsetManager) {
    this.executor = executor;
    this.storeRepository = storeRepository;
    this.offsetManager = offsetManager;
  }

  @Override
  public void channelRead(ChannelHandlerContext context, Object message) throws Exception {
    if(message instanceof GetRequestObject) {
      executor.execute(new StorageWorkerThread(context, (GetRequestObject) message, storeRepository, offsetManager));
    }
  }

}
