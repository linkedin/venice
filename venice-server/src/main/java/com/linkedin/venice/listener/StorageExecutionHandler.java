package com.linkedin.venice.listener;

import com.linkedin.venice.message.GetRequestObject;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.stats.AggServerHttpRequestStats;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpResponseStatus;
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
  private final AggServerHttpRequestStats stats;

  public StorageExecutionHandler(
      @NotNull ExecutorService executor,
      @NotNull StoreRepository storeRepository,
      @NotNull OffsetManager offsetManager,
      @NotNull AggServerHttpRequestStats stats) {
    this.executor = executor;
    this.storeRepository = storeRepository;
    this.offsetManager = offsetManager;
    this.stats = stats;
  }

  @Override
  public void channelRead(ChannelHandlerContext context, Object message) throws Exception {
    if(message instanceof GetRequestObject) {
      // TODO : This creates one thread per request, it is costly and needs to be fixed.
      // TODO : If there is uncaught exception from the thread, it hangs the client requests.
      executor.execute(new StorageWorkerThread(context, (GetRequestObject) message, storeRepository, offsetManager, stats));
    } else {
      context.writeAndFlush(new HttpShortcutResponse("Unrecognized object in StorageExecutionHandler", HttpResponseStatus.INTERNAL_SERVER_ERROR));
    }
  }

}
