package com.linkedin.venice.listener;

import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.utils.DaemonThreadFactory;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HttpChannelInitializer extends ChannelInitializer<SocketChannel> {

  private final ExecutorService executor;
    protected final StorageExecutionHandler storageExecutionHandler;
    private static final long KEEPALIVE_ZERO = 0L;

  //TODO make this configurable
    private static final int numRestServiceStorageThreads = 8;

    public HttpChannelInitializer(StoreRepository storeRepository, OffsetManager offsetManager) {
      this.executor = Executors.newFixedThreadPool(
          numRestServiceStorageThreads,
          new DaemonThreadFactory("StorageExecutionThread"));

      storageExecutionHandler = new StorageExecutionHandler(executor,
                                                            storeRepository,
                                                            offsetManager);
    }

  @Override
  public void initChannel(SocketChannel ch) throws Exception {
    ch.pipeline()
        .addLast(new HttpServerCodec())
        .addLast(new OutboundHttpWrapperHandler())
        .addLast(new GetRequestHttpHandler())
        .addLast("storageExecutionHandler", storageExecutionHandler)
        .addLast(new ErrorCatchingHandler());
  }

}
