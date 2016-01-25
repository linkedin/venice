package com.linkedin.venice.listener;

import com.linkedin.venice.server.StoreRepository;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class ListenerChannelInitializer extends ChannelInitializer<SocketChannel> {

  private final ThreadPoolExecutor threadPoolExecutor;
    private final StorageExecutionHandler storageExecutionHandler;

    private final int numRestServiceStorageThreads = 2;
    private final int restServiceStorageThreadPoolQueueSize = 2;

    public ListenerChannelInitializer(StoreRepository storeRepository) {

        this.threadPoolExecutor = new ThreadPoolExecutor(numRestServiceStorageThreads,
                                                         numRestServiceStorageThreads,
                                                         0L,
                                                         TimeUnit.MILLISECONDS,
                                                         new LinkedBlockingQueue<Runnable>(restServiceStorageThreadPoolQueueSize));

        storageExecutionHandler = new StorageExecutionHandler(threadPoolExecutor,
                                                              storeRepository);
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline()
            .addLast("decoder", new GetRequestDecoder())
            .addLast("storageExecutionHandler", storageExecutionHandler);
    }

}
