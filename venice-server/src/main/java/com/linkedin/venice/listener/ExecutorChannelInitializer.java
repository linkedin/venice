package com.linkedin.venice.listener;

import com.linkedin.venice.server.StoreRepository;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/***
 * Any channel initializer that uses the storageExecutionHandler will need
 * the same code to setup the storageExecutionHandler.  Extend this abstract
 * class instead of re-writing that boilerplate
 */
public abstract class ExecutorChannelInitializer extends ChannelInitializer<SocketChannel> {

  private final ThreadPoolExecutor threadPoolExecutor;
    protected final StorageExecutionHandler storageExecutionHandler;
    private static final long KEEPALIVE_ZERO = 0L;


  //TODO make this configurable
    private static final int numRestServiceStorageThreads = 2;
    private static final int restServiceStorageThreadPoolQueueSize = 2;

    public ExecutorChannelInitializer(StoreRepository storeRepository) {

        this.threadPoolExecutor = new ThreadPoolExecutor(numRestServiceStorageThreads,
                                                         numRestServiceStorageThreads,
                                                         KEEPALIVE_ZERO,
                                                         TimeUnit.MILLISECONDS,
                                                         new LinkedBlockingQueue<Runnable>(restServiceStorageThreadPoolQueueSize));

        storageExecutionHandler = new StorageExecutionHandler(threadPoolExecutor,
                                                              storeRepository);
    }

    @Override
    abstract public void initChannel(SocketChannel ch) throws Exception;

}
