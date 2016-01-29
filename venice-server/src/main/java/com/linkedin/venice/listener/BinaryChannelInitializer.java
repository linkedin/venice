package com.linkedin.venice.listener;

import com.linkedin.venice.server.StoreRepository;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/***
 * Use this channel initializer to support a binary TCP protocol that
 * uses serialized GetRequestObject and GetResponseObject on the wire
 *
 * The GetRequestDecoder takes a serialized object on the wire, and emits
 * a deserialized GetRequestObject
 *
 * The StoreExecutionHandler uses te GetRequestObject, queries the datastore
 * and emits a byte[] of the value returned for that store/key/partition
 *
 * The GetResponseEncoder wraps the value bytes in a GetResponseObject
 * and serializes the object onto the wire
 */

public class BinaryChannelInitializer extends ChannelInitializer<SocketChannel> {

  private final ThreadPoolExecutor threadPoolExecutor;
    private final StorageExecutionHandler storageExecutionHandler;

    private final int numRestServiceStorageThreads = 2;
    private final int restServiceStorageThreadPoolQueueSize = 2;

    public BinaryChannelInitializer(StoreRepository storeRepository) {

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
            .addLast("encoder", new GetResponseEncoder())
            .addLast("storageExecutionHandler", storageExecutionHandler);
    }

}
