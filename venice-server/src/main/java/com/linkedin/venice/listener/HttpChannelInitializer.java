package com.linkedin.venice.listener;

import com.linkedin.venice.server.StoreRepository;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/***
 * Use this channel initializer to support a GET Http requests of the format:
 * /store/key/partition
 *
 * The HttpServerCodec handles the conversion from bytes to usable HTTP objects
 *
 * The GetRequestHttpHandler takes an HTTP object, and generates a GetRequestObject
 *
 * The StoreExecutionHandler uses te GetRequestObject, queries the datastore
 * and emits a byte[] of the value returned for that store/key/partition
 *
 * The OutboundHttpWrapper wraps the bytes of the value in an HTTP response
 * that the HttpServerCodec expects
 */

public class HttpChannelInitializer extends ChannelInitializer<SocketChannel> {

  private final ThreadPoolExecutor threadPoolExecutor;
    private final StorageExecutionHandler storageExecutionHandler;

    private final int numRestServiceStorageThreads = 2;
    private final int restServiceStorageThreadPoolQueueSize = 2;

    public HttpChannelInitializer(StoreRepository storeRepository) {

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
            .addLast(new HttpServerCodec())
            .addLast(new OutboundHttpWrapperHandler())
            .addLast(new GetRequestHttpHandler())
            .addLast("storageExecutionHandler", storageExecutionHandler);
    }

}
