package com.linkedin.venice.listener;

import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.utils.Utils;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;


import static org.jboss.netty.channel.Channels.pipeline;


public class ListenerPipelineFactory implements ChannelPipelineFactory {

    private StoreRepository storeRepository;
    private final ThreadPoolExecutor threadPoolExecutor;
    private final StorageExecutionHandler storageExecutionHandler;

    private final int numRestServiceStorageThreads = 2;
    private final int restServiceStorageThreadPoolQueueSize = 2;

    public ListenerPipelineFactory(StoreRepository storeRepository) {
        this.storeRepository = storeRepository;
        this.threadPoolExecutor = new ThreadPoolExecutor(numRestServiceStorageThreads,
                                                         numRestServiceStorageThreads,
                                                         0L,
                                                         TimeUnit.MILLISECONDS,
                                                         new LinkedBlockingQueue<Runnable>(restServiceStorageThreadPoolQueueSize));

        storageExecutionHandler = new StorageExecutionHandler(threadPoolExecutor,
                                                              storeRepository);
    }


    @Override
    public ChannelPipeline getPipeline() throws Exception {
        // Create a default pipeline implementation.
        ChannelPipeline pipeline = pipeline();
        pipeline.addLast("decoder", new GetRequestDecoder());
        pipeline.addLast("storageExecutionHandler", storageExecutionHandler);
        return pipeline;
    }

}
