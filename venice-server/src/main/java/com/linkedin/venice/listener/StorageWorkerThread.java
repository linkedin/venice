package com.linkedin.venice.listener;

import com.linkedin.venice.message.GetRequestObject;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.store.AbstractStorageEngine;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.log4j.Logger;

public class StorageWorkerThread implements Runnable {

  private GetRequestObject request;
  private StoreRepository storeRepository;
  private OffsetManager offsetManager;
  private ChannelHandlerContext ctx;

  private final Logger logger = Logger.getLogger(StorageWorkerThread.class);

  public StorageWorkerThread(ChannelHandlerContext ctx, GetRequestObject request, StoreRepository storeRepository, OffsetManager offsetManager) {
    this.ctx = ctx;
    this.request = request;
    this.storeRepository = storeRepository;
    this.offsetManager = offsetManager;
  }

  @Override
  public void run() {
    int partition = request.getPartition();
    String topic = request.getTopicString();
    byte[] key = request.getKey();

    AbstractStorageEngine store = storeRepository.getLocalStorageEngine(topic);
    //TODO : handle other exceptions here, if there is uncaught exception
    // the caller times out with no exception relayed back.
    byte[] value = store.get(partition, key);
    if( value != null) {
      long offset = offsetManager.getLastOffset(topic, partition).getOffset();
      StorageResponseObject resp = new StorageResponseObject(value, offset);
      ctx.writeAndFlush(resp);
    } else {
      ctx.writeAndFlush(new HttpShortcutResponse("key not found in resource: " + topic + " and partition: " + partition,
          HttpResponseStatus.NOT_FOUND));
    }
  }

}
