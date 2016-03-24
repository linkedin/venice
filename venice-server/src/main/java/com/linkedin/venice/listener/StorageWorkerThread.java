package com.linkedin.venice.listener;

import com.linkedin.venice.exceptions.PersistenceFailureException;
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

    try {
      byte[] value = store.get(partition, key);
      long offset = offsetManager.getLastOffset(topic, partition).getOffset();
      StorageResponseObject resp = new StorageResponseObject(value, offset);
      ctx.writeAndFlush(resp);
    } catch (PersistenceFailureException e){ //thrown if no key.  TODO Subclass this for a nokey exception
      ctx.writeAndFlush(new HttpError(
          "key not found in resource: " + topic + " and partition: " + partition,
          HttpResponseStatus.NOT_FOUND));
    }

  }

}
