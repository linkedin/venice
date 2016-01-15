package com.linkedin.venice.listener;

import com.linkedin.venice.message.GetRequestObject;
import com.linkedin.venice.message.GetResponseObject;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.store.AbstractStorageEngine;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.MessageEvent;


public class StorageWorkerThread implements Runnable {

  private MessageEvent messageEvent;
  private StoreRepository storeRepository;

  private final Logger logger = Logger.getLogger(StorageWorkerThread.class);

  public StorageWorkerThread(MessageEvent messageEvent, StoreRepository storeRepository) {
    this.messageEvent = messageEvent;
    this.storeRepository = storeRepository;
  }

  @Override
  public void run() {
    Object message = messageEvent.getMessage();

    if(message instanceof GetRequestObject) {
      GetRequestObject request = (GetRequestObject) message;
      int partition = request.getPartition();
      String topic = request.getTopicString();
      byte[] key = request.getKey();

      AbstractStorageEngine store = storeRepository.getLocalStorageEngine(topic);
      byte[] value = store.get(partition, key); //What does this return if no key?

      ChannelBuffer responseContent = ChannelBuffers.dynamicBuffer();
      responseContent.writeBytes(new GetResponseObject(value).serialize());
      this.messageEvent.getChannel().write(responseContent);
    }
  }


}
