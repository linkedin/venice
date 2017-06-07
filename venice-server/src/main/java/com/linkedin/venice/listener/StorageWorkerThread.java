package com.linkedin.venice.listener;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.request.GetRouterRequest;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.listener.response.MultiGetResponseWrapper;
import com.linkedin.venice.listener.request.MultiGetRouterRequestWrapper;
import com.linkedin.venice.listener.response.HttpShortcutResponse;
import com.linkedin.venice.listener.response.StorageResponseObject;
import com.linkedin.venice.message.GetRequestObject;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.record.ValueRecord;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.HashSet;
import java.util.Set;

public class StorageWorkerThread implements Runnable {
  private RouterRequest request;
  private StoreRepository storeRepository;
  private OffsetManager offsetManager;
  private ChannelHandlerContext ctx;


  public StorageWorkerThread(ChannelHandlerContext ctx,
                             RouterRequest request,
                             StoreRepository storeRepository,
                             OffsetManager offsetManager) {
    this.ctx = ctx;
    this.request = request;
    this.storeRepository = storeRepository;
    this.offsetManager = offsetManager;
  }

  private void handleSingleGetRequest(GetRouterRequest singleGetReq) {
    int partition = singleGetReq.getPartition();
    String topic = singleGetReq.getResourceName();
    byte[] key = singleGetReq.getKeyBytes();

    AbstractStorageEngine store = storeRepository.getLocalStorageEngine(topic);

    // TODO: We have a more up to date (and less expensive to call) copy of this data in memory. We should use it.
    long offset = offsetManager.getLastOffset(topic, partition).getOffset();

    long queryStartTime = System.nanoTime();
    byte[] value = store.get(partition, key);
    long bdbQueryLatency = System.nanoTime() - queryStartTime;
    StorageResponseObject response;
    if( value != null) {
      response = new StorageResponseObject(value, offset);
    } else {
      response = new StorageResponseObject(offset);
    }
    response.setBdbQueryLatency(bdbQueryLatency);
    ctx.writeAndFlush(response);
  }

  /**
   * TODO: need to evaluate whether Storage node should do parallel lookup to reduce multi-get latency.
   * @param multiGetReq
   */
  private void handleMultiGetRequest(MultiGetRouterRequestWrapper multiGetReq) {
    String topic = multiGetReq.getResourceName();
    Iterable<MultiGetRouterRequestKeyV1> keys = multiGetReq.getKeys();

    AbstractStorageEngine store = storeRepository.getLocalStorageEngine(topic);
    MultiGetResponseWrapper responseWrapper = new MultiGetResponseWrapper();

    Set<Integer> partitionIdSet = new HashSet<>();
    keys.forEach( routerRequestKey -> {
      int partitionId = routerRequestKey.partitionId;
      if (!partitionIdSet.contains(partitionId)) {
        partitionIdSet.add(partitionId);
        // TODO: We have a more up to date (and less expensive to call) copy of this data in memory. We should use it.
        long offset = offsetManager.getLastOffset(topic, partitionId).getOffset();
        responseWrapper.addPartitionOffsetMapping(partitionId, offset);
      }
    });

    long queryStartTime = System.nanoTime();
    for (MultiGetRouterRequestKeyV1 key : keys) {
      int partitionId = key.partitionId;
      byte[] value = store.get(partitionId, key.keyBytes.array());
      if (null != value) {
        ValueRecord valueRecord = ValueRecord.parseAndCreate(value);

        MultiGetResponseRecordV1 record = new MultiGetResponseRecordV1();
        record.keyIndex = key.keyIndex;
        record.value = valueRecord.getDataNIOByteBuf();
        record.schemaId = valueRecord.getSchemaId();
        responseWrapper.addRecord(record);
      }
    };
    responseWrapper.setBdbQueryLatency(System.nanoTime() - queryStartTime);
    ctx.writeAndFlush(responseWrapper);
  }

  @Override
  public void run() {
    try {
      switch (request.getRequestType()) {
        case SINGLE_GET:
          handleSingleGetRequest((GetRouterRequest) request);
          break;
        case MULTI_GET:
          handleMultiGetRequest((MultiGetRouterRequestWrapper) request);
          break;
        default:
          throw new VeniceException("Unknown request type: " + request.getRequestType());
      }
    } catch (Exception e) {
      ctx.writeAndFlush(new HttpShortcutResponse(e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR));
      ctx.close();
    }
  }
}
