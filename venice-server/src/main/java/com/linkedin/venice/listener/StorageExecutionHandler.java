package com.linkedin.venice.listener;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.request.GetRouterRequest;
import com.linkedin.venice.listener.request.MultiGetRouterRequestWrapper;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.listener.response.HttpShortcutResponse;
import com.linkedin.venice.listener.response.MultiGetResponseWrapper;
import com.linkedin.venice.listener.response.StorageResponseObject;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.record.ValueRecord;
import com.linkedin.venice.utils.LatencyUtils;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.tehuti.utils.Time;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import javax.validation.constraints.NotNull;


/***
 * {@link StorageExecutionHandler} will take the incoming {@link RouterRequest}, and delegate the lookup request to
 * a thread pool {@link #executor}, which is being shared by all the requests.
 * Especially, this handler will execute parallel lookups for {@link MultiGetRouterRequestWrapper}.
 */
@ChannelHandler.Sharable
public class StorageExecutionHandler extends ChannelInboundHandlerAdapter {
  private final ExecutorService executor;
  private StoreRepository storeRepository;
  private final InMemoryOffsetRetriever offsetRetriever;

  public StorageExecutionHandler(@NotNull ExecutorService executor, @NotNull StoreRepository storeRepository,
      @NotNull InMemoryOffsetRetriever offsetRetriever) {
    this.executor = executor;
    this.storeRepository = storeRepository;
    this.offsetRetriever = offsetRetriever;
  }

  @Override
  public void channelRead(ChannelHandlerContext context, Object message) throws Exception {
    if (message instanceof RouterRequest) {
      RouterRequest request = (RouterRequest) message;
      try {
        switch (request.getRequestType()) {
          case SINGLE_GET:
            handleSingleGetRequest(context, (GetRouterRequest) request);
            break;
          case MULTI_GET:
            handleMultiGetRequest(context, (MultiGetRouterRequestWrapper) request);
            break;
          default:
            throw new VeniceException("Unknown request type: " + request.getRequestType());
        }
      } catch (Exception e) {
        context.writeAndFlush(new HttpShortcutResponse(e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR));
      }
    } else {
      context.writeAndFlush(new HttpShortcutResponse("Unrecognized object in StorageExecutionHandler",
          HttpResponseStatus.INTERNAL_SERVER_ERROR));
    }
  }

  private void handleSingleGetRequest(ChannelHandlerContext context, GetRouterRequest request)
  {
    executor.submit(() -> {
      int partition = request.getPartition();
      String topic = request.getResourceName();
      byte[] key = request.getKeyBytes();

      AbstractStorageEngine store = storeRepository.getLocalStorageEngine(topic);

      Optional<Long> offsetObj = offsetRetriever.getOffset(topic, partition);
      long offset = offsetObj.isPresent() ? offsetObj.get() : OffsetRecord.LOWEST_OFFSET;

      long queryStartTimeInNS = System.nanoTime();
      byte[] value = store.get(partition, key);
      double bdbQueryLatency = LatencyUtils.getLatencyInMS(queryStartTimeInNS);
      StorageResponseObject response;
      if( value != null) {
        response = new StorageResponseObject(value, offset);
      } else {
        response = new StorageResponseObject(offset);
      }
      response.setBdbQueryLatency(bdbQueryLatency);
      context.writeAndFlush(response);
    });
  }

  private void handleMultiGetRequest(ChannelHandlerContext context, MultiGetRouterRequestWrapper request)
  {
    String topic = request.getResourceName();
    Iterable<MultiGetRouterRequestKeyV1> keys = request.getKeys();
    AbstractStorageEngine store = storeRepository.getLocalStorageEngine(topic);

    long queryStartTimeInNS = System.nanoTime();
    List<CompletableFuture<Optional<MultiGetResponseRecordV1>>> futureList = new ArrayList<>();
    for (MultiGetRouterRequestKeyV1 key : keys) {
      futureList.add(CompletableFuture.supplyAsync(() -> {
        int partitionId = key.partitionId;
        byte[] value = store.get(partitionId, key.keyBytes.array());
        if (null != value) {
          ValueRecord valueRecord = ValueRecord.parseAndCreate(value);

          MultiGetResponseRecordV1 record = new MultiGetResponseRecordV1();
          record.keyIndex = key.keyIndex;
          record.value = valueRecord.getDataNIOByteBuf();
          record.schemaId = valueRecord.getSchemaId();
          return Optional.of(record);
        }
        return Optional.empty();
      }, executor));
    }
    MultiGetResponseWrapper responseWrapper = new MultiGetResponseWrapper();
    // Wait for all the lookup requests to complete
    CompletableFuture<Void> allDoneFuture = CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]));
    allDoneFuture.handle( (aVoid, throwable) -> {
      for (CompletableFuture<Optional<MultiGetResponseRecordV1>> future : futureList) {
        try {
          Optional<MultiGetResponseRecordV1> record = future.get();
          if (record.isPresent()) {
            responseWrapper.addRecord(record.get());
          }
        } catch (Exception e) {
          context.writeAndFlush(new HttpShortcutResponse(e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR));
          break;
        }
      }
      double bdbQueryLatency = LatencyUtils.getLatencyInMS(queryStartTimeInNS);
      responseWrapper.setBdbQueryLatency(bdbQueryLatency);

      // Offset data
      Set<Integer> partitionIdSet = new HashSet<>();
      keys.forEach( routerRequestKey -> {
        int partitionId = routerRequestKey.partitionId;
        if (!partitionIdSet.contains(partitionId)) {
          partitionIdSet.add(partitionId);
          Optional<Long> offsetObj = offsetRetriever.getOffset(topic, partitionId);
          long offset = offsetObj.isPresent() ? offsetObj.get() : OffsetRecord.LOWEST_OFFSET;
          responseWrapper.addPartitionOffsetMapping(partitionId, offset);
        }
      });

      context.writeAndFlush(responseWrapper);
      return aVoid;
    });
  }
}
