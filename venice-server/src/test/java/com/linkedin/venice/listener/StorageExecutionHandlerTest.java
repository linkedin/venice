package com.linkedin.venice.listener;

import com.linkedin.venice.listener.request.GetRouterRequest;
import com.linkedin.venice.listener.response.StorageResponseObject;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.storage.MetadataRetriever;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.record.ValueRecord;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.router.api.VenicePathParser.*;
import static org.mockito.Mockito.*;

public class StorageExecutionHandlerTest {
  @Test
  public static void storageExecutionHandlerPassesRequestsAndGeneratesResponses()
      throws Exception {
    String topic = "temp-test-topic_v1";
    String keyString = "testkey";
    String valueString = "testvalue";
    int schemaId = 1;
    int partition = 3;
    long expectedOffset = 12345L;
    List<Object> outputArray = new ArrayList<Object>();
    byte[] valueBytes = ValueRecord.create(schemaId, valueString.getBytes()).serialize();
    //[0]""/[1]"action"/[2]"store"/[3]"partition"/[4]"key"
    String uri = "/" + TYPE_STORAGE + "/" + topic + "/" + partition + "/" + keyString;
    HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    GetRouterRequest testRequest = GetRouterRequest.parseGetHttpRequest(httpRequest);

    AbstractStorageEngine testStore = mock(AbstractStorageEngine.class);
    doReturn(valueBytes).when(testStore).get(partition, ByteBuffer.wrap(keyString.getBytes()));

    StoreRepository testRepository = mock(StoreRepository.class);
    doReturn(testStore).when(testRepository).getLocalStorageEngine(topic);

    MetadataRetriever mockMetadataRetriever = mock(MetadataRetriever.class);
    doReturn(Optional.of(expectedOffset)).when(mockMetadataRetriever).getOffset(Mockito.anyString(), Mockito.anyInt());

    ReadOnlySchemaRepository schemaRepo = mock(ReadOnlySchemaRepository.class);

    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    doReturn(new UnpooledByteBufAllocator(true)).when(mockCtx).alloc();
    when(mockCtx.writeAndFlush(any())).then(i -> {
      outputArray.add(i.getArguments()[0]);
      return null;
    });

    ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(2));

    //Actual test
    StorageExecutionHandler testHandler = new StorageExecutionHandler(threadPoolExecutor, threadPoolExecutor, testRepository, schemaRepo,
        mockMetadataRetriever, null);
    testHandler.channelRead(mockCtx, testRequest);

    //Wait for async stuff to finish
    int count = 1;
    while (outputArray.size()<1) {
      Thread.sleep(10); //on my machine, consistently fails with only 10ms, intermittent at 15ms, success at 20ms
      count +=1;
      if (count > 200){ // two seconds
        throw new RuntimeException("Timeout waiting for StorageExecutionHandler output to appear");
      }
    }

    //parsing of response
    Assert.assertEquals(outputArray.size(), 1);
    StorageResponseObject obj = (StorageResponseObject) outputArray.get(0);
    byte[] response = obj.getValueRecord().getDataInBytes();
    long offset = obj.getOffset();

    //Verification
    Assert.assertEquals(response, valueString.getBytes());
    Assert.assertEquals(offset, expectedOffset);
    Assert.assertEquals(obj.getValueRecord().getSchemaId(), schemaId);
  }

}
