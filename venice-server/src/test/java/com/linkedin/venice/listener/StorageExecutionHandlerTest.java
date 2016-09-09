package com.linkedin.venice.listener;

import com.linkedin.venice.message.GetRequestObject;
import com.linkedin.venice.offsets.BdbOffsetManager;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.record.ValueRecord;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

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

    GetRequestObject testRequest = new GetRequestObject(topic.toCharArray(), partition, keyString.getBytes());

    AbstractStorageEngine testStore = mock(AbstractStorageEngine.class);
    doReturn(valueBytes).when(testStore).get(partition, keyString.getBytes());

    StoreRepository testRepository = mock(StoreRepository.class);
    doReturn(testStore).when(testRepository).getLocalStorageEngine(topic);

    OffsetRecord mockOffset = mock(OffsetRecord.class);
    doReturn(expectedOffset).when(mockOffset).getOffset();
    OffsetManager mockOffsetManager = mock(BdbOffsetManager.class);
    doReturn(mockOffset).when(mockOffsetManager).getLastOffset(Mockito.anyString(), Mockito.anyInt());

    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    doReturn(new UnpooledByteBufAllocator(true)).when(mockCtx).alloc();
    when(mockCtx.writeAndFlush(any())).then(i -> {
      outputArray.add(i.getArguments()[0]);
      return null;
    });

    ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(2));

    //Actual test
    StorageExecutionHandler testHandler = new StorageExecutionHandler(threadPoolExecutor, testRepository, mockOffsetManager);
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
