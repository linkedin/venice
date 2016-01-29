package com.linkedin.venice.listener;

import com.linkedin.venice.message.GetRequestObject;
import com.linkedin.venice.message.GetResponseObject;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.store.AbstractStorageEngine;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StorageExecutionHandlerTest {
  @Test
  public static void storageExecutionHandlerPassesRequestsAndGeneratesResponses()
      throws Exception {
    String topic = "temp-test-topic";
    String keyString = "testkey";
    String valueString = "testvalue";
    int partition = 3;
    List<Object> outputArray = new ArrayList<Object>();

    GetRequestObject testRequest = new GetRequestObject(topic.toCharArray(), partition, keyString.getBytes());

    AbstractStorageEngine testStore = mock(AbstractStorageEngine.class);
    when(testStore.get(partition, keyString.getBytes())).thenReturn(valueString.getBytes());

    StoreRepository testRepository = mock(StoreRepository.class);
    when(testRepository.getLocalStorageEngine(topic)).thenReturn(testStore);

    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    when(mockCtx.alloc()).thenReturn(new UnpooledByteBufAllocator(true));
    when(mockCtx.writeAndFlush(any())).then(i -> {
      outputArray.add(i.getArguments()[0]);
      return null;
    });

    ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(2));

    //Actual test
    StorageExecutionHandler testHandler = new StorageExecutionHandler(threadPoolExecutor, testRepository);
    testHandler.channelRead(mockCtx, testRequest);

    //Wait for async stuff to finish
    int count = 1;
    while (outputArray.size()<1) {
      Thread.sleep(10); //on my machine, consistenly fails with only 10ms, intermittent at 15ms, success at 20ms
      count +=1;
      if (count > 200){ // two seconds
        throw new RuntimeException("Timeout waiting for StorageExecutionHandler output to appear");
      }
    }

    //parsing of response
    Assert.assertEquals(outputArray.size(), 1);
    byte[] response = (byte[]) outputArray.get(0);

    //Verification
    Assert.assertEquals(response, valueString.getBytes());
  }
}
