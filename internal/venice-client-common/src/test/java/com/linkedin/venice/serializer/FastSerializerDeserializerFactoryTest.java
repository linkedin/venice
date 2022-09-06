package com.linkedin.venice.serializer;

import com.linkedin.venice.read.protocol.response.streaming.StreamingFooterRecordV1;
import com.linkedin.venice.utils.TestUtils;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class FastSerializerDeserializerFactoryTest {
  @Test(timeOut = 300000)
  public void concurrentFastAvroVerification() throws InterruptedException {
    int concurrentNum = 5;
    AtomicInteger fastClassGenCountForGeneric = new AtomicInteger(0);
    AtomicInteger fastClassGenCountForSpecific = new AtomicInteger(0);
    ExecutorService executor = Executors.newFixedThreadPool(concurrentNum);
    try {
      for (int i = 0; i < concurrentNum; ++i) {
        executor.submit(() -> {
          if (FastSerializerDeserializerFactory
              .verifyWhetherFastSpecificDeserializerWorks(StreamingFooterRecordV1.class)) {
            fastClassGenCountForSpecific.getAndIncrement();
          }
          if (FastSerializerDeserializerFactory.verifyWhetherFastGenericDeserializerWorks()) {
            fastClassGenCountForGeneric.getAndIncrement();
          }
        });
      }
      executor.shutdown();
      executor.awaitTermination(300000, TimeUnit.MILLISECONDS);
      Assert.assertEquals(fastClassGenCountForGeneric.get(), 1);
      Assert.assertEquals(fastClassGenCountForSpecific.get(), 1);
    } finally {
      TestUtils.shutdownExecutor(executor);
    }
  }
}
