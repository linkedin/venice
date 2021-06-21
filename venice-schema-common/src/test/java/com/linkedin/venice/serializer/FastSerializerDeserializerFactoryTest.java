package com.linkedin.venice.serializer;

import com.linkedin.venice.read.protocol.response.streaming.StreamingFooterRecordV1;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class FastSerializerDeserializerFactoryTest {

  @Test(timeOut = 300000)
  public void concurrentFastAvroVerification() throws InterruptedException {
    String javaVersion = System.getProperty("java.version");
    if (javaVersion.startsWith("11.")) {
      /**
       * Important: Skip the test for JDK11 since the following assertion will fail.
       * There are behavior changes between JDK8 and JDK11, and we have reported this issue to the JDK team to figure
       * out the root cause.
       */
      return;
    }
    int concurrentNum = 5;
    AtomicInteger fastClassGenCountForGeneric = new AtomicInteger(0);
    AtomicInteger fastClassGenCountForSpecific = new AtomicInteger(0);
    ExecutorService executor = Executors.newFixedThreadPool(concurrentNum);
    for (int i = 0; i < concurrentNum; ++i) {
      executor.submit(() -> {
        if (FastSerializerDeserializerFactory.verifyWhetherFastSpecificDeserializerWorks(
            StreamingFooterRecordV1.class)) {
          fastClassGenCountForSpecific.getAndIncrement();
        }
        if (FastSerializerDeserializerFactory.verifyWhetherFastGenericDeserializerWorks()) {
          fastClassGenCountForGeneric.getAndIncrement();
        }
      });
    }
    executor.shutdownNow();
    executor.awaitTermination(300000, TimeUnit.MILLISECONDS);
    Assert.assertEquals(fastClassGenCountForGeneric.get(), 1);
    Assert.assertEquals(fastClassGenCountForSpecific.get(), 1);
  }
}
