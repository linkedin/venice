package com.linkedin.alpini.jna;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = "unit")
public class TestThreadUtils {
  public void testGetPlatform() {
    Assert.assertNotEquals(ThreadUtils.getPlatform().toString(), "UNKNOWN");
  }

  public void testSetThreadName() throws Exception {

    ExecutorService executorService =
        Executors.newSingleThreadExecutor(ThreadUtils.decorateName(Executors.defaultThreadFactory()));
    try {

      executorService.submit(() -> System.out.println("Hello world")).get();

    } finally {
      executorService.shutdown();
    }
  }

}
