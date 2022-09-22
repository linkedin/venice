package com.linkedin.alpini.io;

import com.linkedin.alpini.base.concurrency.CompletableFutureTask;
import com.linkedin.alpini.base.concurrency.ExecutorService;
import com.linkedin.alpini.base.concurrency.Executors;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 1/24/18.
 */
public class TestCompressOutputStream {
  @Test(groups = "unit")
  public void testBasic() throws IOException {
    ExecutorService ex = Executors.newCachedThreadPool();
    try {

      byte[] input = "This is a quick and simple test of the test foo blah".getBytes(StandardCharsets.US_ASCII);

      CompletableFuture<Void> task1;
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      try (CompressOutputStream os = new CompressOutputStream(5, ex, 1, bos)) {
        os.write(input);
        os.write(input);
        os.write(input);

        CompletableFutureTask<Void> task = new CompletableFutureTask<>(() -> {
          os.await();
          return null;
        });
        task1 = task.toCompletableFuture();
        ex.execute(task);

        os.write(input);
        os.write(input);
      }
      task1.join();

      byte[] bytes1 = bos.toByteArray();

      CompletableFuture<Void> task2;
      bos = new ByteArrayOutputStream();
      try (CompressOutputStream os = new CompressOutputStream(5, ex, 4, bos)) {
        os.write(input);
        os.write(input);

        CompletableFutureTask<Void> task = new CompletableFutureTask<>(() -> {
          os.await(1, TimeUnit.SECONDS);
          return null;
        });
        task2 = task.toCompletableFuture();
        ex.execute(task);

        os.write(input);
        os.write(input);
        os.write(input);
      }
      task2.join();

      byte[] bytes2 = bos.toByteArray();

      Assert.assertTrue(bytes1.length < input.length * 5);
      Assert.assertTrue(bytes2.length < input.length * 5);

    } finally {
      ex.shutdownNow();
    }
  }

}
