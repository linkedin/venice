package com.linkedin.venice.compression;

import com.github.luben.zstd.Zstd;
import com.linkedin.venice.utils.TestUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestVeniceCompressor {
  private static final Logger LOGGER = LogManager.getLogger(TestVeniceCompressor.class);

  @DataProvider(name = "Stateless-Compressor")
  public static Object[][] statelessCompressorProvider() {
    return new Object[][] { { CompressionStrategy.NO_OP }, { CompressionStrategy.GZIP } };
  }

  @Test
  public void testMultiThreadZstdCompression() throws IOException {
    byte[] dictionary = ZstdWithDictCompressor.buildDictionaryOnSyntheticAvroData();
    try (VeniceCompressor compressor =
        new CompressorFactory().createCompressorWithDictionary(dictionary, Zstd.maxCompressionLevel())) {
      runTest(compressor);
    }
  }

  @Test(dataProvider = "Stateless-Compressor")
  public void testMultiThreadCompression(CompressionStrategy compressionStrategy) throws IOException {
    try (VeniceCompressor compressor = new CompressorFactory().getCompressor(compressionStrategy)) {
      runTest(compressor);
    }
  }

  private void runTest(VeniceCompressor compressor) {
    int threadPoolSize = 100;
    int numThreads = threadPoolSize * 10;
    ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);
    try {
      for (int i = 0; i < numThreads; i++) {
        Runnable runnable = () -> {
          Random rd = new Random();
          byte[] data = new byte[50];
          rd.nextBytes(data);
          ByteBuffer dataBuffer = ByteBuffer.wrap(data);
          try {
            compressor.compress(dataBuffer);
          } catch (Exception e) {
            LOGGER.error(e);
            throw new RuntimeException(e);
          }
        };
        executorService.submit(runnable);
      }
    } finally {
      executorService.shutdown();
    }

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
      try {
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
        Assert.fail();
      }
    });
  }
}
