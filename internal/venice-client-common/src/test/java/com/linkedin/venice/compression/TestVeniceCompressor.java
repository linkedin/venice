package com.linkedin.venice.compression;

import com.github.luben.zstd.Zstd;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestVeniceCompressor {
  private static final Logger LOGGER = LogManager.getLogger(TestVeniceCompressor.class);
  private static final long TEST_TIMEOUT = 5 * Time.MS_PER_SECOND;

  @DataProvider(name = "Stateless-Compressor")
  public static Object[][] statelessCompressorProvider() {
    return new Object[][] { { CompressionStrategy.NO_OP }, { CompressionStrategy.GZIP } };
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testMultiThreadZstdCompression() throws IOException {
    byte[] dictionary = ZstdWithDictCompressor.buildDictionaryOnSyntheticAvroData();
    try (VeniceCompressor compressor =
        new CompressorFactory().createCompressorWithDictionary(dictionary, Zstd.maxCompressionLevel())) {
      runTests(compressor);
    }
  }

  @Test(dataProvider = "Stateless-Compressor", timeOut = TEST_TIMEOUT)
  public void testMultiThreadCompression(CompressionStrategy compressionStrategy) throws IOException {
    try (VeniceCompressor compressor = new CompressorFactory().getCompressor(compressionStrategy)) {
      runTests(compressor);
    }
  }

  private void runTests(VeniceCompressor compressor) {
    runTestInternal(compressor, SourceDataType.DIRECT_BYTE_BUFFER);
    runTestInternal(compressor, SourceDataType.NON_DIRECT_BYTE_BUFFER);
    runTestInternal(compressor, SourceDataType.BYTE_ARRAY);
  }

  private void runTestInternal(VeniceCompressor compressor, SourceDataType type) {
    int threadPoolSize = 1;
    int numRunnables = 1000;
    ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);
    List<Future> compressionFutures = new ArrayList<>(numRunnables);
    try {
      for (int i = 0; i < numRunnables; i++) {
        Random rd = new Random();
        byte[] data = new byte[50];
        Runnable runnable = () -> {
          rd.nextBytes(data);
          try {
            ByteBuffer dataBuffer;
            ByteBuffer deflatedData;
            ByteBuffer reinflatedData;
            switch (type) {
              case DIRECT_BYTE_BUFFER:
                dataBuffer = ByteBuffer.allocateDirect(data.length);
                dataBuffer.put(data);
                dataBuffer.position(0);
                deflatedData = compressor.compress(dataBuffer, ByteUtils.SIZE_OF_INT);
                reinflatedData = compressor.decompress(deflatedData);
                Assert.assertEquals(reinflatedData, dataBuffer);
                break;
              case NON_DIRECT_BYTE_BUFFER:
                dataBuffer = ByteBuffer.wrap(data);
                deflatedData = compressor.compress(dataBuffer, ByteUtils.SIZE_OF_INT);
                reinflatedData = compressor.decompress(deflatedData);
                Assert.assertEquals(reinflatedData, dataBuffer);
                break;
              case BYTE_ARRAY:
                byte[] deflated = compressor.compress(data);
                reinflatedData = compressor.decompress(deflated, 0, deflated.length);
                Assert.assertEquals(reinflatedData.array(), data);
                break;
              default: // Defensive code
                break;
            }
          } catch (Exception e) {
            LOGGER.error(e);
            throw new RuntimeException(e);
          }
        };
        compressionFutures.add(executorService.submit(runnable));
      }
    } finally {
      executorService.shutdown();
    }

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, false, true, () -> {
      try {
        executorService.awaitTermination(1, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Assert.fail();
      }
    });

    try {
      for (Future compressionFuture: compressionFutures) {
        compressionFuture.get();
      }
    } catch (Throwable t) {
      Assert.fail("Compression must succeed", t);
    }
  }

  private enum SourceDataType {
    DIRECT_BYTE_BUFFER, NON_DIRECT_BYTE_BUFFER, BYTE_ARRAY
  }
}
