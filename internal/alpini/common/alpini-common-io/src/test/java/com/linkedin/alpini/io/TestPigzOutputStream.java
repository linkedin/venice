package com.linkedin.alpini.io;

import com.linkedin.alpini.base.concurrency.ExecutorService;
import com.linkedin.alpini.base.concurrency.Executors;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.zip.CRC32;
import java.util.zip.GZIPInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * @author Antony T Curtis <acurtis@linkedin.com>
 */
public class TestPigzOutputStream {
  private static final Logger LOG = LogManager.getLogger(TestPigzOutputStream.class);

  private ExecutorService _executor;

  @BeforeClass(groups = { "unit", "functional" })
  public void beforeClass() {
    _executor = Executors.newCachedThreadPool();
  }

  @AfterClass(groups = { "unit", "functional" }, alwaysRun = true)
  public void afterClass() {
    _executor.shutdown();
  }

  @DataProvider
  public Object[][] concurrencyGenerator() {
    int availableProcessors = Runtime.getRuntime().availableProcessors();
    availableProcessors = Math.min(6, Math.max(2, availableProcessors / 2));
    List<Object[]> result = new ArrayList<Object[]>();
    for (int concurrency = 1; concurrency <= availableProcessors; concurrency++)
      result.add(new Object[] { 9, concurrency });
    for (int concurrency = 1; concurrency <= availableProcessors; concurrency++)
      result.add(new Object[] { 6, concurrency });
    for (int concurrency = 1; concurrency <= availableProcessors; concurrency++)
      result.add(new Object[] { 1, concurrency });
    return result.toArray(new Object[result.size()][]);
  }

  @Test(groups = { "functional" }, singleThreaded = true, dataProvider = "concurrencyGenerator", enabled = false)
  public void testPigzToNullConcurrency(int compressionLevel, int numberOfThreads) throws IOException {
    long start = System.nanoTime();
    PigzOutputStream os = new PigzOutputStream(compressionLevel, _executor, numberOfThreads, new NullOutputStream());
    for (int i = 0; i < 10000000; i++) {
      os.write(("Hello world " + i).getBytes());
    }
    os.close();

    double seconds = (System.nanoTime() - start) / 1000000000.0;
    LOG.info(
        String.format(
            "CompressionLevel %d Threads=%d took %.3f seconds to %d bytes at %.3f KB/s\n",
            compressionLevel,
            numberOfThreads,
            seconds,
            os.getBytesWritten(),
            os.getBytesCompressed() / (1024.0 * seconds)));
  }

  @Test(groups = { "unit" }, singleThreaded = true, dataProvider = "concurrencyGenerator")
  public void testPigzToDecompressConcurrency(int compressionLevel, int numberOfThreads) throws Exception {
    final PipedInputStream pis = new PipedInputStream();
    PigzOutputStream os =
        new PigzOutputStream(compressionLevel, _executor, numberOfThreads, new PipedOutputStream(pis));

    Future<Long> check = _executor.submit(new Callable<Long>() {
      private final InputStream is = new GZIPInputStream(pis);
      private final CRC32 crc = new CRC32();
      private final byte[] buffer = new byte[1024];

      @Override
      public Long call() throws Exception {
        for (;;) {
          int read = is.read(buffer);
          if (read == -1)
            break;
          crc.update(buffer, 0, read);
        }
        is.close();
        return crc.getValue();
      }
    });

    CRC32 crc = new CRC32();
    for (int i = 0; i < 10000; i++) {
      byte[] bytes = ("Hello world " + i).getBytes();
      crc.update(bytes);
      os.write(bytes);
    }
    os.close();
    Assert.assertEquals((long) check.get(), crc.getValue());
  }
}
