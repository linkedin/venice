package com.linkedin.alpini.io;

import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * @author Antony T Curtis <acurtis@linkedin.com>
 */
public class TestRateLimitedStream {
  private static final Logger LOG = LogManager.getLogger(TestRateLimitedStream.class);

  private static final double ERROR_THRESHOLD = 5; /* 5 % error */

  @DataProvider
  public Object[][] unitTestRates(ITestContext iTestContext) {
    return new Object[][] { { 1000000L, 5000000L }, // 1 MB/s, 5MB
        { 10000000L, 50000000L }, // 10 MB/s, 50MB

        // Slow speeds requested by kachandr
        { 3360L, 10000L }, // 33.6 Kbaud!
        { 240L, 1000L }, // 2400 baud!
        { 30L, 90L }, // 300 baud!
        { 5, 50 } // morse code speed! Yes, this speed was specifically requested.
    };
  }

  @DataProvider
  public Object[][] functionalTestRates() {
    return new Object[][] { { 1000000L, 10000000L }, // 1 MB/s, 10MB
        { 10000000L, 100000000L }, // 10 MB/s, 100MB
        { 100000000L, 1000000000L }, // 100 MB/s, 1000MB
    };
  }

  /**
   * Unit tests with shorter run time
   */
  @Test(groups = { "unit" }, dataProvider = "unitTestRates")
  public void rateLimiterUnitTest(long rate, long length) throws IOException {
    rateLimiterTest(rate, length);
  }

  /**
   * Functional tests with longer run time
   */
  @Test(groups = { "functional" }, dataProvider = "functionalTestRates")
  public void rateLimiterFunctionalTest(long rate, long length) throws IOException {
    rateLimiterTest(rate, length);
  }

  /**
   * Verify RateLimitedStream by comparing the target rate and actual rate when streaming data
   * with various length
   *
   * @param rate target rate of the stream
   * @param length length of the data to be streamed
   * @throws IOException
   */
  private void rateLimiterTest(long rate, long length) throws IOException {
    int retries = 1;
    double error;
    long actualRate;
    do {
      NullOutputStream sink = new NullOutputStream();
      RateLimitedStream copy = new RateLimitedStream(LOG, new ZeroInputStream(length), sink, rate, false);
      copy.run();
      actualRate = copy.getActualBps();

      error = Math.abs((rate - actualRate) * 100.0 / rate);

      LOG.info("Length={} Requested Rate {} ... actual rate {}, error={}%", length, rate, actualRate, error);

      Assert.assertTrue(copy.getCompletionFuture().isSuccess());
      Assert.assertEquals(sink.getBytesWritten(), length);
    } while (retries-- > 0 && (error > ERROR_THRESHOLD && Math.abs(rate - actualRate) > 10)); // within 5 % or total
                                                                                              // difference is small
                                                                                              // than 10 bytes

    Assert.assertTrue(retries >= 0);
  }

  @DataProvider
  public Object[][] processTests() {
    return new Object[][] { { 1, "Hello World" }, { 1, "" }, { 1, "This is a longer string for no apparent reason" },

        // ESPENG-5128 - because java.lang.Process may close the InputStream when the process exits before all the
        // output was drained, these tests with zero sleep may intermittently fail. This is a feature-bug in the JVM
        // implementation because there is no way to get the InputStream before the child process is exec. A hack
        // fix is possible using java.nio.channels.Pipe and is left as an exercise for some future engineer.
        // { 0, "Hello World, again"},
        // { 0, "" }
    };
  }

  @Test(groups = { "functional" }, dataProvider = "processTests")
  public void testProcess(int sleep, String text) throws IOException {
    ProcessBuilder builder = new ProcessBuilder("/bin/bash", "-c", "sleep " + sleep + "; echo -n \"" + text + "\"");
    NullOutputStream sink = new NullOutputStream();

    Process process = builder.redirectErrorStream(true).start();
    RateLimitedStream copy = new RateLimitedStream(LOG, process.getInputStream(), sink, 0, true);

    copy.run();

    Assert.assertTrue(copy.getCompletionFuture().isDone());
    Assert.assertNull(copy.getCompletionFuture().getCause());
    Assert.assertTrue(copy.getCompletionFuture().isSuccess());
    Assert.assertEquals(sink.getBytesWritten(), text.length());
  }

}
