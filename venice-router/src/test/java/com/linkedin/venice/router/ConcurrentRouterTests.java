package com.linkedin.venice.router;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.utils.FlakyTestRetryAnalyzer;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ConcurrentRouterTests {

  private static final Logger LOGGER = Logger.getLogger(ConcurrentRouterTests.class);
  private int invocationCount;
  private List<Thread> threadList;
  private boolean timeOut;
  private VeniceClusterWrapper venice;

  @BeforeClass
  public void initialSetUp() {
    Utils.thisIsLocalhost();
    invocationCount = 0;
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    threadList = new ArrayList<>();
    timeOut = true;
  }

  @AfterMethod(alwaysRun = true)
  public void cleanUp() {
    // N.B.: AfterMethod logs are not visible in individual test runs... need to look in the global ConcurrentRouterTests log.
    LOGGER.info("cleanUp invoked for invocationCount " + invocationCount);
    try {
      int threadProcessed = 0, threadAlive = 0;
      for (Thread t : threadList){
        threadProcessed++;
        if (t.isAlive()) {
          threadAlive++;
          t.interrupt();
          try {
            t.join();
          } catch (InterruptedException e) {
            LOGGER.error("Interrupted while trying to join a lingering thread ", e);
          }
        }
      }
      LOGGER.info("cleanUp finished for invocationCount " + invocationCount + (timeOut ? " which timed out" : "") + ". " +
          threadAlive + "/" + threadProcessed + " threads were still alive and have been interrupted.");
      if (null == venice) {
        LOGGER.info("VeniceCluster not initialized during invocation " + invocationCount);
      } else {
        IOUtils.closeQuietly(venice);
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception during cleanUp.", e);
    }
    invocationCount++;
  }

  /**
   * This is a slow test, ~ 10 seconds.  It probably doesn't need to run every time we run the test suite.
   * It makes a bunch of concurrent requests to both the ssl and non-ssl ports of the router.  If something is
   * thread starved, then a request will hang and timeout, failing this test.
   * @throws InterruptedException
   */
  @Test(retryAnalyzer = FlakyTestRetryAnalyzer.class, timeOut = 20 * Time.MS_PER_SECOND)
  public void routerDoesntHangOnConcurrentSslAndNonSslRequests() throws InterruptedException {
    try {
      venice = ServiceFactory.getVeniceCluster();
      //Assert.fail in a thread doesn't get caught by the test, so we save any failure messages until the end.
      List<String> threadFailures = new ArrayList<>();

      String url = venice.getRandomRouterURL();
      String sslUrl = venice.getRandomRouterSslURL();
      int timeout = 5000;
      for (int i=0; i<200; i++){
        CloseableHttpAsyncClient client = SslUtils.getMinimalHttpClient(2,2, Optional.of(SslUtils.getLocalSslFactory()));
        client.start();
        HttpGet request = new HttpGet((i >= 100 ? url : sslUrl) + "/master_controller"); //half normal, half ssl
        Thread t = new Thread(()-> {
          for (int j=0; j<10; j++) { // each thread makes 10 requests before exiting
            try {
              Thread.sleep(1);
              HttpResponse response = client.execute(request, null).get(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e){
              System.out.println("Thread interrupted");
              break;
            } catch (ExecutionException e) {
              threadFailures.add("Execution exception on " + request.getURI().toString());
            } catch (TimeoutException e) {
              threadFailures.add("exceeded " + timeout + "ms timeout on " + request.getURI().toString());
            }
          }
          try {
            client.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        });
        t.start();
        threadList.add(t);
      }
      for (Thread t : threadList){
        t.join(0);
      }
      for (String s : threadFailures){
        Assert.fail(s);
      }
    } finally {
      // Finally is not even called when timing out!
      LOGGER.info("Finally block invoked.");
      timeOut = false;
      LOGGER.info("Finally block finished.");
    }
  }
}
