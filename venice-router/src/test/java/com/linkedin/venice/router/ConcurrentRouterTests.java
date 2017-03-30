package com.linkedin.venice.router;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ConcurrentRouterTests {

  /**
   * This is a slow test, ~ 10 seconds.  It probably doesn't need to run every time we run the test suite.
   * It makes a bunch of concurrent requests to both the ssl and non-ssl ports of the router.  If something is
   * thread starved, then a request will hang and timeout, failing this test.
   * @throws InterruptedException
   */
  @Test
  public void routerDoesntHangOnConcurrentSslAndNonSslRequests() throws InterruptedException {
    Utils.thisIsLocalhost();
    VeniceClusterWrapper venice = ServiceFactory.getVeniceCluster();
    List<Thread> threadList = new ArrayList<>();
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
      t.join(5000);
    }
    for (String s : threadFailures){
      Assert.fail(s);
    }
    venice.close();

  }
}
