package com.linkedin.venice.D2;

import com.linkedin.common.callback.Callback;
import com.linkedin.common.util.None;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import org.apache.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.linkedin.venice.HttpConstants.*;


public class D2ClientUtils {
  private static Logger logger = Logger.getLogger(D2ClientUtils.class);
  private static long DEFAULT_D2_STARTUP_TIMEOUT_MS = 5000;
  private static long DEFAULT_D2_SHUTDOWN_TIMEOUT_MS = 60000;

  static public void startClient(D2Client client) {
    startClient(client, DEFAULT_D2_STARTUP_TIMEOUT_MS);
  }

  static public void startClient(D2Client client, long timeoutInMs) {
    CountDownLatch latch = new CountDownLatch(1);

    client.start(new Callback<None>() {
      @Override
      public void onError(Throwable e) {
        latch.countDown();
        logger.error("D2 client failed to startup", e);
      }

      @Override
      public void onSuccess(None result) {
        latch.countDown();
        logger.info("D2 client started successfully");
      }
    });

    try {
      latch.await(timeoutInMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new VeniceException("latch wait was interrupted, d2 client may not have had enough time to startup", e);
    }

    if (latch.getCount() > 0) {
      throw new VeniceException("Time out after " + timeoutInMs + "ms waiting for D2 client to startup");
    }
  }

  static public void shutdownClient(D2Client client) {
    shutdownClient(client, DEFAULT_D2_SHUTDOWN_TIMEOUT_MS);
  }

  static public void shutdownClient(D2Client client, long timeoutInMs) {
    long startTime = System.nanoTime();
    CountDownLatch latch = new CountDownLatch(1);

    try {
      client.shutdown(new Callback<None>() {
        @Override
        public void onError(Throwable e) {
          latch.countDown();
          logger.error("Error when shutting down D2 client", e);
        }

        @Override
        public void onSuccess(None result) {
          latch.countDown();
          logger.info("D2 client shutdown completed");
        }
      });
    } catch (RejectedExecutionException e) {
      logger.warn("D2 client cannot initiate asynchronous shutdown procedure. "
          + "It may be already closed or in a bad state. Will assume that the client is shut down and proceed.", e);
      latch.countDown();
    }

    try {
      latch.await(timeoutInMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new VeniceException("latch wait was interrupted, d2 client may not have had enough time to shutdown", e);
    } finally {
      logger.info("D2 client shutdown took " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime) + "ms.");
    }

    if (latch.getCount() > 0) {
      throw new VeniceException("Time out after " + timeoutInMs + "ms waitinig for D2 client to shutdown");
    }
  }

  public static RestRequest createD2GetRequest(String requestPath) {
    return createD2GetRequest(requestPath, Collections.EMPTY_MAP);
  }

  public static RestRequest createD2GetRequest(String requestPath, Map<String, String> headers) {
    URI  requestUri;
    try {
      requestUri = new URI(requestPath);
    } catch (URISyntaxException e) {
      throw new VeniceException("Failed to create URI for path " + requestPath, e);
    }

    return new RestRequestBuilder(requestUri).setMethod("get").setHeaders(headers).build();
  }

   static public RestResponse sendD2GetRequest(String requestPath, D2Client client) {
     RestResponse response;
     try {
       response = client.restRequest(createD2GetRequest(requestPath)).get();
     } catch (Exception e) {
       throw new VeniceException("D2 client failed to sent request, " + requestPath, e);
     }

     return response;
   }

  public static RestRequest createD2PostRequest(String requestPath, Map<String, String> headers, byte[] body, int keyCount) {
    URI requestUri;
    try {
      requestUri = new URI(requestPath);
    } catch (URISyntaxException e) {
      throw new VeniceException("Failed to create URI for path " + requestPath, e);
    }

    RestRequestBuilder builder = new RestRequestBuilder(requestUri).setMethod("post").setHeaders(headers).setEntity(body);
    if (keyCount != 0) {
      builder.setHeader(VENICE_KEY_COUNT, Integer.toString(keyCount));
    }

    return builder.build();
  }
}
