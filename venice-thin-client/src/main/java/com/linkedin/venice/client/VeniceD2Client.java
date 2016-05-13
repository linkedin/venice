package com.linkedin.venice.client;

import com.google.common.net.HttpHeaders;
import com.linkedin.common.callback.Callback;
import com.linkedin.common.util.None;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.log4j.Logger;


/**
 * Client for making queries to Venice.
 * This client is threadsafe; re-use it.  There is ~130ms overhead on the first request as the client initializes.
 */
public class VeniceD2Client implements VeniceThinClient {
  private static final Logger logger = Logger.getLogger(VeniceD2Client.class);
  private static final long d2StartupTimeoutMs = TimeUnit.SECONDS.toMillis(10);
  private static final long d2ShutdownTimeoutMs = TimeUnit.SECONDS.toMillis(30);

  private final D2Client d2Client;
  private final String d2ServiceName;

  public VeniceD2Client(String zkConnection, String d2ServiceName) throws InterruptedException {
    this(zkConnection, d2ServiceName, "/d2", 5000, null);
  }
  public VeniceD2Client(String zkConnection, String d2ServiceName, String zkBasePath, int zkTimeout, String d2BackupFsPath) throws InterruptedException {
    this.d2ServiceName = d2ServiceName;
    D2ClientBuilder builder = new D2ClientBuilder().setZkHosts(zkConnection)
        .setZkSessionTimeout(zkTimeout, TimeUnit.MILLISECONDS)
        .setZkStartupTimeout(zkTimeout, TimeUnit.MILLISECONDS)
        .setLbWaitTimeout(zkTimeout, TimeUnit.MILLISECONDS)
        .setBasePath(zkBasePath);
    if (d2BackupFsPath != null){
      builder.setFsBasePath(d2BackupFsPath);
    }
    d2Client = builder.build();

    CountDownLatch latch = new CountDownLatch(1);
    AtomicBoolean d2StartupSuccess = new AtomicBoolean(false);
    d2Client.start(new Callback<None>() {
      @Override
      public void onError(Throwable e) {
        latch.countDown();
        throw new RuntimeException("d2client throws error on startup", e);
      }

      @Override
      public void onSuccess(None result) {
        d2StartupSuccess.set(true);
        latch.countDown();
      }
    });

    latch.await(d2StartupTimeoutMs, TimeUnit.MILLISECONDS);
    if (latch.getCount() > 0){
      throw new RuntimeException("Timed out after " + d2StartupTimeoutMs + "ms waiting for D2Client to startup");
    }
    if (!d2StartupSuccess.get()){
      throw new RuntimeException("d2client failed to startup");
    }
    logger.info("Successfully created VeniceD2Client");
  }

  /***
   * Get the value associated with your key from the specified store.
   * If you don't want to deal with the future and want to use this as a blocking client:
   *
   * byte[] value = client.get(store, key).get();
   *
   * Note: the .get() call can throw a java.util.concurrent.ExecutionException.
   * You can examine this with .getCause() to see the underlying VeniceClientException.
   * We throw a VeniceNotFoundException if the key is not found
   * We throw a VeniceServerErrorException if the server throws a 500 error
   * We throw a VeniceClientException for any other errors during the request.
   *
   * @param storeName
   * @param key
   * @return a Future wrapping the byte[] of the value associated with your key.
   */
  public Future<byte[]> get(String storeName, byte[] key){
    String b64key = encoder.encodeToString(key);
    String requestUrl = "d2://" + d2ServiceName +
        "/" + STORAGE_TYPE +
        "/" + storeName +
        "/" + b64key + B64_FORMAT;

    URI requestUri;
    try {
      requestUri = new URI(requestUrl);
    } catch (URISyntaxException e) {
      throw new RuntimeException("Failed to create URI for d2 client", e);
    }
    RestRequest request = new RestRequestBuilder(requestUri).setMethod("get").build();
    CompletableFuture<byte[]> valueFuture = new CompletableFuture<>();

    d2Client.restRequest(request, new Callback<RestResponse>() {
      @Override
      public void onError(Throwable e) {
        if (e instanceof RestException){ /* Get the RestResonse for status codes other than 200 */
          RestResponse result = ((RestException) e).getResponse();
          onSuccess(result);
        } else {
          logger.error(e);
          valueFuture.completeExceptionally(new VeniceClientException(e));
        }
      }

      @Override
      public void onSuccess(RestResponse result) {
        int statusCode = result.getStatus();
        String contentType = result.getHeader(HttpHeaders.CONTENT_TYPE);
        byte[] body = result.getEntity().copyBytes();
        VeniceThinClient.useResultToCompleteFuture(statusCode, contentType, body, valueFuture);
      }
    });
    return valueFuture;
  }

  @Override
  public void close(){

    CountDownLatch stopLatch = new CountDownLatch(1);
    d2Client.shutdown(new Callback<None>() {
      @Override
      public void onError(Throwable e) {
        logger.error("Error when shutting down d2client", e);
        stopLatch.countDown();
      }

      @Override
      public void onSuccess(None result) {
        logger.info("VeniceD2Client shutdown complete");
        stopLatch.countDown();
      }
    });
    try {
      stopLatch.await(d2ShutdownTimeoutMs, TimeUnit.MILLISECONDS);
      if (stopLatch.getCount() > 0){
        logger.error("D2Client shutdown timed out after " + d2ShutdownTimeoutMs + "ms");
      }
    } catch (InterruptedException e) {
      logger.warn("d2client shutdown interrupted");
    }

  }
}
