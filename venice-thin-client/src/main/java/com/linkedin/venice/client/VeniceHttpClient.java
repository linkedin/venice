package com.linkedin.venice.client;

import com.google.common.net.HttpHeaders;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.log4j.Logger;


/**
 * Client for making queries to Venice.
 * This client is threadsafe; re-use it.  There is ~130ms overhead on the first request as the client initializes.
 */
public class VeniceHttpClient implements VeniceThinClient {
  private static final Logger logger = Logger.getLogger(VeniceHttpClient.class);

  private final String routerHost;
  private final int routerPort;
  private final CloseableHttpAsyncClient httpClient;

  public VeniceHttpClient(String routerHost, int routerPort){
    this.routerHost = routerHost;
    this.routerPort = routerPort;
    httpClient = HttpAsyncClients.createDefault();
    httpClient.start();
    logger.info("VeniceHttpClient started");
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
    String requestUrl = "http://" + routerHost + ":" + routerPort +
        "/" + STORAGE_TYPE +
        "/" + storeName +
        "/" + b64key + B64_FORMAT;
    final HttpGet request = new HttpGet(requestUrl);
    CompletableFuture<byte[]> valueFuture = new CompletableFuture<>();
    httpClient.execute(request, new FutureCallback<HttpResponse>() {
      @Override
      public void completed(HttpResponse result) {
        int statusCode = result.getStatusLine().getStatusCode();
        byte[] body;
        String contentType = result.getFirstHeader(HttpHeaders.CONTENT_TYPE).getValue();
        try (InputStream bodyStream = result.getEntity().getContent()) {
          body = IOUtils.toByteArray(bodyStream);
        } catch (IOException e) {
          valueFuture.completeExceptionally(new VeniceClientException(e));
          return;
        }
        VeniceThinClient.useResultToCompleteFuture(statusCode, contentType, body, valueFuture);
      }

      @Override
      public void failed(Exception ex) {
        valueFuture.completeExceptionally(new VeniceClientException(ex));
      }

      @Override
      public void cancelled() {
        valueFuture.completeExceptionally(new VeniceClientException("Request cancelled"));
      }
    });
    return valueFuture;
  }

  @Override
  public void close(){
    try {
      httpClient.close();
      logger.info("VeniceHttpClient closed");
    } catch (IOException e) {
      logger.error("Failed to close internal CloseableHttpAsyncClient", e);
    }
  }

  public static int main(String[] args) throws VeniceClientException {
    throw new VeniceClientException("The " + VeniceHttpClient.class.getSimpleName() +
        " is not usable interactively. Stay tuned!!");
  }

}
