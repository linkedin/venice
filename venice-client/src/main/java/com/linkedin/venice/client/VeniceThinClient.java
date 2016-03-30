package com.linkedin.venice.client;

import com.google.common.net.HttpHeaders;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
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
public class VeniceThinClient implements Closeable {
  private static final Logger logger = Logger.getLogger(VeniceThinClient.class);

  private final String routerHost;
  private final int routerPort;
  private final CloseableHttpAsyncClient httpClient;
  private static final Base64.Encoder encoder = Base64.getEncoder();
  private static final String STORAGE_TYPE = "storage";
  private static final String B64_FORMAT = "?f=b64";

  public VeniceThinClient(String routerHost, int routerPort){
    this.routerHost = routerHost;
    this.routerPort = routerPort;
    httpClient = HttpAsyncClients.createDefault();
    httpClient.start();
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
        String msg = null;
        if (contentType.startsWith("text")) { // support text/plain and text/html
          msg = new String(body, StandardCharsets.UTF_8);
        }

        switch (statusCode) {
          case HttpStatus.SC_OK:
            valueFuture.complete(body);
            break;
          case HttpStatus.SC_NOT_FOUND:
            if (msg != null) {
              valueFuture.completeExceptionally(new VeniceNotFoundException(msg));
            } else {
              valueFuture.completeExceptionally(new VeniceNotFoundException());
            }
            break;
          case HttpStatus.SC_INTERNAL_SERVER_ERROR:
            if (msg != null) {
              valueFuture.completeExceptionally(new VeniceServerErrorException(msg));
            } else {
              valueFuture.completeExceptionally(new VeniceServerErrorException());
            }
            break;
          case HttpStatus.SC_BAD_REQUEST:
          default:
            if (msg != null) {
              valueFuture.completeExceptionally(new VeniceClientException(msg));
            } else {
              valueFuture
                  .completeExceptionally(new VeniceClientException("Router responds with status code: " + statusCode));
            }
        }
      }

      @Override
      public void failed(Exception ex) {
        valueFuture.completeExceptionally(new VeniceClientException("Request failed"));
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
    } catch (IOException e) {
      logger.error("Failed to close internal CloseableHttpAsyncClient", e);
    }
  }

}
