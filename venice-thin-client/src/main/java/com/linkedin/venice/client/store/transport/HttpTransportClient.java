package com.linkedin.venice.client.store.transport;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceServerErrorException;
import com.linkedin.venice.client.store.DeserializerFetcher;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * {@link CloseableHttpAsyncClient} based TransportClient implementation.
 * @param <V>
 */
public class HttpTransportClient<V> extends TransportClient<V> {
  private Logger logger = Logger.getLogger(HttpTransportClient.class);

  // Example: 'http://router-host:80/'
  private final String routerUrl;
  private final CloseableHttpAsyncClient httpClient;

  public HttpTransportClient(String routerUrl) {
    this.routerUrl = routerUrl;
    httpClient = HttpAsyncClients.createDefault();
    httpClient.start();
  }

  @Override
  public Future<V> get(String requestPath) {
    HttpGet request = getHttpRequest(requestPath);
    CompletableFuture<V> valueFuture = new CompletableFuture<>();
    httpClient.execute(request, new HttpTransportClientCallback<>(valueFuture, getDeserializerFetcher()));
    return valueFuture;
  }

  @Override
  public Future<byte[]> getRaw(String requestPath) {
    HttpGet request = getHttpRequest(requestPath);
    CompletableFuture<byte[]> valueFuture = new CompletableFuture<>();
    httpClient.execute(request, new HttpTransportClientCallback<>(valueFuture));
    return valueFuture;
  }

  private HttpGet getHttpRequest(String requestPath) {
    String requestUrl = routerUrl + requestPath;
    return new HttpGet(requestUrl);
  }

  @Override
  public void close() {
    try {
      httpClient.close();
      logger.info("HttpStoreClient closed");
    } catch (IOException e) {
      logger.error("Failed to close internal CloseableHttpAsyncClient", e);
    }
  }

  /**
   * The same {@link CloseableHttpAsyncClient} could not be used to send out another request in its own callback function.
   * @return
   */
  @Override
  public TransportClient<V> getCopyIfNotUsableInCallback() {
    return new HttpTransportClient<>(routerUrl);
  }

  private static class HttpTransportClientCallback<T> extends TransportClientCallback<T> implements FutureCallback<HttpResponse> {
    public HttpTransportClientCallback(CompletableFuture<T> valueFuture, DeserializerFetcher<T> fetcher) {
      super(valueFuture, fetcher);
    }

    public HttpTransportClientCallback(CompletableFuture<T> valueFuture) {
      super(valueFuture);
    }

    @Override
    public void failed(Exception ex) {
      getValueFuture().completeExceptionally(new VeniceClientException(ex));
    }

    @Override
    public void cancelled() {
      getValueFuture().completeExceptionally(new VeniceClientException("Request cancelled"));
    }

    @Override
    public void completed(HttpResponse result) {
      int statusCode = result.getStatusLine().getStatusCode();

      String schemaId = null;
      // If we try to retrieve the header value directly, and the 'getValue' will hang if the header doesn't exist.
      if (!isNeedRawResult()) {
        Header schemaIdHeader = result.getFirstHeader(HEADER_VENICE_SCHEMA_ID);
        if (HttpStatus.SC_OK == statusCode) {
          if (null == schemaIdHeader) {
            getValueFuture().completeExceptionally(new VeniceServerErrorException("Header: " +
                HEADER_VENICE_SCHEMA_ID + " doesn't exist"));
            return;
          }
          schemaId = schemaIdHeader.getValue();
        }
      }
      byte[] body = null;
      try (InputStream bodyStream = result.getEntity().getContent()) {
        body = IOUtils.toByteArray(bodyStream);
      } catch (IOException e) {
        getValueFuture().completeExceptionally(new VeniceClientException(e));
        return;
      }

      completeFuture(statusCode, body, schemaId);
    }
  }
}
