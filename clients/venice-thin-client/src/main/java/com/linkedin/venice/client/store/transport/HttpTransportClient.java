package com.linkedin.venice.client.store.transport;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.httpclient5.HttpClient5Utils;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.security.SSLFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleResponseConsumer;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.nio.AsyncRequestProducer;
import org.apache.hc.core5.http.nio.support.AsyncRequestBuilder;
import org.apache.hc.core5.http2.HttpVersionPolicy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * {@link CloseableHttpAsyncClient} based TransportClient implementation.
 */
public class HttpTransportClient extends TransportClient {
  private final static Logger LOGGER = LogManager.getLogger(HttpTransportClient.class);

  // Example: 'http://router-host:80/'
  protected final String routerUrl;
  protected int maxConnectionsTotal;
  protected int maxConnectionsPerRoute;
  private final CloseableHttpAsyncClient httpClient;

  public HttpTransportClient(String routerUrl, int maxConnectionsTotal, int maxConnectionsPerRoute) {
    this(routerUrl, buildClient(routerUrl, maxConnectionsTotal, maxConnectionsPerRoute, false, null));
    this.maxConnectionsTotal = maxConnectionsTotal;
    this.maxConnectionsPerRoute = maxConnectionsPerRoute;
  }

  public HttpTransportClient(String routerUrl, CloseableHttpAsyncClient httpClient) {
    this.routerUrl = ensureTrailingSlash(routerUrl);
    this.httpClient = httpClient;
    httpClient.start();
  }

  /**
   * Note: The callback that is triggered by {@link CloseableHttpAsyncClient} runs in the same thread as one of it's worker
   * threads and if the users of the future run tasks that block the release of the future thread, a deadlock will occur.
   * Hence, use the async handlers of {@link CompletableFuture} if you plan to use the same client to make multiple
   * requests sequentially.
   **/
  protected static CloseableHttpAsyncClient buildClient(
      String routerUrl,
      int maxConnectionsTotal,
      int maxConnectionsPerRoute,
      boolean requireHHtp2,
      SSLFactory sslFactory) {

    if (requireHHtp2) {
      // HTTP2 only client doesn't allow you to configure the ConnectionManager
      LOGGER.info("Creating a TLS HTTP2 only client to {}", routerUrl);
      return new HttpClient5Utils.HttpClient5Builder().setSslContext(sslFactory.getSSLContext()).build();
    }

    LOGGER.info(
        "Creating a HTTP1 only client to {} ({} maxConnections, {} maxConnectionsPerRoute)",
        routerUrl,
        maxConnectionsTotal,
        maxConnectionsPerRoute);
    return HttpAsyncClients.custom()
        .setVersionPolicy(HttpVersionPolicy.FORCE_HTTP_1)
        .setConnectionManager(
            PoolingAsyncClientConnectionManagerBuilder.create()
                .setMaxConnTotal(maxConnectionsTotal)
                .setMaxConnPerRoute(maxConnectionsPerRoute)
                .setTlsStrategy(
                    sslFactory != null
                        ? ClientTlsStrategyBuilder.create().setSslContext(sslFactory.getSSLContext()).build()
                        : null)
                .build())
        .build();
  }

  @Override
  public CompletableFuture<TransportClientResponse> get(String requestPath, Map<String, String> headers) {
    AsyncRequestProducer request = getHttpGetRequest(requestPath, headers);
    CompletableFuture<TransportClientResponse> valueFuture = new CompletableFuture<>();
    httpClient.execute(request, SimpleResponseConsumer.create(), new HttpTransportClientCallback(valueFuture));
    return valueFuture;
  }

  /**
   * Note: The callback that is triggered by {@link CloseableHttpAsyncClient} runs in the same thread as one of it's worker
   * threads and if the users of the future run tasks that block the release of the future thread, a deadlock will occur.
   * Hence, use the async handlers of {@link CompletableFuture} if you plan to use the same client to make multiple
   * requests sequentially.
   */
  @Override
  public CompletableFuture<TransportClientResponse> post(
      String requestPath,
      Map<String, String> headers,
      byte[] requestBody) {
    AsyncRequestProducer request = getHttpPostRequest(requestPath, headers, requestBody);
    CompletableFuture<TransportClientResponse> valueFuture = new CompletableFuture<>();
    httpClient.execute(request, SimpleResponseConsumer.create(), new HttpTransportClientCallback(valueFuture));
    return valueFuture;
  }

  /**
   * Leverage non-streaming post to achieve feature parity.
   * Note: The callback that is triggered by {@link CloseableHttpAsyncClient} runs in the same thread as one of it's worker
   * threads and if the users of the future run tasks that block the release of the future thread, a deadlock will occur.
   * Hence, use the async handlers of {@link CompletableFuture} if you plan to use the same client to make multiple
   * requests sequentially.
   */
  @Override
  public void streamPost(
      String requestPath,
      Map<String, String> headers,
      byte[] requestBody,
      TransportClientStreamingCallback callback,
      int keyCount) {
    try {
      CompletableFuture<TransportClientResponse> responseFuture = post(requestPath, headers, requestBody);
      responseFuture.whenComplete((response, throwable) -> {
        if (throwable != null) {
          callback.onCompletion(Optional.of(new VeniceClientException(throwable)));
        } else {
          // Compose the header map
          Map<String, String> responseHeaderMap = new HashMap<>();
          if (response.isSchemaIdValid()) {
            responseHeaderMap.put(HttpConstants.VENICE_SCHEMA_ID, Integer.toString(response.getSchemaId()));
          }
          responseHeaderMap.put(
              HttpConstants.VENICE_COMPRESSION_STRATEGY,
              Integer.toString(response.getCompressionStrategy().getValue()));
          callback.onHeaderReceived(responseHeaderMap);

          callback.onDataReceived(ByteBuffer.wrap(response.getBody()));
          callback.onCompletion(Optional.empty());
        }
      });
    } catch (Exception e) {
      callback.onCompletion(Optional.of(new VeniceClientException(e)));
    }
  }

  private String getHttpRequestUrl(String requestPath) {
    return routerUrl + requestPath;
  }

  private AsyncRequestProducer getHttpGetRequest(String requestPath, Map<String, String> headers) {
    AsyncRequestBuilder httpGet = AsyncRequestBuilder.get(getHttpRequestUrl(requestPath));
    headers.forEach((K, V) -> {
      httpGet.addHeader(K, V);
    });
    return httpGet.build();
  }

  private AsyncRequestProducer getHttpPostRequest(String requestPath, Map<String, String> headers, byte[] body) {
    AsyncRequestBuilder httpPost = AsyncRequestBuilder.post(getHttpRequestUrl(requestPath));
    headers.forEach((K, V) -> {
      httpPost.setHeader(K, V);
    });
    httpPost.setEntity(body, ContentType.APPLICATION_OCTET_STREAM);
    return httpPost.build();
  }

  @Override
  public void close() {
    try {
      httpClient.close();
      LOGGER.debug("HttpStoreClient closed");
    } catch (IOException e) {
      LOGGER.error("Failed to close internal CloseableHttpAsyncClient", e);
    }
  }

  private static class HttpTransportClientCallback extends TransportClientCallback
      implements FutureCallback<SimpleHttpResponse> {
    public HttpTransportClientCallback(CompletableFuture<TransportClientResponse> valueFuture) {
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
    public void completed(SimpleHttpResponse result) {
      int statusCode = result.getCode();

      int schemaId = SchemaData.INVALID_VALUE_SCHEMA_ID;
      // If we try to retrieve the header value directly, and the 'getValue' will hang if the header doesn't exist.
      Header schemaIdHeader = result.getFirstHeader(HttpConstants.VENICE_SCHEMA_ID);
      if (HttpStatus.SC_OK == statusCode) {
        if (schemaIdHeader != null) {
          schemaId = Integer.parseInt(schemaIdHeader.getValue());
        }
      }

      CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;
      Header compressionHeader = result.getFirstHeader(HttpConstants.VENICE_COMPRESSION_STRATEGY);
      if (compressionHeader != null) {
        compressionStrategy = CompressionStrategy.valueOf(Integer.parseInt(compressionHeader.getValue()));
      }

      byte[] body = result.getBody() != null ? result.getBody().getBodyBytes() : null;
      completeFuture(statusCode, schemaId, compressionStrategy, body);
    }
  }

  private static String ensureTrailingSlash(String input) {
    if (input.endsWith("/")) {
      return input;
    } else {
      return input + "/";
    }
  }

  public String toString() {
    return this.getClass().getSimpleName() + "(routerUrl: " + routerUrl + ")";
  }

  public int getMaxConnectionsTotal() {
    return maxConnectionsTotal;
  }

  public int getMaxConnectionsPerRoute() {
    return maxConnectionsPerRoute;
  }
}
