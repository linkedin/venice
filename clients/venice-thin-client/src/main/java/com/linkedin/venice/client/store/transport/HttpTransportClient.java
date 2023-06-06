package com.linkedin.venice.client.store.transport;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.authentication.ClientAuthenticationProvider;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.schema.SchemaData;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * {@link CloseableHttpAsyncClient} based TransportClient implementation.
 */
public class HttpTransportClient extends TransportClient {
  private final Logger logger = LogManager.getLogger(HttpTransportClient.class);

  // Example: 'http://router-host:80/'
  protected final String routerUrl;
  private final CloseableHttpAsyncClient httpClient;
  protected final ClientAuthenticationProvider authenticationProvider;

  public HttpTransportClient(String routerUrl, ClientAuthenticationProvider authenticationProvider) {
    this(routerUrl, HttpAsyncClients.createDefault(), authenticationProvider);
  }

  public HttpTransportClient(
      String routerUrl,
      CloseableHttpAsyncClient httpClient,
      ClientAuthenticationProvider authenticationProvider) {
    this.routerUrl = ensureTrailingSlash(routerUrl);
    this.httpClient = httpClient;
    this.authenticationProvider = authenticationProvider;
    httpClient.start();
  }

  @Override
  public CompletableFuture<TransportClientResponse> get(String requestPath, Map<String, String> headers) {
    HttpGet request = getHttpGetRequest(requestPath, headers);
    CompletableFuture<TransportClientResponse> valueFuture = new CompletableFuture<>();
    httpClient.execute(request, new HttpTransportClientCallback(valueFuture));
    return valueFuture;
  }

  @Override
  public CompletableFuture<TransportClientResponse> post(
      String requestPath,
      Map<String, String> headers,
      byte[] requestBody) {
    HttpPost request = getHttpPostRequest(requestPath, headers, requestBody);
    CompletableFuture<TransportClientResponse> valueFuture = new CompletableFuture<>();
    httpClient.execute(request, new HttpTransportClientCallback(valueFuture));
    return valueFuture;
  }

  /**
   * Leverage non-streaming post to achieve feature parity.
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

  private HttpGet getHttpGetRequest(String requestPath, Map<String, String> headers) {
    HttpGet httpGet = new HttpGet(getHttpRequestUrl(requestPath));
    headers.forEach((K, V) -> {
      httpGet.addHeader(K, V);
    });
    if (authenticationProvider != null) {
      authenticationProvider.getHTTPAuthenticationHeaders().forEach((K, V) -> {
        httpGet.addHeader(K, V);
      });
    }
    return httpGet;
  }

  private HttpPost getHttpPostRequest(String requestPath, Map<String, String> headers, byte[] body) {
    HttpPost httpPost = new HttpPost(getHttpRequestUrl(requestPath));
    headers.forEach((K, V) -> {
      httpPost.setHeader(K, V);
    });
    if (authenticationProvider != null) {
      authenticationProvider.getHTTPAuthenticationHeaders().forEach((K, V) -> {
        httpPost.addHeader(K, V);
      });
    }
    BasicHttpEntity entity = new BasicHttpEntity();
    entity.setContent(new ByteArrayInputStream(body));
    httpPost.setEntity(entity);

    return httpPost;
  }

  @Override
  public void close() {
    try {
      httpClient.close();
      logger.debug("HttpStoreClient closed");
    } catch (IOException e) {
      logger.error("Failed to close internal CloseableHttpAsyncClient", e);
    }
  }

  /**
   * The same {@link CloseableHttpAsyncClient} could not be used to send out another request in its own callback function.
   * @return
   */
  @Override
  public TransportClient getCopyIfNotUsableInCallback() {
    return new HttpTransportClient(routerUrl, authenticationProvider);
  }

  private static class HttpTransportClientCallback extends TransportClientCallback
      implements FutureCallback<HttpResponse> {
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
    public void completed(HttpResponse result) {
      int statusCode = result.getStatusLine().getStatusCode();

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

      byte[] body;
      try (InputStream bodyStream = result.getEntity().getContent()) {
        body = IOUtils.toByteArray(bodyStream);
      } catch (IOException e) {
        getValueFuture().completeExceptionally(new VeniceClientException(e));
        return;
      }

      completeFuture(statusCode, schemaId, compressionStrategy, body);
    }
  }

  public static String ensureTrailingSlash(String input) {
    if (input.endsWith("/")) {
      return input;
    } else {
      return input + "/";
    }
  }

  public String toString() {
    return this.getClass().getSimpleName() + "(routerUrl: " + routerUrl + ")";
  }
}
