package com.linkedin.venice.fastclient.transport;

import com.linkedin.common.callback.Callback;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.transport.TransportClientCallback;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.schema.SchemaData;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * R2 based Transport Client Interface.
 * So far, this class doesn't do anything special about the connection management, such as connection pool (size),
 * and connection warming and so on, and we will rely on the features offered by R2.
 * If we notice anything specific required to be implemented outside of R2, we will improve the implementation here.
 */
public class R2TransportClient extends InternalTransportClient {
  private final Client r2Client;

  public R2TransportClient(Client r2Client) {
    this.r2Client = r2Client;
  }

  @Override
  public CompletableFuture<TransportClientResponse> get(String requestUrl, Map<String, String> headers) {
    // Build rest request
    RestRequest request = D2ClientUtils.createD2GetRequest(requestUrl, headers);
    CompletableFuture<TransportClientResponse> valueFuture = new CompletableFuture<>();
    r2Client.restRequest(request, new R2TransportClientCallback(valueFuture));
    return valueFuture;
  }

  public CompletableFuture<TransportClientResponse> post(
      String requestUrl,
      Map<String, String> headers,
      byte[] requestBody) {
    RestRequest request = D2ClientUtils.createD2PostRequest(requestUrl, headers, requestBody);
    CompletableFuture<TransportClientResponse> valueFuture = new CompletableFuture<>();
    r2Client.restRequest(request, new R2TransportClientCallback(valueFuture));
    return valueFuture;
  }

  @Override
  public void close() throws IOException {

  }

  private static class R2TransportClientCallback extends TransportClientCallback implements Callback<RestResponse> {
    private final Logger logger = LogManager.getLogger(R2TransportClientCallback.class);

    public R2TransportClientCallback(CompletableFuture<TransportClientResponse> valueFuture) {
      super(valueFuture);
    }

    @Override
    public void onError(Throwable e) {
      if (e instanceof RestException) {
        // Get the RestResponse for status codes other than 200
        RestResponse result = ((RestException) e).getResponse();
        onSuccess(result);
      } else {
        logger.error("", e);
        getValueFuture().completeExceptionally(new VeniceClientException(e));
      }
    }

    @Override
    public void onSuccess(RestResponse result) {
      int statusCode = result.getStatus();

      int schemaId = SchemaData.INVALID_VALUE_SCHEMA_ID;
      String schemaIdHeader = null;
      if (HttpStatus.SC_OK == statusCode) {
        schemaIdHeader = result.getHeader(HttpConstants.VENICE_SCHEMA_ID);
        if (schemaIdHeader != null) {
          schemaId = Integer.parseInt(schemaIdHeader);
        }
      }

      CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;
      String compressionHeader = result.getHeader(HttpConstants.VENICE_COMPRESSION_STRATEGY);
      if (compressionHeader != null) {
        compressionStrategy = CompressionStrategy.valueOf(Integer.parseInt(compressionHeader));
      }

      /**
       * TODO: consider to pass back {@link java.io.InputStream} instead of making a copy of response bytes
       */
      byte[] body = result.getEntity().copyBytes();
      completeFuture(statusCode, schemaId, compressionStrategy, body);
    }
  }
}
