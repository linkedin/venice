package com.linkedin.venice.client.store.transport;

import com.linkedin.common.callback.Callback;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.data.ByteString;
import com.linkedin.r2.R2Constants;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestMethod;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.stream.StreamRequest;
import com.linkedin.r2.message.stream.StreamRequestBuilder;
import com.linkedin.r2.message.stream.StreamResponse;
import com.linkedin.r2.message.stream.entitystream.ByteStringWriter;
import com.linkedin.r2.message.stream.entitystream.EntityStream;
import com.linkedin.r2.message.stream.entitystream.EntityStreams;
import com.linkedin.r2.message.stream.entitystream.ReadHandle;
import com.linkedin.r2.message.stream.entitystream.Reader;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.schema.SchemaData;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.log4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


/**
 * {@link D2Client} based TransportClient implementation.
 */
public class D2TransportClient extends TransportClient {
  private Logger logger = Logger.getLogger(D2TransportClient.class);

  private final D2Client d2Client;

  //indicate whether it is a private d2 created by TransportClient or it is a public
  //d2 shared by multiply TransportClient. The TransportClient only takes care of
  //start/shutdown a d2 client if is is private.
  private final boolean privateD2Client;
  private String d2ServiceName;

  /**
   * Construct by an existing D2Client (such as from the pegasus-d2-client-default-cmpt).
   *
   * @param d2ServiceName
   * @param d2Client
   */
  public D2TransportClient(String d2ServiceName, D2Client d2Client){
    this.d2ServiceName = d2ServiceName;
    this.d2Client = d2Client;
    this.privateD2Client = false;
  }

  /**
   * Construct by customized zookeeper and other configs.
   * @param zkConnection
   * @param d2ServiceName
   * @param zkBasePath
   * @param zkTimeout
   */
  public D2TransportClient(String zkConnection,
                               String d2ServiceName,
                               String zkBasePath,
                               int zkTimeout) {
    this.d2ServiceName = d2ServiceName;
    D2ClientBuilder builder = new D2ClientBuilder().setZkHosts(zkConnection)
        .setZkSessionTimeout(zkTimeout, TimeUnit.MILLISECONDS)
        .setZkStartupTimeout(zkTimeout, TimeUnit.MILLISECONDS)
        .setLbWaitTimeout(zkTimeout, TimeUnit.MILLISECONDS)
        .setBasePath(zkBasePath);
    d2Client = builder.build();
    this.privateD2Client = true;

    D2ClientUtils.startClient(d2Client);
  }

  @Override
  public CompletableFuture<TransportClientResponse> get(String requestPath, Map<String, String> headers) {
    RestRequest request = getRestGetRequest(requestPath, headers);
    CompletableFuture<TransportClientResponse> valueFuture = new CompletableFuture<>();
    RequestContext requestContext = new RequestContext();
    requestContext.putLocalAttr(R2Constants.R2_OPERATION, "get"); //required for d2 backup requests
    d2Client.restRequest(request, requestContext, new D2TransportClientCallback(valueFuture));
    return valueFuture;
  }

  @Override
  public CompletableFuture<TransportClientResponse> post(String requestPath, Map<String, String> headers,
      byte[] requestBody) {
    RestRequest request = getRestPostRequest(requestPath, headers, requestBody);
    CompletableFuture<TransportClientResponse> valueFuture = new CompletableFuture<>();

    d2Client.restRequest(request, getRequestContextForPost(), new D2TransportClientCallback(valueFuture));
    return valueFuture;
  }

  // TODO: we may want to differentiate 'compute' from 'batchget'
  private RequestContext getRequestContextForPost() {
    RequestContext requestContext = new RequestContext();
    requestContext.putLocalAttr(R2Constants.R2_OPERATION, "batchget"); //required for d2 backup requests

    return requestContext;
  }

  @Override
  public void streamPost(String requestPath, Map<String, String> headers, byte[] requestBody,
      TransportClientStreamingCallback callback) {
    try {
      String requestUrl = getD2RequestUrl(requestPath);
      StreamRequestBuilder requestBuilder = new StreamRequestBuilder(URI.create(requestUrl));
      headers.forEach((name, value) -> requestBuilder.addHeaderValue(name, value));
      requestBuilder.setMethod(RestMethod.POST);
      StreamRequest streamRequest = requestBuilder.build(EntityStreams.newEntityStream(new ByteStringWriter(ByteString.unsafeWrap(requestBody))));

      RequestContext requestContext = getRequestContextForPost();
      requestContext.putLocalAttr(com.linkedin.r2.filter.R2Constants.IS_FULL_REQUEST, true);

      d2Client.streamRequest(streamRequest, requestContext, new Callback<StreamResponse>() {
        @Override
        public void onSuccess(StreamResponse result) {
          Map<String, String> headers = result.getHeaders();
          callback.onHeaderReceived(headers);

          EntityStream entityStream = result.getEntityStream();
          /**
           * All the operations in Reader is synchronized:
           * 1. To guarantee the invocation order, for example:
           *    a. onDataAvailable should be invoked before onDeserializationCompletion/onError;
           *    b. onDeserializationCompletion/onError could only be invoked at most once;
           * 2. To avoid using too many threads in R2 thread pool, and if the user would
           * like to speed up the callback execution, it can always to process the callback
           * in its own thread pool.
           */
          entityStream.setReader(new Reader() {
            private boolean isDone = false;
            private ReadHandle rh;

            @Override
            public void onInit(ReadHandle rh) {
              this.rh = rh;
              rh.request(10);
            }

            @Override
            public synchronized void onDataAvailable(ByteString data) {
              if (isDone) {
                logger.warn("Received data after completion and data length: " + data.length());
                return;
              } else {
                callback.onDataReceived(data.asByteBuffer());
              }
              // TODO: We might need to trigger write any away to clean up the buffer, or maybe throw an exception?
              rh.request(1);
            }

            @Override
            public synchronized void onDone() {
              if (isDone) {
                logger.warn("onDone got invoked after completion");
                return;
              }
              callback.onCompletion(Optional.empty());
              isDone = true;
            }

            @Override
            public synchronized void onError(Throwable e) {
              if (isDone) {
                logger.warn("onError got invoked after completion");
                return;
              }
              callback.onCompletion(Optional.of(new VeniceClientException(e)));
            }
          });
        }

        @Override
        public void onError(Throwable e) {
          /**
           * The following onDeserializationCompletion invocation shouldn't need to be protected since
           * either 'onSuccess' or 'onError' can be invoked, but not both.
           */
          callback.onCompletion(Optional.of(new VeniceClientException(e)));
        }
      });
    } catch (Throwable t) {
      /**
       * Always trigger {@link callback.onCompletion} to finish the request
       */
      callback.onCompletion(Optional.of(new VeniceClientException("Received exception when sending out request", t)));
    }
  }

  private String getD2RequestUrl(String requestPath) {
    return "d2://" + d2ServiceName + "/" + requestPath;
  }

  private RestRequest getRestGetRequest(String requestPath, Map<String, String> headers) {
    String requestUrl = getD2RequestUrl(requestPath);
    return D2ClientUtils.createD2GetRequest(requestUrl, headers);
  }

  private RestRequest getRestPostRequest(String requestPath, Map<String, String> headers, byte[] body) {
    String requestUrl = getD2RequestUrl(requestPath);
    return D2ClientUtils.createD2PostRequest(requestUrl, headers, body);
  }

  @Override
  public synchronized void close() {
    if (privateD2Client) {
      D2ClientUtils.shutdownClient(d2Client);
    } else {
      logger.info("This is a shared D2Client. TransportClient is not responsible to shut it down. Please do it manually.");
    }
  }

  private static class D2TransportClientCallback extends TransportClientCallback implements Callback<RestResponse> {
    private Logger logger = Logger.getLogger(D2TransportClient.class);

    public D2TransportClientCallback(CompletableFuture<TransportClientResponse> valueFuture) {
      super(valueFuture);
    }

    @Override
    public void onError(Throwable e) {
      if (e instanceof RestException){
        // Get the RestResponse for status codes other than 200
        RestResponse result = ((RestException) e).getResponse();
        onSuccess(result);
      } else {
        logger.error(e);
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
        if (null != schemaIdHeader) {
          schemaId = Integer.parseInt(schemaIdHeader);
        }
      }

      CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;
      String compressionHeader = result.getHeader(HttpConstants.VENICE_COMPRESSION_STRATEGY);
      if (compressionHeader != null) {
        compressionStrategy = CompressionStrategy.valueOf(Integer.valueOf(compressionHeader));
      }

      /**
       * TODO: consider to pass back {@link java.io.InputStream} instead of making a copy of response bytes
       */
      byte[] body = result.getEntity().copyBytes();
      completeFuture(statusCode, schemaId, compressionStrategy, body);
    }
  }

  public D2Client getD2Client() {
    return this.d2Client;
  }

  public String toString() {
    return this.getClass().getSimpleName() + "(d2ServiceName: " + d2ServiceName + ")";
  }
}
