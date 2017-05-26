package com.linkedin.venice.client.store.transport;

import com.linkedin.common.callback.Callback;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceServerException;
import com.linkedin.venice.client.store.ClientHttpCallback;
import com.linkedin.venice.client.store.DeserializerFetcher;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.log4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


/**
 * {@link D2Client} based TransportClient implementation.
 * @param <V>
 */
public class D2TransportClient<V> extends TransportClient<V> {
  public static final String DEFAULT_D2_ZK_BASE_PATH = "/d2";
  public static final int DEFAULT_ZK_TIMEOUT_IN_MS = 5000;

  private Logger logger = Logger.getLogger(D2TransportClient.class);

  private static final long d2StartupTimeoutMs = TimeUnit.SECONDS.toMillis(10);
  private static final long d2ShutdownTimeoutMs = TimeUnit.SECONDS.toMillis(30);

  private final D2Client d2Client;

  //indicate whether it is a private d2 created by TransportClient or it is a public
  //d2 shared by multiply TransportClient. The TransportClient only takes care of
  //start/shutdown a d2 client if is is private.
  private final Boolean privateD2Client;
  private final String d2ServiceName;

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
   * Construct by a custom specified zookeeper cluster
   * @param zkConnection
   * @param d2ServiceName
   */
  public D2TransportClient(String zkConnection, String d2ServiceName) throws VeniceClientException {
    this(zkConnection, d2ServiceName, DEFAULT_D2_ZK_BASE_PATH, DEFAULT_ZK_TIMEOUT_IN_MS);
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
  public Future<V> get(String requestPath, ClientHttpCallback callback) {
    RestRequest request = getRestRequest(requestPath);
    CompletableFuture<V> valueFuture = new CompletableFuture<>();
    d2Client.restRequest(request, new D2TransportClientCallback<>(valueFuture, getDeserializerFetcher(), callback));
    return valueFuture;
  }

  @Override
  public Future<byte[]> getRaw(String requestPath) {
    RestRequest request = getRestRequest(requestPath);
    CompletableFuture<byte[]> valueFuture = new CompletableFuture<>();
    d2Client.restRequest(request, new D2TransportClientCallback<>(valueFuture));
    return valueFuture;
  }

  private RestRequest getRestRequest(String requestPath) {
    String requestUrl = "d2://" + d2ServiceName + "/" + requestPath;
    return D2ClientUtils.createD2GetReqeust(requestUrl);
  }

  @Override
  public synchronized void close() {
    if (privateD2Client) {
      D2ClientUtils.shutdownClient(d2Client);
    } else {
      logger.info("This is a shared D2Client. TransportClient is not responsible to shut it down. Please do it manually.");
    }
  }

  private static class D2TransportClientCallback<T> extends TransportClientCallback<T> implements Callback<RestResponse> {
    private Logger logger = Logger.getLogger(D2TransportClient.class);

    public D2TransportClientCallback(CompletableFuture<T> valueFuture, DeserializerFetcher<T> fetcher, ClientHttpCallback callback) {
      super(valueFuture, fetcher, callback);
    }

    public D2TransportClientCallback(CompletableFuture<T> valueFuture) {
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
        callback.executeOnError();
        getValueFuture().completeExceptionally(new VeniceClientException(e));
      }
    }

    @Override
    public void onSuccess(RestResponse result) {
      int statusCode = result.getStatus();
      String schemaId = null;
      if (!isNeedRawResult() && HttpStatus.SC_OK == statusCode) {
        schemaId = result.getHeader(HEADER_VENICE_SCHEMA_ID);
        if (null == schemaId) {
          getValueFuture().completeExceptionally(new VeniceServerException("Header: "
              + HEADER_VENICE_SCHEMA_ID + " doesn't exist"));
          callback.executeOnError();
          return;
        }
      }
      byte[] body = result.getEntity().copyBytes();
      completeFuture(statusCode, body, schemaId);
    }
  }

  public String toString() {
    return this.getClass().getSimpleName() + "(d2ServiceName: " + d2ServiceName + ")";
  }
}
