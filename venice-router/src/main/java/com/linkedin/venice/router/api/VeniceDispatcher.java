package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.base.concurrency.AsyncFuture;
import com.linkedin.ddsstorage.base.concurrency.AsyncPromise;
import com.linkedin.ddsstorage.netty4.misc.BasicHttpRequest;
import com.linkedin.ddsstorage.router.api.PartitionDispatchHandler4;
import com.linkedin.ddsstorage.router.api.Scatter;
import com.linkedin.ddsstorage.router.api.ScatterGatherRequest;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.exceptions.QuotaExceededException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.throttle.ReadRequestThrottler;
import com.linkedin.venice.utils.SslUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.tehuti.metrics.MetricsRepository;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import javax.annotation.Nonnull;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.log4j.Logger;

import static com.linkedin.venice.HttpConstants.*;


public class VeniceDispatcher implements PartitionDispatchHandler4<Instance, VeniceStoragePath, RouterKey>, Closeable{

  private static final String REQUIRED_API_VERSION = "1";
  private final String scheme;

  private static final Logger logger = Logger.getLogger(VeniceDispatcher.class);

  // see: https://hc.apache.org/httpcomponents-asyncclient-dev/quickstart.html
  private final CloseableHttpAsyncClient httpClient;

  // key is (resource + "_" + partition)
  private final ConcurrentMap<String, Long> offsets = new ConcurrentHashMap<>();
  private final VeniceHostHealth healthMontior;

  private final AggRouterHttpRequestStats stats;

  private ReadRequestThrottler readRequestThrottler;

   // How many offsets behind can a storage node be for a partition and still be considered 'caught up'
  private long acceptableOffsetLag = 10000; /* TODO: make this configurable for streaming use-case */

  /**
   *
   * @param healthMonitor
   * @param clientTimeoutMillis
   * @param metricsRepository
   * @param sslFactory if this is present, it will be used to make SSL requests to storage nodes.
   */
  public VeniceDispatcher(VeniceHostHealth healthMonitor, int clientTimeoutMillis, MetricsRepository metricsRepository, Optional<SSLEngineComponentFactory> sslFactory){
    httpClient = SslUtils.getMinimalHttpClient(50, 1000, sslFactory);
    httpClient.start();
    this.healthMontior = healthMonitor;
    stats = new AggRouterHttpRequestStats(metricsRepository);
    scheme = sslFactory.isPresent() ? HTTPS_PREFIX : HTTP_PREFIX;
  }

  @Override
  public void dispatch(
      @Nonnull Scatter<Instance, VeniceStoragePath, RouterKey> scatter,
      @Nonnull ScatterGatherRequest<Instance, RouterKey> part,
      @Nonnull VeniceStoragePath path,
      @Nonnull BasicHttpRequest request,
      @Nonnull AsyncPromise<Instance> hostSelected,
      @Nonnull AsyncPromise<List<FullHttpResponse>> responseFuture,
      @Nonnull AsyncPromise<HttpResponseStatus> retryFuture,
      @Nonnull AsyncFuture<Void> timeoutFuture,
      @Nonnull Executor contextExecutor) {

    long startTime = System.currentTimeMillis();

    String storeName = Version.parseStoreFromKafkaTopicName(path.getResourceName());
    int keySize = path.getPartitionKey().getBytes().length;
    stats.recordRequest(storeName);
    stats.recordKeySize(storeName, keySize);

    Instance host;
    try {
      int hostCount = part.getHosts().size();
      if (0 == hostCount) {
        throw new VeniceException("Could not find ready-to-serve replica for request: " + part);
      }
      host = part.getHosts().get( (int)(System.currentTimeMillis() % hostCount) );  //cheap random host selection
      hostSelected.setSuccess(host);
    } catch (Exception e) {
      hostSelected.setFailure(e);
      throw new VeniceException("Failed to route request to a host", e);
    }
    try {
      readRequestThrottler.mayThrottleRead(storeName, readRequestThrottler.getReadCapacity());
    } catch (QuotaExceededException e) {
      stats.recordThrottledRequest(storeName);
      throw e;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Routing request to host: " + host.getHost() + ":" + host.getPort());
    }

    String requestPath = path.getLocation();
    logger.debug("Using request path: " + requestPath);

    //  http(s)://host:port/path
    String address = scheme + host.getHost() + ":" + host.getPort() + "/" + requestPath;
    final HttpGet requestToNode = new HttpGet(address);
    requestToNode.addHeader(HttpConstants.VENICE_API_VERSION, REQUIRED_API_VERSION);
    httpClient.execute(requestToNode, new FutureCallback<org.apache.http.HttpResponse>() {

      @Override
      public void completed(org.apache.http.HttpResponse result) {
        int statusCode = result.getStatusLine().getStatusCode();
        Iterator<String> partitionNames = part.getPartitionsNames().iterator();
        String partitionName = partitionNames.next();
        if (partitionNames.hasNext()){
          logger.error("There must be only one partition in a request, handling request as if there is only one partition");
        }
        long offset = Long.parseLong(result.getFirstHeader(HttpConstants.VENICE_OFFSET).getValue());
        String offsetKey = path.getResourceName() + "_" + partitionName;
        if (statusCode == 200
            && offsets.containsKey(offsetKey)
            && offset + acceptableOffsetLag < offsets.get(offsetKey)
            && part.getHosts().size() > 1) {
          healthMontior.setPartitionAsSlow(host, partitionName);
          contextExecutor.execute(() -> {
            // Triggers an immediate router retry excluding the host we selected.
            retryFuture.setSuccess(HttpResponseStatus.SERVICE_UNAVAILABLE);
          });
          return;
        }
        offsets.put(offsetKey, offset);
        int responseStatus = result.getStatusLine().getStatusCode();
        FullHttpResponse response;
        byte[] contentToByte;

        try (InputStream contentStream = result.getEntity().getContent()) {
          contentToByte = IOUtils.toByteArray(contentStream);
          stats.recordValueSize(storeName, contentToByte.length);
        } catch (IOException e) {
          completeWithError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
          stats.recordUnhealthyRequest(storeName);
          return;
        }

        ByteBuf content = Unpooled.wrappedBuffer(contentToByte);

        //TODO: timeout should be configurable and be defined by the HttpAysncClient
        boolean timeout = System.currentTimeMillis() - startTime > 50 * 1000;
        switch (responseStatus){
          case HttpStatus.SC_OK:
            response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, content);
            if (timeout)
              stats.recordUnhealthyRequest(storeName);
            else
              stats.recordHealthyRequest(storeName);
            break;
          case HttpStatus.SC_NOT_FOUND:
            response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND, content);
            if (timeout)
              stats.recordUnhealthyRequest(storeName);
            else
              stats.recordHealthyRequest(storeName);
            break;
          case HttpStatus.SC_INTERNAL_SERVER_ERROR:
          default: //Path Parser will throw BAD_REQUEST responses.
            response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_GATEWAY, content);
            stats.recordUnhealthyRequest(storeName);
        }

        int valueSchemaId = Integer.parseInt(result.getFirstHeader(HttpConstants.VENICE_SCHEMA_ID).getValue());
        response.headers()
            .set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes())
            .set(HttpHeaderNames.CONTENT_TYPE, HttpConstants.APPLICATION_OCTET)
            .set(HttpConstants.VENICE_STORE_VERSION, path.getVersionNumber())
            .set(HttpConstants.VENICE_PARTITION, numberFromPartitionName(partitionName))
            .set(HttpConstants.VENICE_SCHEMA_ID, valueSchemaId);

        contextExecutor.execute(() -> {
          responseFuture.setSuccess(Collections.singletonList(response));
        });

        stats.recordLatency(storeName, System.currentTimeMillis() - startTime);
      }

      @Override
      public void failed(Exception ex) {
        completeWithError(HttpResponseStatus.INTERNAL_SERVER_ERROR, ex);
      }

      @Override
      public void cancelled() {
        completeWithError(HttpResponseStatus.INTERNAL_SERVER_ERROR,
            new VeniceException("Request to storage node was cancelled"));
      }

      private void completeWithError(HttpResponseStatus status, Throwable e) {

        String errMsg = e.getMessage();
        if (null == errMsg) {
          errMsg = "Unknown error, caught: " + e.getClass().getCanonicalName();
        }
        ByteBuf content =  Unpooled.wrappedBuffer(errMsg.getBytes(StandardCharsets.UTF_8));

        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);
        response.headers()
            .set(HttpHeaderNames.CONTENT_TYPE, HttpConstants.TEXT_PLAIN)
            .set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());

        contextExecutor.execute(() -> {
          responseFuture.setSuccess(Collections.singletonList(response));
        });
      }
    });
  }

  public void close(){
    IOUtils.closeQuietly(httpClient);
  }

  public ReadRequestThrottler getReadRequestThrottler() {
    return readRequestThrottler;
  }

  public void setReadRequestThrottler(ReadRequestThrottler readRequestThrottler) {
    this.readRequestThrottler = readRequestThrottler;
  }

  protected static String numberFromPartitionName(String partitionName){
    return partitionName.substring(partitionName.lastIndexOf("_")+1);
  }
}
