package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.base.concurrency.AsyncFuture;
import com.linkedin.ddsstorage.base.concurrency.AsyncPromise;
import com.linkedin.ddsstorage.netty4.misc.BasicHttpRequest;
import com.linkedin.ddsstorage.router.api.PartitionDispatchHandler4;
import com.linkedin.ddsstorage.router.api.Scatter;
import com.linkedin.ddsstorage.router.api.ScatterGatherRequest;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.common.PartitionOffsetMapUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.SslUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import javax.annotation.Nonnull;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.log4j.Logger;

import static com.linkedin.venice.HttpConstants.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;


public class VeniceDispatcher implements PartitionDispatchHandler4<Instance, VenicePath, RouterKey>, Closeable{
  private final String scheme;

  private static final Logger logger = Logger.getLogger(VeniceDispatcher.class);

  // see: https://hc.apache.org/httpcomponents-asyncclient-dev/quickstart.html
  private final int clientPoolSize;
  private final ArrayList<CloseableHttpAsyncClient> clientPool;
  private final Random random = new Random();

  // key is (resource + "_" + partition)
  private final ConcurrentMap<String, Long> offsets = new ConcurrentHashMap<>();
  private final VeniceHostHealth healthMontior;

   // How many offsets behind can a storage node be for a partition and still be considered 'caught up'
  private long acceptableOffsetLag = 10000; /* TODO: make this configurable for streaming use-case */

  /**
   *
   * @param healthMonitor
   * @param sslFactory if this is present, it will be used to make SSL requests to storage nodes.
   */
  public VeniceDispatcher(VeniceRouterConfig config, VeniceHostHealth healthMonitor,
      Optional<SSLEngineComponentFactory> sslFactory) {
    this.healthMontior = healthMonitor;
    this.scheme = sslFactory.isPresent() ? HTTPS_PREFIX : HTTP_PREFIX;

    this.clientPoolSize = config.getHttpClientPoolSize();
    int totalIOThreadNum = Runtime.getRuntime().availableProcessors();
    int maxConnPerRoute = config.getMaxOutgoingConnPerRoute();
    int maxConn = config.getMaxOutgoingConn();

    /**
     * Using a client pool to reduce lock contention introduced by {@link org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager#requestConnection}
     * and {@link org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager#releaseConnection}.
     */
    int ioThreadNumPerClient = (int)Math.ceil(((double)totalIOThreadNum) / clientPoolSize);
    int maxConnPerRoutePerClient = (int)Math.ceil(((double)maxConnPerRoute) / clientPoolSize);
    int totalMaxConnPerClient = (int)Math.ceil(((double)maxConn) / clientPoolSize);
    clientPool = new ArrayList<>();
    for (int i = 0; i < clientPoolSize; ++i) {
      CloseableHttpAsyncClient client = SslUtils.getMinimalHttpClient(ioThreadNumPerClient, maxConnPerRoutePerClient, totalMaxConnPerClient, sslFactory);
      client.start();
      clientPool.add(client);
    }
  }

  @Override
  public void dispatch(
      @Nonnull Scatter<Instance, VenicePath, RouterKey> scatter,
      @Nonnull ScatterGatherRequest<Instance, RouterKey> part,
      @Nonnull VenicePath path,
      @Nonnull BasicHttpRequest request,
      @Nonnull AsyncPromise<Instance> hostSelected,
      @Nonnull AsyncPromise<List<FullHttpResponse>> responseFuture,
      @Nonnull AsyncPromise<HttpResponseStatus> retryFuture,
      @Nonnull AsyncFuture<Void> timeoutFuture,
      @Nonnull Executor contextExecutor) {
    String storeName = path.getStoreName();
    Instance host;
    try {
      int hostCount = part.getHosts().size();
      if (1 != hostCount) {
        throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.of(storeName), Optional.of(path.getRequestType()),
            INTERNAL_SERVER_ERROR, "There should be only one chosen replica for request: " + part);
      }
      host = part.getHosts().get(0);
      hostSelected.setSuccess(host);
    } catch (Exception e) {
      hostSelected.setFailure(e);
      throw e;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Routing request to host: " + host.getHost() + ":" + host.getPort());
    }

    //  http(s)://host:port/path
    String address = scheme + host.getHost() + ":" + host.getPort() + "/";
    final HttpUriRequest routerRequest = path.composeRouterRequest(address);

    CloseableHttpAsyncClient selectedClient = clientPool.get(Math.abs(random.nextInt() % clientPoolSize));
    selectedClient.execute(routerRequest, new FutureCallback<org.apache.http.HttpResponse>() {

      @Override
      public void completed(org.apache.http.HttpResponse result) {
        int statusCode = result.getStatusLine().getStatusCode();
        Set<String> partitionNames = part.getPartitionsNames();
        String resourceName = path.getResourceName();
        String offsetHeader = result.getFirstHeader(HttpConstants.VENICE_OFFSET).getValue();
        // TODO: make this logic consistent across single-get and multi-get
        switch (path.getRequestType()) {
          case SINGLE_GET:
            Iterator<String> partitionIterator = partitionNames.iterator();
            String partitionName = partitionIterator.next();
            if (partitionIterator.hasNext()) {
              logger.error(
                  "There must be only one partition in a request, handling request as if there is only one partition");
            }
            long offset = Long.parseLong(offsetHeader);
            if (statusCode == HttpStatus.SC_OK) {
              checkOffsetLag(resourceName, partitionName, host, offset);
              /*
              // The following code could block online serving if all the partitions are marked as slow.
              // And right now there is no logic to randomly return one host if none is available;
              // TODO: find a way to mark host slow safely.

              healthMontior.setPartitionAsSlow(host, partitionName);
              contextExecutor.execute(() -> {
                // Triggers an immediate router retry excluding the host we selected.
                retryFuture.setSuccess(HttpResponseStatus.SERVICE_UNAVAILABLE);
              });
              return;
              */
            }
            break;
          case MULTI_GET:
            // Get partition offset header
            try {
              Map<Integer, Long> partitionOffsetMap = PartitionOffsetMapUtils.deserializePartitionOffsetMap(offsetHeader);
              partitionNames.forEach(pName -> {
                int partitionId = HelixUtils.getPartitionId(pName);
                if (partitionOffsetMap.containsKey(partitionId)) {
                  /**
                   * TODO: whether we should mark host as slow for multi-get request.
                   *
                   * Right now, the scatter mode being used for multi-get only returns one host per request, so we
                   * could not mark it slow directly if the offset lag is big since it could potentially mark all the hosts
                   * to be slow.
                   *
                   * For streaming case, one possible solution is to use sticky routing so that the requests for a given
                   * partition will hit one specific host consistently.
                   */
                  checkOffsetLag(resourceName, pName, host, partitionOffsetMap.get(partitionId));

                } else {
                  logger.error("Multi-get response doesn't contain offset for partition: " + pName);
                }
              });
            } catch (IOException e) {
              logger.error("Failed to parse partition offset map from content: " + offsetHeader);
            }
            break;
        }
        int responseStatus = result.getStatusLine().getStatusCode();
        FullHttpResponse response;
        byte[] contentToByte;

        try (InputStream contentStream = result.getEntity().getContent()) {
          contentToByte = IOUtils.toByteArray(contentStream);
        } catch (IOException e) {
          completeWithError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
          return;
        }

        ByteBuf content = Unpooled.wrappedBuffer(contentToByte);

        switch (responseStatus){
          case HttpStatus.SC_OK:
            response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, content);
            break;
          case HttpStatus.SC_NOT_FOUND:
            response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND, content);
            break;
          case HttpStatus.SC_INTERNAL_SERVER_ERROR:
          default: //Path Parser will throw BAD_REQUEST responses.
            response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, BAD_GATEWAY, content);
        }

        int valueSchemaId = Integer.parseInt(result.getFirstHeader(HttpConstants.VENICE_SCHEMA_ID).getValue());
        response.headers()
            .set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes())
            .set(HttpHeaderNames.CONTENT_TYPE, result.getFirstHeader(HttpHeaders.CONTENT_TYPE).getValue())
            .set(HttpConstants.VENICE_SCHEMA_ID, valueSchemaId);
        if (path.getRequestType().equals(RequestType.SINGLE_GET)) {
          // For multi-get, the partition is not returned to client
          response.headers().set(HttpConstants.VENICE_PARTITION, numberFromPartitionName(partitionNames.iterator().next()));
        }

        contextExecutor.execute(() -> {
          responseFuture.setSuccess(Collections.singletonList(response));
        });
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

  private String getOffsetKey(String resourceName, String partitionName) {
    return resourceName + "_" + partitionName;
  }

  public void checkOffsetLag(String resourceName, String partitionName, Instance host, long offset) {
    String offsetKey = getOffsetKey(resourceName, partitionName);
    if (offsets.containsKey(offsetKey)) {
      long prevOffset = offsets.get(offsetKey);
      long diff = prevOffset - offset;
      if (diff > acceptableOffsetLag) {
        // TODO: we should find a better way to mark host as unhealthy and still maintain high availability
        // TODO: this piece of log could impact the router performance if it gets printed log file every time
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Host: " + host + ", partition: " + partitionName + " is slower than other replica, offset diff: " + diff
                  + ", and acceptable lag: " + acceptableOffsetLag);
        }
      }
      if (diff < 0) {
        offsets.put(offsetKey, offset);
      }
    } else {
      offsets.put(offsetKey, offset);
    }
  }

  public void close(){
    clientPool.stream().forEach( client -> IOUtils.closeQuietly(client));
  }

  protected static String numberFromPartitionName(String partitionName){
    return partitionName.substring(partitionName.lastIndexOf("_")+1);
  }
}
