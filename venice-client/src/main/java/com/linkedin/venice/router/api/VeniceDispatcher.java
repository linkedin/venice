package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.base.concurrency.AsyncFuture;
import com.linkedin.ddsstorage.base.concurrency.AsyncPromise;
import com.linkedin.ddsstorage.netty3.misc.BasicHttpRequest;
import com.linkedin.ddsstorage.router.api.PartitionDispatchHandler;
import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.ddsstorage.router.api.Scatter;
import com.linkedin.ddsstorage.router.api.ScatterGatherRequest;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.nio.client.HttpAsyncClient;
import org.apache.http.protocol.HTTP;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;


/**
 * Created by mwise on 3/9/16.
 */
public class VeniceDispatcher implements PartitionDispatchHandler<Instance, Path, RouterKey>, Closeable{

  private static final String HTTP = "http://";

  private static final Logger logger = Logger.getLogger(VeniceDispatcher.class);

  // see: https://hc.apache.org/httpcomponents-asyncclient-dev/quickstart.html
  private final Map<Instance, CloseableHttpAsyncClient> clientPool;

  public VeniceDispatcher(){
    clientPool = new ConcurrentHashMap<>();
  }

  @Override
  public void dispatch(
      Scatter<Instance, Path, RouterKey> scatter,
      ScatterGatherRequest<Instance, RouterKey> part,
      Path path,
      BasicHttpRequest request,
      AsyncPromise<Instance> hostSelected,
      AsyncPromise<List<HttpResponse>> responseFuture,
      AsyncPromise<HttpResponseStatus> retryFuture,
      AsyncFuture<Void> timeoutFuture,
      Executor contextExecutor) {

    Instance host;
    try {
      int hostCount = part.getHosts().size();
      host = part.getHosts().get( ((int)System.currentTimeMillis()) % hostCount );  //cheap random host selection
      hostSelected.setSuccess(host);
    } catch (Exception e) {
      hostSelected.setFailure(e);
      throw new VeniceException("Failed to route request to a host");
    }
    logger.info("Routing request to host: " + host.getHost() + ":" + host.getHttpPort());

    CloseableHttpAsyncClient httpClient;
    if (clientPool.containsKey(host)){
      httpClient = clientPool.get(host);
    } else {
      httpClient = HttpAsyncClients.custom()
          .setConnectionReuseStrategy(new DefaultConnectionReuseStrategy())  //Supports connection re-use if able
          .setMaxConnPerRoute(2) // concurrent execute commands beyond this limit get queued internally by the client
          .setMaxConnTotal(2)
          .build();
      clientPool.put(host, httpClient);
    }

    String requestPath = path.getLocation();
    logger.info("Using request path: " + requestPath);

    //  http://host:port/path
    String address = HTTP + host.getHost() + ":" + host.getHttpPort() + "/" + requestPath;
    final HttpGet requestToNode = new HttpGet(address);
    httpClient.execute(requestToNode, new FutureCallback<org.apache.http.HttpResponse>() {

      @Override
      public void completed(org.apache.http.HttpResponse result) {
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        try (InputStream contentStream = result.getEntity().getContent()) {
          response.setContent(ChannelBuffers.wrappedBuffer(IOUtils.toByteArray(contentStream)));
        } catch (IOException e) {
          completeWithError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
          return;
        }
        HttpHeaders.setContentLength(response, response.getContent().readableBytes());
        HttpHeaders.setHeader(response, HttpHeaders.Names.CONTENT_TYPE, org.apache.http.protocol.HTTP.OCTET_STREAM_TYPE);
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
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
        response.setContent(ChannelBuffers.wrappedBuffer(e.getMessage().getBytes(StandardCharsets.UTF_8)));
        HttpHeaders.setContentLength(response, response.getContent().readableBytes());
        HttpHeaders.setHeader(response, HttpHeaders.Names.CONTENT_TYPE, org.apache.http.protocol.HTTP.PLAIN_TEXT_TYPE);
        contextExecutor.execute(() -> {
          responseFuture.setSuccess(Collections.singletonList(response));
        });
      }
    });
  }

  public void close(){
    for (CloseableHttpAsyncClient client : clientPool.values()){
      try {
        client.close();
      } catch (IOException e) {
        logger.error("Error closing an async http client", e);
      }
    }
    clientPool.clear();
  }
}
