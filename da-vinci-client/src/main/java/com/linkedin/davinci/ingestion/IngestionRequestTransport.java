package com.linkedin.davinci.ingestion;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.ingestion.protocol.enums.IngestionAction;
import com.linkedin.venice.utils.Time;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.io.Closeable;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.log4j.Logger;

import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.*;
import static java.lang.Thread.*;


/**
 * IngestionRequestTransport is a Netty client that sends HttpRequest to listener services
 * and retrieves HttpResponse from channel.
 */
public class IngestionRequestTransport implements Closeable {
  private static final Logger logger = Logger.getLogger(IngestionRequestTransport.class);
  private static final int RETRY_WAIT_TIME_IN_MS = 30 * Time.MS_PER_SECOND;
  private final int port;
  private IngestionRequestTransportHandler responseHandler;
  private final EventLoopGroup workerGroup;
  private final Bootstrap bootstrap;

  public IngestionRequestTransport(int port) {
    this.port = port;
    this.responseHandler = new IngestionRequestTransportHandler();
    workerGroup = new NioEventLoopGroup();
    bootstrap = new Bootstrap();
    bootstrap.group(workerGroup);
    bootstrap.channel(NioSocketChannel.class);
    bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
    bootstrap.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new HttpResponseDecoder());
        ch.pipeline().addLast(new HttpObjectAggregator(1024 * 1024 * 64));
        ch.pipeline().addLast(new HttpRequestEncoder());
        ch.pipeline().addLast(getResponseHandler());
      }
    });
    logger.info("Ingestion Request Transport client created for target port: " + port);
  }

  public <T extends SpecificRecordBase, S extends SpecificRecordBase> T sendRequest(IngestionAction action, S param) {
    String endpoint = "/" + action.toString();
    String hostAndPort = "localhost:" + port;
    // Serialize request content based on IngestionAction.
    byte[] content = serializeIngestionActionRequest(action, param);
    ByteBuf contentBuf = Unpooled.wrappedBuffer(content);
    HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, endpoint, contentBuf);
    httpRequest.headers()
        .set(HttpHeaderNames.HOST, hostAndPort)
        .set(HttpHeaderNames.CONTENT_LENGTH, contentBuf.readableBytes());
    if (logger.isDebugEnabled()) {
      logger.debug("IngestionRequestTransport sending request: " + httpRequest);
    }
    // Exception will be thrown if connection is bad.
    FullHttpResponse response;
    try {
      response = sendRequest(httpRequest);
    } catch (InterruptedException e) {
      throw new VeniceException("Caught interrupted exception ", e);
    }
    if (response == null) {
      throw new VeniceException("Received null response from ingestion process.");
    }
    if (logger.isDebugEnabled()) {
      logger.debug("IngestionRequestTransport received response: " + response);
    }

    try {
      if (!response.status().equals(HttpResponseStatus.OK)) {
        ByteBuf message = response.content();
        String stringMessage = message.readCharSequence(message.readableBytes(), org.apache.commons.io.Charsets.UTF_8).toString();
        throw new VeniceException("Encounter error code: " + response.status() + " with error message: " + stringMessage);
      } else {
        byte[] responseContent = new byte[response.content().readableBytes()];
        response.content().readBytes(responseContent);
        // Deserialize response content based on IngestionAction.
        return deserializeIngestionActionResponse(action, responseContent);
      }
    } finally {
      // FullHttpResponse is a reference-counted object that requires explicit de-allocation.
      response.release();
    }
  }

  public <T extends SpecificRecordBase, S extends SpecificRecordBase> T sendRequestWithRetry(IngestionAction action, S param, int maxAttempt) {
    // Sanity check for maxAttempt argument.
    if (maxAttempt <= 0) {
      throw new IllegalArgumentException("maxAttempt must be a positive integer");
    }
    T result = null;
    int retryCount = 0;
    long startTimeIsMs = System.currentTimeMillis();
    while (true) {
      try {
        result = sendRequest(action, param);
        break;
      } catch (VeniceException e) {
        retryCount++;
        if (retryCount != maxAttempt) {
          logger.warn("Encounter exception when sending request, will retry for " + retryCount + "/" + maxAttempt + " time.");
        } else {
          long totalTimeInMs = System.currentTimeMillis() - startTimeIsMs;
          throw new VeniceException("Failed to send request to remote forked process after " + maxAttempt + " attempts, total time spent in millis: " + totalTimeInMs , e);
        }
      }
      try {
        Thread.sleep(RETRY_WAIT_TIME_IN_MS);
      } catch (InterruptedException e) {
        logger.info("sendRequestWithRetry was interrupted", e);
        currentThread().interrupt();
        break;
      }
    }
    return result;
  }

  @Override
  public void close() {
    workerGroup.shutdownGracefully();
  }

  private synchronized FullHttpResponse sendRequest(HttpRequest request) throws InterruptedException {
    String host = "localhost";
    ChannelFuture f = bootstrap.connect(host, port).sync();
    f.channel().writeAndFlush(request);
    f.channel().closeFuture().sync();
    return responseHandler.getResponse();
  }

  private IngestionRequestTransportHandler getResponseHandler() {
    IngestionRequestTransportHandler handler = new IngestionRequestTransportHandler();
    responseHandler = handler;
    return handler;
  }
}
