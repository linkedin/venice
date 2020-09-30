package com.linkedin.venice.ingestion;

import com.google.common.base.Charsets;
import com.linkedin.common.callback.Callback;
import com.linkedin.common.util.None;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.ingestion.protocol.IngestionMetricsReport;
import com.linkedin.venice.ingestion.protocol.IngestionTaskCommand;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.ingestion.protocol.InitializationConfigs;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
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
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

/**
 * IngestionUtils class contains methods used for communication between ingestion client and server.
 */
public class IngestionUtils {
  private static final Logger logger = Logger.getLogger(IngestionUtils.class);
  private static final int D2_STARTUP_TIMEOUT = 60000;

  public static final InternalAvroSpecificSerializer<InitializationConfigs> initializationConfigSerializer =
          AvroProtocolDefinition.INITIALIZATION_CONFIGS.getSerializer();
  public static final InternalAvroSpecificSerializer<IngestionTaskCommand> ingestionTaskCommandSerializer =
          AvroProtocolDefinition.INGESTION_TASK_COMMAND.getSerializer();
  public static final InternalAvroSpecificSerializer<IngestionTaskReport> ingestionTaskReportSerializer =
          AvroProtocolDefinition.INGESTION_TASK_REPORT.getSerializer();
  public static final InternalAvroSpecificSerializer<IngestionMetricsReport> ingestionMetricsReportSerializer =
          AvroProtocolDefinition.INGESTION_METRICS_REPORT.getSerializer();

  public static byte[] serializeInitializationConfigs(InitializationConfigs initializationConfigs) {
    return initializationConfigSerializer.serialize(null, initializationConfigs);
  }

  public static byte[] serializeIngestionTaskCommand(IngestionTaskCommand ingestionTaskCommand) {
    return ingestionTaskCommandSerializer.serialize(null, ingestionTaskCommand);
  }

  public static byte[] serializeIngestionTaskReport(IngestionTaskReport ingestionTaskReport) {
    return ingestionTaskReportSerializer.serialize(null, ingestionTaskReport);
  }

  public static byte[] serializeIngestionMetricsReport(IngestionMetricsReport ingestionMetricsReport) {
    return ingestionMetricsReportSerializer.serialize(null, ingestionMetricsReport);
  }

  public static IngestionTaskReport deserializeIngestionTaskReport(byte[] content) {
    return ingestionTaskReportSerializer.deserialize(null, content);
  }

  public static IngestionTaskCommand deserializeIngestionTaskCommand(byte[] content) {
    return ingestionTaskCommandSerializer.deserialize(null, content);
  }

  public static InitializationConfigs deserializeInitializationConfigs(byte[] content) {
    return initializationConfigSerializer.deserialize(null, content);
  }

  public static IngestionMetricsReport deserializeIngestionMetricsReport(byte[] content) {
    return ingestionMetricsReportSerializer.deserialize(null, content);
  }

  public static HttpResponse buildHttpResponse(HttpResponseStatus status, String msg) {
    ByteBuf contentBuf = Unpooled.copiedBuffer(msg, Charsets.UTF_8);
    return buildHttpResponse(status, contentBuf);
  }

  public static HttpResponse buildHttpResponse(HttpResponseStatus status, byte[] content) {
    return buildHttpResponse(status, Unpooled.wrappedBuffer(content));
  }

  public static HttpResponse buildHttpResponse(HttpResponseStatus status, ByteBuf contentBuf) {
    HttpResponse httpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, contentBuf);
    httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, contentBuf.readableBytes());
    return httpResponse;
  }

  public static byte[] readHttpResponseContent(FullHttpResponse response) {
    byte[] responseContent = new byte[response.content().readableBytes()];
    response.content().readBytes(responseContent);
    return responseContent;
  }

  public static byte[] readHttpRequestContent(FullHttpRequest response) {
    byte[] responseContent = new byte[response.content().readableBytes()];
    response.content().readBytes(responseContent);
    return responseContent;
  }

  public static void startD2Client(D2Client d2Client) {
    CountDownLatch latch = new CountDownLatch(1);
    d2Client.start(new Callback<None>() {
      @Override
      public void onSuccess(None result) {
        latch.countDown();
        logger.info("D2 client started successfully");
      }

      @Override
      public void onError(Throwable e) {
        latch.countDown();
        logger.error("D2 client failed to startup", e);
      }
    });
    try {
      latch.await(D2_STARTUP_TIMEOUT, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new VeniceException("latch wait was interrupted, d2 client may not have had enough time to startup", e);
    }

    if (latch.getCount() > 0) {
      throw new VeniceException("Time out after " + D2_STARTUP_TIMEOUT + "ms waiting for D2 client to startup");
    }
  }

  /**
   * waitPortBinding is used to test server port binding in other process. Since we cannot control the connection setup
   * on other process, we can only test by trying to establish a connection to the target port.
   * @param port Target port to test connection.
   * @param timeout Connection timeout.
   * @throws Exception
   */
  public static void waitPortBinding(int port, int timeout) throws Exception{
    long timeoutTime = System.currentTimeMillis() + timeout;
    long waitTime = 100;
    EventLoopGroup workerGroup = new NioEventLoopGroup();
    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(workerGroup);
    bootstrap.channel(NioSocketChannel.class);
    bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
    bootstrap.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) {
      }
    });
    long startTime = System.currentTimeMillis();
    while (true) {
      try {
        ChannelFuture f = bootstrap.connect("localhost", port).sync();
        f.channel().close();
        break;
      } catch (Exception e) {
        com.linkedin.venice.utils.Utils.sleep(waitTime);
        if (System.currentTimeMillis() > timeoutTime) {
          throw e;
        }
      }
    }
    long endTime = System.currentTimeMillis();
    logger.info("Connect time in millis: " + (endTime - startTime));
  }
}
