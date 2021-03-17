package com.linkedin.davinci.ingestion;

import com.google.common.base.Charsets;
import com.linkedin.common.callback.Callback;
import com.linkedin.common.util.None;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.ingestion.protocol.IngestionMetricsReport;
import com.linkedin.venice.ingestion.protocol.IngestionStorageMetadata;
import com.linkedin.venice.ingestion.protocol.IngestionTaskCommand;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.ingestion.protocol.InitializationConfigs;
import com.linkedin.venice.ingestion.protocol.ProcessShutdownCommand;
import com.linkedin.venice.ingestion.protocol.enums.IngestionAction;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.Utils;
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
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import static com.linkedin.venice.ingestion.protocol.enums.IngestionAction.*;


/**
 * IngestionUtils class contains methods used for communication between ingestion client and server.
 */
public class IngestionUtils {
  private static final Logger logger = Logger.getLogger(IngestionUtils.class);
  private static final int D2_STARTUP_TIMEOUT = 60000;

  private static final InternalAvroSpecificSerializer<InitializationConfigs> initializationConfigSerializer =
          AvroProtocolDefinition.INITIALIZATION_CONFIGS.getSerializer();
  private static final InternalAvroSpecificSerializer<IngestionTaskCommand> ingestionTaskCommandSerializer =
          AvroProtocolDefinition.INGESTION_TASK_COMMAND.getSerializer();
  private static final InternalAvroSpecificSerializer<IngestionTaskReport> ingestionTaskReportSerializer =
          AvroProtocolDefinition.INGESTION_TASK_REPORT.getSerializer();
  private static final InternalAvroSpecificSerializer<IngestionMetricsReport> ingestionMetricsReportSerializer =
          AvroProtocolDefinition.INGESTION_METRICS_REPORT.getSerializer();
  private static final InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer =
      AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer();
  private static final InternalAvroSpecificSerializer<IngestionStorageMetadata> ingestionStorageMetadataSerializer =
      AvroProtocolDefinition.INGESTION_STORAGE_METADATA.getSerializer();
  private static final InternalAvroSpecificSerializer<ProcessShutdownCommand> processShutdownCommandSerializer =
      AvroProtocolDefinition.PROCESS_SHUTDOWN_COMMAND.getSerializer();
  private static final InternalAvroSpecificSerializer<IngestionTaskCommand> ingestionDummyContentSerializer =
      ingestionTaskCommandSerializer;


  private static final Map<IngestionAction, InternalAvroSpecificSerializer> ingestionActionToRequestSerializerMap =
      Stream.of(
          new AbstractMap.SimpleEntry<>(INIT, initializationConfigSerializer),
          new AbstractMap.SimpleEntry<>(COMMAND, ingestionTaskCommandSerializer),
          new AbstractMap.SimpleEntry<>(REPORT, ingestionTaskReportSerializer),
          // TODO: The request of metric is dummy
          new AbstractMap.SimpleEntry<>(METRIC, ingestionDummyContentSerializer),
          // TODO: The request of heartbeat is dummy
          new AbstractMap.SimpleEntry<>(HEARTBEAT, ingestionDummyContentSerializer),
          new AbstractMap.SimpleEntry<>(UPDATE_METADATA, ingestionStorageMetadataSerializer),
          new AbstractMap.SimpleEntry<>(SHUTDOWN_COMPONENT, processShutdownCommandSerializer)
      ).collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

  private static final Map<IngestionAction, InternalAvroSpecificSerializer> ingestionActionToResponseSerializerMap =
      Stream.of(
          // TODO: The response of init is dummy
          new AbstractMap.SimpleEntry<>(INIT, ingestionDummyContentSerializer),
          new AbstractMap.SimpleEntry<>(COMMAND, ingestionTaskReportSerializer),
          // TODO: The response of report is dummy
          new AbstractMap.SimpleEntry<>(REPORT, ingestionDummyContentSerializer),
          new AbstractMap.SimpleEntry<>(METRIC, ingestionMetricsReportSerializer),
          new AbstractMap.SimpleEntry<>(HEARTBEAT, ingestionTaskCommandSerializer),
          new AbstractMap.SimpleEntry<>(UPDATE_METADATA, ingestionTaskReportSerializer),
          new AbstractMap.SimpleEntry<>(SHUTDOWN_COMPONENT, ingestionTaskReportSerializer)
      ).collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

  private static final IngestionTaskCommand dummyCommand = new IngestionTaskCommand();
  static {
    dummyCommand.topicName = "";
  }
  private static final byte[] dummyContent = ingestionTaskCommandSerializer.serialize(null, dummyCommand);

  public static <T extends SpecificRecordBase> byte[] serializeIngestionActionRequest(IngestionAction action, T param) {
    return ingestionActionToRequestSerializerMap.get(action).serialize(null, param);
  }

  public static <T extends SpecificRecordBase> T deserializeIngestionActionRequest(IngestionAction action, byte[] content) {
    return (T) (ingestionActionToRequestSerializerMap.get(action).deserialize(null, content));
  }

  public static <T extends SpecificRecordBase> byte[] serializeIngestionActionResponse(IngestionAction action, T param) {
    return ingestionActionToResponseSerializerMap.get(action).serialize(null, param);
  }

  public static <T extends SpecificRecordBase> T deserializeIngestionActionResponse(IngestionAction action, byte[] content) {
    return (T) (ingestionActionToResponseSerializerMap.get(action).deserialize(null, content));
  }

  public static IngestionTaskCommand getDummyCommand() {
    return dummyCommand;
  }

  public static byte[] getDummyContent() {
    return dummyContent;
  }

  public static byte[] serializeStoreVersionState(String topicName, StoreVersionState storeVersionState) {
    return storeVersionStateSerializer.serialize(topicName, storeVersionState);
  }

  public static StoreVersionState deserializeStoreVersionState(String topicName, byte[] content) {
    return storeVersionStateSerializer.deserialize(topicName, content);
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
   * @param maxAttempt Max number of connection retries before it announces fail to connect.
   * @throws Exception
   */
  public static void waitPortBinding(int port, int maxAttempt) throws Exception {
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
    int retryCount = 0;
    while (true) {
      try {
        ChannelFuture f = bootstrap.connect("localhost", port).sync();
        f.channel().close();
        break;
      } catch (Exception e) {
        retryCount++;
        if (retryCount > maxAttempt) {
          logger.info("Fail to connect to target port " + port + " after " + maxAttempt + " retries.");
          throw e;
        }
        Utils.sleep(waitTime);
      }
    }
    long endTime = System.currentTimeMillis();
    logger.info("Connect time to target port in millis: " + (endTime - startTime));
  }

  /**
   * releaseTargetPortBinding aims to release the target port by killing dangling ingestion isolation process bound to
   * the port, which is created from previous deployment and was not killed due to failures.
   */
  public static void releaseTargetPortBinding(int port) {
    String processIds = executeShellCommand("/usr/sbin/lsof -t -i :" + port);
    if (processIds.length() == 0) {
      logger.info("No process is running on target port");
    }
    logger.info("All processes:\n" + processIds);
    for (String processId : processIds.split("\n")) {
      if (!processId.equals("")) {
        int pid = Integer.parseInt(processId);
        logger.info("Target port: " + port + " is bind to process id: " + pid);
        String fullProcessName = executeShellCommand("ps -p " + pid + " -o command");
        if (fullProcessName.contains(IngestionService.class.getName())) {
          executeShellCommand("kill " +  pid);
          logger.info("Killed IngestionService process on pid " + pid);
        } else {
          logger.info("Target port is bind to unknown process: " + fullProcessName);
        }
      }
    }
  }

  public static String executeShellCommand(String command) {
    try {
      Process process = Runtime.getRuntime().exec(command);
      String output = IOUtils.toString(process.getInputStream());
      int exitCode = process.waitFor();
      if (exitCode != 0) {
        logger.info("Exit code " + exitCode + " when executing shell command: " + command);
        return "";
      }
      IOUtils.closeQuietly(process.getInputStream());
      IOUtils.closeQuietly(process.getOutputStream());
      IOUtils.closeQuietly(process.getErrorStream());
      return output;
    } catch (Exception e) {
      logger.info("Encounter exception when executing shell command: " + command, e);
      return "";
    }
  }
}
