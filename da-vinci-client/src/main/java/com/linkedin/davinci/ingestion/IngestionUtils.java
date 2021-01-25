package com.linkedin.davinci.ingestion;

import com.google.common.base.Charsets;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.common.callback.Callback;
import com.linkedin.common.util.None;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.ingestion.protocol.IngestionMetricsReport;
import com.linkedin.venice.ingestion.protocol.IngestionStorageMetadata;
import com.linkedin.venice.ingestion.protocol.IngestionTaskCommand;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.ingestion.protocol.InitializationConfigs;
import com.linkedin.venice.ingestion.protocol.ProcessShutdownCommand;
import com.linkedin.venice.ingestion.protocol.enums.IngestionCommandType;
import com.linkedin.venice.ingestion.protocol.enums.IngestionComponentType;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.IngestionAction;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.ForkedJavaProcess;
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
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
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
  public static final InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer =
      AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer();
  public static final InternalAvroSpecificSerializer<IngestionStorageMetadata> ingestionStorageMetadataSerializer =
      AvroProtocolDefinition.INGESTION_STORAGE_METADATA.getSerializer();
  public static final InternalAvroSpecificSerializer<ProcessShutdownCommand> processShutdownCommandSerializer =
      AvroProtocolDefinition.PROCESS_SHUTDOWN_COMMAND.getSerializer();

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

  public static byte[] serializeStoreVersionState(String topicName, StoreVersionState storeVersionState) {
    return storeVersionStateSerializer.serialize(topicName, storeVersionState);
  }

  public static byte[] serializeIngestionStorageMetadata(IngestionStorageMetadata ingestionStorageMetadata) {
    return ingestionStorageMetadataSerializer.serialize(null, ingestionStorageMetadata);
  }

  public static byte[] serializeProcessShutdownCommand(ProcessShutdownCommand processShutdownCommand) {
    return processShutdownCommandSerializer.serialize(null, processShutdownCommand);
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

  public static StoreVersionState deserializeStoreVersionState(String topicName, byte[] content) {
    return storeVersionStateSerializer.deserialize(topicName, content);
  }

  public static IngestionStorageMetadata deserializeIngestionStorageMetadata(byte[] content) {
    return ingestionStorageMetadataSerializer.deserialize(null, content);
  }

  public static ProcessShutdownCommand deserializeProcessShutdownCommand(byte[] content) {
    return processShutdownCommandSerializer.deserialize(null, content);
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

  public static void waitHealthCheck(IngestionRequestClient client, int maxAttempt) throws Exception {
    long waitTime = 100;
    IngestionTaskCommand ingestionTaskCommand = new IngestionTaskCommand();
    ingestionTaskCommand.commandType = IngestionCommandType.HEARTBEAT.getValue();
    ingestionTaskCommand.topicName = "";
    byte[] content = serializeIngestionTaskCommand(ingestionTaskCommand);
    HttpRequest httpRequest = client.buildHttpRequest(IngestionAction.COMMAND, content);
    int retryCount = 0;
    while (true) {
      try {
        FullHttpResponse response = client.sendRequest(httpRequest);
        if (response.status().equals(HttpResponseStatus.OK)) {
          logger.info("Ingestion service server health check passed.");
          break;
        } else {
          throw new VeniceException("Got non-OK response from ingestion service: " + response.status());
        }
      } catch (Exception e) {
        retryCount++;
        if (retryCount > maxAttempt) {
          logger.info("Fail to pass health-check for ingestion service after " + maxAttempt + " retries.");
          throw e;
        }
        Utils.sleep(waitTime);
      }
    }
  }


  public static Process startForkedIngestionProcess(VeniceConfigLoader configLoader) {
    int ingestionServicePort = configLoader.getVeniceServerConfig().getIngestionServicePort();
    try (IngestionRequestClient ingestionRequestClient = new IngestionRequestClient(ingestionServicePort)) {
      // Add blocking call to release target port binding.
      releaseTargetPortBinding(ingestionServicePort);
      // Start forking child ingestion process.
      Process isolatedIngestionService = ForkedJavaProcess.exec(IngestionService.class, String.valueOf(ingestionServicePort));
      // Wait for server in forked child process to bind the listening port.
      waitPortBinding(ingestionServicePort, 100);
      // Wait for server in forked child process to pass health check.
      waitHealthCheck(ingestionRequestClient, 100);
      InitializationConfigs initializationConfigs = new InitializationConfigs();
      initializationConfigs.aggregatedConfigs = new HashMap<>();
      configLoader.getCombinedProperties().toProperties().forEach((key, value) -> initializationConfigs.aggregatedConfigs.put(key.toString(), value.toString()));
      // Block restoring on disk store data as it has been restored on the main process when live update suppression is enabled.
      if (configLoader.getVeniceServerConfig().freezeIngestionIfReadyToServeOrLocalDataExists()) {
        initializationConfigs.aggregatedConfigs.put(ConfigKeys.SERVER_RESTORE_DATA_PARTITIONS_ENABLED, "false");
        initializationConfigs.aggregatedConfigs.put(ConfigKeys.SERVER_RESTORE_METADATA_PARTITION_ENABLED, "false");
      }
      logger.info("Sending initialization aggregatedConfigs to child process: " + initializationConfigs.aggregatedConfigs);
      byte[] content = serializeInitializationConfigs(initializationConfigs);
      HttpRequest httpRequest = ingestionRequestClient.buildHttpRequest(IngestionAction.INIT, content);
      FullHttpResponse response = ingestionRequestClient.sendRequest(httpRequest);
      if (!response.status().equals(HttpResponseStatus.OK)) {
        ByteBuf message = response.content();
        String stringMessage = message.readCharSequence(message.readableBytes(), org.apache.commons.io.Charsets.UTF_8).toString();
        throw new VeniceException("Isolated ingestion service initialization failed: " + stringMessage);
      }
      // FullHttpResponse is a reference-counted object that requires explicit de-allocation.
      response.release();
      logger.info("Isolated ingestion service initialization finished.");
      return isolatedIngestionService;
    } catch (Exception e) {
      throw new VeniceException("Exception caught during initialization of ingestion service.", e);
    }
  }

  public static void subscribeTopicPartition(IngestionRequestClient client, String topicName, int partitionId) {
    // Send ingestion request to ingestion service.
    IngestionTaskCommand ingestionTaskCommand = new IngestionTaskCommand();
    ingestionTaskCommand.commandType = IngestionCommandType.START_CONSUMPTION.getValue();
    ingestionTaskCommand.topicName = topicName;
    ingestionTaskCommand.partitionId = partitionId;
    byte[] content = serializeIngestionTaskCommand(ingestionTaskCommand);
    try {
      HttpRequest httpRequest = client.buildHttpRequest(IngestionAction.COMMAND, content);
      FullHttpResponse response = client.sendRequest(httpRequest);
      if (response.status().equals(HttpResponseStatus.OK)) {
        byte[] responseContent = new byte[response.content().readableBytes()];
        response.content().readBytes(responseContent);
        IngestionTaskReport ingestionTaskReport = deserializeIngestionTaskReport(responseContent);
        logger.info("Received ingestion task report response: " + ingestionTaskReport);
      } else {
        logger.warn("Received bad ingestion task report response: " + response.status() + " for topic: " + topicName + ", partition: " + partitionId);
      }
      // FullHttpResponse is a reference-counted object that requires explicit de-allocation.
      response.release();
    } catch (Exception e) {
      throw new VeniceException("Received exception in start consumption", e);
    }
  }

  public static void shutdownForkedProcessComponent(IngestionRequestClient client, IngestionComponentType ingestionComponentType) {
    // Send ingestion request to ingestion service.
    ProcessShutdownCommand processShutdownCommand = new ProcessShutdownCommand();
    processShutdownCommand.componentType = ingestionComponentType.getValue();

    byte[] content = serializeProcessShutdownCommand(processShutdownCommand);
    try {
      HttpRequest httpRequest = client.buildHttpRequest(IngestionAction.SHUTDOWN_COMPONENT, content);
      FullHttpResponse response = client.sendRequest(httpRequest);
      if (response.status().equals(HttpResponseStatus.OK)) {
        byte[] responseContent = new byte[response.content().readableBytes()];
        response.content().readBytes(responseContent);
        IngestionTaskReport ingestionTaskReport = deserializeIngestionTaskReport(responseContent);
        logger.info("Received ingestion task report response: " + ingestionTaskReport);
      } else {
        logger.warn("Received bad ingestion task report response: " + response.status() + " for shutting down component" + ingestionComponentType);
      }
      // FullHttpResponse is a reference-counted object that requires explicit de-allocation.
      response.release();
    } catch (Exception e) {
      throw new VeniceException("Received exception in component shutdown", e);
    }
  }

  static String executeShellCommand(String command) {
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
