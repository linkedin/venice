package com.linkedin.davinci.ingestion.utils;

import com.linkedin.common.callback.Callback;
import com.linkedin.common.util.None;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.ingestion.isolated.IsolatedIngestionServer;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactoryImpl;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.ingestion.protocol.IngestionMetricsReport;
import com.linkedin.venice.ingestion.protocol.IngestionStorageMetadata;
import com.linkedin.venice.ingestion.protocol.IngestionTaskCommand;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.ingestion.protocol.ProcessShutdownCommand;
import com.linkedin.venice.ingestion.protocol.enums.IngestionAction;
import com.linkedin.venice.ingestion.protocol.enums.IngestionReportType;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.security.DefaultSSLFactory;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
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
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.swing.text.html.Option;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import static com.linkedin.parseq.Task.*;
import static com.linkedin.venice.CommonConfigKeys.*;
import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.ingestion.protocol.enums.IngestionAction.*;


/**
 * IsolatedIngestionUtils class contains methods used for communication between ingestion client and server.
 */
public class IsolatedIngestionUtils {
  public static final String INGESTION_ISOLATION_CONFIG_PREFIX = "isolated";

  private static final Logger logger = Logger.getLogger(IsolatedIngestionUtils.class);
  private static final int D2_STARTUP_TIMEOUT = 60000;
  private static final int SHELL_COMMAND_WAIT_TIME = 1000;

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
    ByteBuf contentBuf = Unpooled.copiedBuffer(msg, StandardCharsets.UTF_8);
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
   * releaseTargetPortBinding aims to release the target port by killing lingering ingestion process bound to
   * the port, which is created from previous deployment and was not killed due to unexpected failures.
   */
  public static void releaseTargetPortBinding(int port) {
    logger.info("Releasing binging on target port: " + port);
    Optional<Integer> lingeringIngestionProcessPid = getLingeringIngestionProcessId(port);
    if (lingeringIngestionProcessPid.isPresent()) {
      int pid = lingeringIngestionProcessPid.get();
      logger.info("Lingering ingestion process ID: " + pid);
      executeShellCommand("kill " + pid);
      boolean hasLingeringProcess = true;
      while (hasLingeringProcess) {
        try {
          Thread.sleep(SHELL_COMMAND_WAIT_TIME);
          if (!getLingeringIngestionProcessId(port).isPresent()) {
            logger.info("Lingering ingestion process on pid " + pid + " is killed.");
            hasLingeringProcess = false;
          }
        } catch (InterruptedException e) {
          // Keep the interruption flag.
          Thread.currentThread().interrupt();
          logger.info("Shell command execution was interrupted");
          break;
        }
      }
    } else {
      logger.info("No lingering ingestion process is running on target port: " + port);
    }
  }

  /**
   * This method returns lingering forked ingestion process PID if it exists.
   * Since the lsof command will return all processes associated with the port number(including main process), we will
   * need to iterate all PIDs and filter out ingestion process by name.
   */
  public static Optional<Integer> getLingeringIngestionProcessId(int port) {
    Optional<Integer> isolatedIngestionProcessPid = Optional.empty();
    String processIds = executeShellCommand("/usr/sbin/lsof -t -i :" + port);
    if (processIds.length() != 0) {
      logger.info("Target port is associated to process IDs:\n" + processIds);
      for (String processId : processIds.split("\n")) {
        if (!processId.equals("")) {
          int pid = Integer.parseInt(processId);
          String fullProcessName = executeShellCommand("ps -p " + pid + " -o command");
          logger.info("Target port: " + port + " is associated to process: " + fullProcessName + " with pid: " + pid);
          if (fullProcessName.contains(IsolatedIngestionServer.class.getName())) {
            isolatedIngestionProcessPid = Optional.of(pid);
          }
        }
      }
    }
    return isolatedIngestionProcessPid;
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
      Utils.closeQuietlyWithErrorLogged(process.getInputStream());
      Utils.closeQuietlyWithErrorLogged(process.getOutputStream());
      Utils.closeQuietlyWithErrorLogged(process.getErrorStream());
      return output;
    } catch (Exception e) {
      logger.info("Encounter exception when executing shell command: " + command, e);
      return "";
    }
  }

  public static IngestionTaskReport createIngestionTaskReport(IngestionReportType ingestionReportType, String kafkaTopic, int partitionId, long offset, String message) {
    IngestionTaskReport report = new IngestionTaskReport();
    report.reportType = ingestionReportType.getValue();
    report.message = message;
    report.topicName = kafkaTopic;
    report.partitionId = partitionId;
    report.offset = offset;
    report.offsetRecordArray = Collections.emptyList();
    return report;
  }

  public static IngestionTaskReport createIngestionTaskReport(String kafkaTopic, int partitionId) {
    IngestionTaskReport report = new IngestionTaskReport();
    report.isPositive = true;
    report.message = "";
    report.topicName = kafkaTopic;
    report.partitionId = partitionId;
    report.offset = 0;
    report.offsetRecordArray = Collections.emptyList();
    return report;
  }

  public static void destroyPreviousIsolatedIngestionProcess(Process isolatedIngestionServiceProcess) {
    if (isolatedIngestionServiceProcess != null) {
      long startTime = System.currentTimeMillis();
      logger.info("Destroying lingering isolated ingestion process.");
      isolatedIngestionServiceProcess.destroy();
      long endTime = System.currentTimeMillis();
      logger.info("Isolated ingestion process has been destroyed in " + (endTime - startTime) + "ms.");
    }
  }

  public static IngestionTaskReport createIngestionTaskReport() {
    return createIngestionTaskReport("", 0);
  }

  public static IngestionTaskReport createIngestionTaskReport(IngestionReportType ingestionReportType, String kafkaTopic, int partitionId) {
    return createIngestionTaskReport(ingestionReportType, kafkaTopic, partitionId, 0, "");
  }

  public static IngestionTaskReport createIngestionTaskReport(IngestionReportType ingestionReportType, String kafkaTopic, int partitionId, String message) {
    return createIngestionTaskReport(ingestionReportType, kafkaTopic, partitionId, 0, message);
  }

  public static String buildAndSaveConfigsForForkedIngestionProcess(VeniceConfigLoader configLoader) {
    String configFileName = "ForkedIngestionProcess.conf";
    PropertyBuilder propertyBuilder = new PropertyBuilder();
    configLoader.getCombinedProperties().toProperties().forEach((key, value) -> propertyBuilder.put(key.toString(), value.toString()));
    // Override ingestion isolation's customized configs.
    configLoader.getCombinedProperties().clipAndFilterNamespace(INGESTION_ISOLATION_CONFIG_PREFIX).toProperties()
        .forEach((key, value) -> propertyBuilder.put(key.toString(), value.toString()));
    VeniceProperties veniceProperties = propertyBuilder.build();

    String configBasePath = configLoader.getVeniceServerConfig().getDataBasePath();
    try {
      // Make sure the base path exists so we can store the config file in the path.
      Files.createDirectories(Paths.get(configBasePath));
    } catch (IOException e) {
      throw new VeniceException(e);
    }

    String configFilePath = Paths.get(configBasePath, configFileName).toAbsolutePath().toString();
    File initializationConfig = new File(configFilePath);
    if (initializationConfig.exists()) {
      LOGGER.warn("Initialization config file already exists, will delete the old configs.");
      if (!initializationConfig.delete()) {
        throw new VeniceException("Unable to delete config file at path: " + configFilePath);
      }
    }
    try {
      veniceProperties.storeFlattened(initializationConfig);
      logger.info("Forked process configs are stored into: " + configFilePath);
    } catch (IOException e) {
      throw new VeniceException(e);
    }
    return configFilePath;
  }

  public static Optional<SSLEngineComponentFactory> getSSLEngineComponentFactory(VeniceConfigLoader configLoader) {
    try {
      if (configLoader.getCombinedProperties().getBoolean(SERVER_INGESTION_ISOLATION_SSL_ENABLED, false)) {
        // If SSL communication is enabled but SSL configs are missing, we should fail fast here.
        if (!configLoader.getCombinedProperties().getBoolean(SSL_ENABLED, false)) {
          throw new VeniceException("Ingestion isolation SSL is enabled for communication, but SSL configs are missing.");
        }
        logger.info("SSL is enabled, will create SSLEngineComponentFactory");
        SSLConfig sslConfig = new SSLConfig(configLoader.getCombinedProperties());
        return Optional.of(new SSLEngineComponentFactoryImpl(sslConfig.getSslEngineComponentConfig()));
      } else {
        logger.warn("SSL is not enabled");
        return Optional.empty();
      }
    } catch (Exception e) {
      throw new VeniceException("Caught exception during SSLEngineComponentFactory creation", e);
    }
  }

  /**
   * Create SSLFactory for inter-process communication encryption purpose.
   */
  public static Optional<SSLFactory> getSSLFactoryForInterProcessCommunication(VeniceConfigLoader configLoader) {
    if (configLoader.getCombinedProperties().getBoolean(SERVER_INGESTION_ISOLATION_SSL_ENABLED, false)) {
      try {
        // If SSL communication is enabled but SSL configs are missing, we should fail fast here.
        if (!configLoader.getCombinedProperties().getBoolean(SSL_ENABLED, false)) {
          throw new VeniceException("Ingestion isolation SSL is enabled for communication, but SSL configs are missing.");
        }
        logger.info("SSL is enabled, will create SSLFactory");
        return Optional.of(new DefaultSSLFactory(configLoader.getCombinedProperties().toProperties()));
      } catch (Exception e) {
        throw new VeniceException("Caught exception during SSLFactory creation", e);
      }
    } else {
      return Optional.empty();
    }
  }

  /**
   * Create SSLFactory for D2Client in ClientConfig, which will be used by different ingestion components.
   */
  public static Optional<SSLFactory> getSSLFactoryForIngestion(VeniceConfigLoader configLoader) {
    if (configLoader.getCombinedProperties().getBoolean(SSL_ENABLED, false)) {
      try {
        logger.info("SSL is enabled, will create SSLFactory");
        return Optional.of(new DefaultSSLFactory(configLoader.getCombinedProperties().toProperties()));
      } catch (Exception e) {
        throw new VeniceException("Caught exception during SSLFactory creation", e);
      }
    } else {
      return Optional.empty();
    }
  }
}
