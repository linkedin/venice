package com.linkedin.davinci.ingestion.utils;

import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_TYPE;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEY_PASSWORD;
import static com.linkedin.venice.ConfigKeys.IDENTITY_PARSER_CLASS;
import static com.linkedin.venice.ConfigKeys.LOCAL_REGION_NAME;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_ACL_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_PRINCIPAL_NAME;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_SSL_ENABLED;
import static com.linkedin.venice.ingestion.protocol.enums.IngestionAction.COMMAND;
import static com.linkedin.venice.ingestion.protocol.enums.IngestionAction.GET_LOADED_STORE_USER_PARTITION_MAPPING;
import static com.linkedin.venice.ingestion.protocol.enums.IngestionAction.HEARTBEAT;
import static com.linkedin.venice.ingestion.protocol.enums.IngestionAction.METRIC;
import static com.linkedin.venice.ingestion.protocol.enums.IngestionAction.REPORT;
import static com.linkedin.venice.ingestion.protocol.enums.IngestionAction.SHUTDOWN_COMPONENT;
import static com.linkedin.venice.ingestion.protocol.enums.IngestionAction.UPDATE_METADATA;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.ingestion.isolated.IsolatedIngestionServer;
import com.linkedin.davinci.ingestion.isolated.IsolatedIngestionServerAclHandler;
import com.linkedin.venice.authorization.DefaultIdentityParser;
import com.linkedin.venice.authorization.IdentityParser;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.ingestion.protocol.IngestionMetricsReport;
import com.linkedin.venice.ingestion.protocol.IngestionStorageMetadata;
import com.linkedin.venice.ingestion.protocol.IngestionTaskCommand;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.ingestion.protocol.LoadedStoreUserPartitionMapping;
import com.linkedin.venice.ingestion.protocol.ProcessShutdownCommand;
import com.linkedin.venice.ingestion.protocol.enums.IngestionAction;
import com.linkedin.venice.ingestion.protocol.enums.IngestionReportType;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.security.DefaultSSLFactory;
import com.linkedin.venice.security.SSLConfig;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.ForkedJavaProcess;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.RegionUtils;
import com.linkedin.venice.utils.SslUtils;
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
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class contains methods used for communication between ingestion client and server.
 */
public class IsolatedIngestionUtils {
  public static final String INGESTION_ISOLATION_CONFIG_PREFIX = "isolated";
  public static final String ISOLATED_INGESTION_CONFIG_FILENAME = "IsolatedIngestionConfig.conf";
  public static final String ISOLATED_INGESTION_KAFKA_CLUSTER_MAP_FILENAME = "IsolatedIngestionKafkaClusterMap.conf";
  public static final String FORKED_PROCESS_METADATA_FILENAME = "ForkedProcessMetadata.conf";

  public static final String PID = "pid";

  private static final Logger LOGGER = LogManager.getLogger(IsolatedIngestionUtils.class);
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
  private static final InternalAvroSpecificSerializer<LoadedStoreUserPartitionMapping> storeUserPartitionMappingSerializer =
      AvroProtocolDefinition.LOADED_STORE_USER_PARTITION_MAPPING.getSerializer();

  private static final Map<IngestionAction, InternalAvroSpecificSerializer> ingestionActionToRequestSerializerMap =
      Stream
          .of(
              new AbstractMap.SimpleEntry<>(COMMAND, ingestionTaskCommandSerializer),
              new AbstractMap.SimpleEntry<>(REPORT, ingestionTaskReportSerializer),
              new AbstractMap.SimpleEntry<>(METRIC, ingestionMetricsReportSerializer),
              new AbstractMap.SimpleEntry<>(HEARTBEAT, ingestionDummyContentSerializer),
              new AbstractMap.SimpleEntry<>(UPDATE_METADATA, ingestionStorageMetadataSerializer),
              new AbstractMap.SimpleEntry<>(SHUTDOWN_COMPONENT, processShutdownCommandSerializer),
              new AbstractMap.SimpleEntry<>(GET_LOADED_STORE_USER_PARTITION_MAPPING, ingestionDummyContentSerializer))
          .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

  private static final Map<IngestionAction, InternalAvroSpecificSerializer> ingestionActionToResponseSerializerMap =
      Stream.of(
          new AbstractMap.SimpleEntry<>(COMMAND, ingestionTaskReportSerializer),
          new AbstractMap.SimpleEntry<>(REPORT, ingestionDummyContentSerializer),
          new AbstractMap.SimpleEntry<>(METRIC, ingestionDummyContentSerializer),
          new AbstractMap.SimpleEntry<>(HEARTBEAT, ingestionTaskCommandSerializer),
          new AbstractMap.SimpleEntry<>(UPDATE_METADATA, ingestionTaskReportSerializer),
          new AbstractMap.SimpleEntry<>(SHUTDOWN_COMPONENT, ingestionTaskReportSerializer),
          new AbstractMap.SimpleEntry<>(GET_LOADED_STORE_USER_PARTITION_MAPPING, storeUserPartitionMappingSerializer))
          .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

  private static final IngestionTaskCommand DUMMY_COMMAND = new IngestionTaskCommand();
  static {
    DUMMY_COMMAND.topicName = "";
  }
  private static final byte[] DUMMY_CONTENT = ingestionTaskCommandSerializer.serialize(null, DUMMY_COMMAND);

  public static <T extends SpecificRecordBase> byte[] serializeIngestionActionRequest(IngestionAction action, T param) {
    return ingestionActionToRequestSerializerMap.get(action).serialize(null, param);
  }

  public static <T extends SpecificRecordBase> T deserializeIngestionActionRequest(
      IngestionAction action,
      byte[] content) {
    return (T) (ingestionActionToRequestSerializerMap.get(action).deserialize(null, content));
  }

  public static <T extends SpecificRecordBase> byte[] serializeIngestionActionResponse(
      IngestionAction action,
      T param) {
    return ingestionActionToResponseSerializerMap.get(action).serialize(null, param);
  }

  public static <T extends SpecificRecordBase> T deserializeIngestionActionResponse(
      IngestionAction action,
      byte[] content) {
    return (T) (ingestionActionToResponseSerializerMap.get(action).deserialize(null, content));
  }

  public static IngestionTaskCommand getDummyCommand() {
    return DUMMY_COMMAND;
  }

  public static byte[] getDummyContent() {
    return DUMMY_CONTENT;
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

  public static IngestionAction getIngestionActionFromRequest(HttpRequest req) {
    // Sometimes req.uri() gives a full uri (eg https://host:port/path) and sometimes it only gives a path
    // Generating a URI lets us always take just the path.
    String[] requestParts = URI.create(req.uri()).getPath().split("/");
    HttpMethod reqMethod = req.method();
    if (!reqMethod.equals(HttpMethod.POST) || requestParts.length < 2) {
      throw new VeniceException("Cannot parse request for: " + req.uri());
    }

    try {
      return IngestionAction.valueOf(requestParts[1].toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new VeniceException(
          "Only able to parse POST requests for IngestionActions. Cannot support action: " + requestParts[1],
          e);
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
          LOGGER.info("Fail to connect to target port {} after {} retries.", port, maxAttempt);
          throw e;
        }
        Utils.sleep(waitTime);
      }
    }
    long endTime = System.currentTimeMillis();
    LOGGER.info("Connect time to target port in millis: {}", endTime - startTime);
  }

  /**
   * releaseTargetPortBinding aims to release the target port by killing lingering ingestion process bound to
   * the port, which is created from previous deployment and was not killed due to unexpected failures.
   */
  public static void releaseTargetPortBinding(int port) {
    LOGGER.info("Releasing isolated ingestion process binding on target port: {}", port);
    Optional<Integer> lingeringIngestionProcessPid = getLingeringIngestionProcessId(port);
    if (lingeringIngestionProcessPid.isPresent()) {
      int pid = lingeringIngestionProcessPid.get();
      LOGGER.info("Found lingering ingestion process ID: {}", pid);
      executeShellCommand("kill -9 " + pid);
      boolean hasLingeringProcess = true;
      while (hasLingeringProcess) {
        try {
          Thread.sleep(SHELL_COMMAND_WAIT_TIME);
          lingeringIngestionProcessPid = getLingeringIngestionProcessId(port);
          if (!lingeringIngestionProcessPid.isPresent() || lingeringIngestionProcessPid.get() != pid) {
            LOGGER.info("Lingering ingestion process on pid {} is killed.", pid);
            hasLingeringProcess = false;
          }
        } catch (InterruptedException e) {
          // Keep the interruption flag.
          Thread.currentThread().interrupt();
          LOGGER.info("Shell command execution was interrupted");
          break;
        }
      }
    } else {
      LOGGER.info("No lingering ingestion process is running on target port: {}", port);
    }
  }

  /**
   * This method returns lingering forked ingestion process PID if it exists.
   * Since the lsof command will return all processes associated with the port number(including main process), we will
   * need to iterate all PIDs and filter out ingestion process by name.
   */
  public static Optional<Integer> getLingeringIngestionProcessId(int port) {
    Optional<Integer> isolatedIngestionProcessPid = Optional.empty();
    String cmd = "lsof -t -i :" + port;
    String processIds = executeShellCommand("/usr/sbin/" + cmd);
    if (processIds.isEmpty()) {
      processIds = executeShellCommand(cmd);
    }
    if (processIds.length() != 0) {
      LOGGER.info("Target port is associated to process IDs:\n {}", processIds);
      for (String processId: processIds.split("\n")) {
        if (!processId.equals("")) {
          int pid = Integer.parseInt(processId);
          String fullProcessName = executeShellCommand("ps -p " + pid + " -o command");
          LOGGER.info("Target port: {} is associated to process: {} with pid: {}", port, fullProcessName, pid);
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
        if (command.contains("kill")) {
          throw new VeniceException("Encountered exitCode " + exitCode + " when executing shell command: " + command);
        } else {
          LOGGER.info("Exit code {} when executing shell command: {}", exitCode, command);
          return "";
        }
      }
      Utils.closeQuietlyWithErrorLogged(process.getInputStream());
      Utils.closeQuietlyWithErrorLogged(process.getOutputStream());
      Utils.closeQuietlyWithErrorLogged(process.getErrorStream());
      return output;
    } catch (Exception e) {
      if (command.contains("kill")) {
        throw new VeniceException("Encountered exception when executing shell command: " + command, e);
      } else if (e.getMessage().contains("No such file or directory")) {
        // This is a common case, so we mute the full exception stacktrace.
        LOGGER.info("Command not found. Exception message: {}", e.getMessage());
      } else {
        LOGGER.info("Encounter exception when executing shell command: {}", command, e);
      }
      return "";
    }
  }

  public static IngestionTaskReport createIngestionTaskReport(
      IngestionReportType ingestionReportType,
      String kafkaTopic,
      int partitionId,
      long offset,
      String message) {
    IngestionTaskReport report = new IngestionTaskReport();
    report.reportType = ingestionReportType.getValue();
    report.message = message;
    report.topicName = kafkaTopic;
    report.partitionId = partitionId;
    report.offset = offset;
    return report;
  }

  public static IngestionTaskReport createIngestionTaskReport(String kafkaTopic, int partitionId) {
    IngestionTaskReport report = new IngestionTaskReport();
    report.isPositive = true;
    report.message = "";
    report.topicName = kafkaTopic;
    report.partitionId = partitionId;
    report.offset = 0;
    return report;
  }

  public static void destroyIsolatedIngestionProcess(Process isolatedIngestionServiceProcess) {
    if (isolatedIngestionServiceProcess != null) {
      long startTime = System.currentTimeMillis();
      LOGGER.info("Destroying lingering isolated ingestion process.");
      isolatedIngestionServiceProcess.destroy();
      long endTime = System.currentTimeMillis();
      LOGGER.info("Isolated ingestion process has been destroyed in {} ms.", endTime - startTime);
    }
  }

  /**
   * Kill isolated ingestion process by provided PID.
   */
  public static void destroyIsolatedIngestionProcessByPid(long pid) {
    String fullProcessName = executeShellCommand("ps -p " + pid + " -o command");
    if (fullProcessName.contains(IsolatedIngestionServer.class.getName())) {
      executeShellCommand("kill -9 " + pid);
    } else {
      LOGGER.warn("PID: {} does not belong to isolated ingestion process, will not kill the process.", pid);
    }
  }

  /**
   * This method takes two steps to kill any lingering forked ingestion process previously created by the same user.
   * It first tries to locate the PID of the lingering forked ingestion process stored in the metadata file. It then uses
   * lsof command to locate isolated ingestion process binding to the same port and kill it.
   */
  public static void destroyLingeringIsolatedIngestionProcess(VeniceConfigLoader configLoader) {
    try {
      String configBasePath = configLoader.getVeniceServerConfig().getDataBasePath();
      VeniceProperties properties = loadVenicePropertiesFromFile(configBasePath, FORKED_PROCESS_METADATA_FILENAME);
      if (properties.containsKey(PID)) {
        long pid = properties.getLong(PID);
        destroyIsolatedIngestionProcessByPid(pid);
      }
    } catch (FileNotFoundException e) {
      LOGGER.info("No lingering ingestion process was found, so there is nothing to cleanup. Moving on.");
    } catch (Exception e) {
      LOGGER.warn("Caught an exception while trying to clean up a lingering ingestion process.", e);
    }
    releaseTargetPortBinding(configLoader.getVeniceServerConfig().getIngestionServicePort());
  }

  public static IngestionTaskReport createIngestionTaskReport() {
    return createIngestionTaskReport("", 0);
  }

  public static IngestionTaskReport createIngestionTaskReport(
      IngestionReportType ingestionReportType,
      String kafkaTopic,
      int partitionId,
      String message) {
    return createIngestionTaskReport(ingestionReportType, kafkaTopic, partitionId, 0, message);
  }

  public static String buildAndSaveConfigsForForkedIngestionProcess(VeniceConfigLoader configLoader) {
    PropertyBuilder propertyBuilder = new PropertyBuilder();
    configLoader.getCombinedProperties()
        .toProperties()
        .forEach((key, value) -> propertyBuilder.put(key.toString(), value.toString()));
    // Override ingestion isolation's customized configs.
    configLoader.getCombinedProperties()
        .clipAndFilterNamespace(INGESTION_ISOLATION_CONFIG_PREFIX)
        .toProperties()
        .forEach((key, value) -> propertyBuilder.put(key.toString(), value.toString()));
    IdentityParser identityParser = getIdentityParser(configLoader);
    propertyBuilder.put(IDENTITY_PARSER_CLASS, identityParser.getClass().getName());
    maybePopulateServerIngestionPrincipal(propertyBuilder, configLoader, identityParser);
    // Populate region name to isolated ingestion process resolved from env/system property.
    propertyBuilder.put(LOCAL_REGION_NAME, RegionUtils.getLocalRegionName(configLoader.getCombinedProperties(), false));
    VeniceProperties veniceProperties = propertyBuilder.build();
    String configBasePath = configLoader.getVeniceServerConfig().getDataBasePath();
    return storeVenicePropertiesToFile(configBasePath, ISOLATED_INGESTION_CONFIG_FILENAME, veniceProperties);
  }

  public static void saveForkedIngestionKafkaClusterMapConfig(VeniceConfigLoader configLoader) {
    String configBasePath = configLoader.getVeniceServerConfig().getDataBasePath();
    String configFilePath =
        Paths.get(configBasePath, ISOLATED_INGESTION_KAFKA_CLUSTER_MAP_FILENAME).toAbsolutePath().toString();
    File configFile = new File(configFilePath);
    if (configFile.exists()) {
      LOGGER.info("Kafka cluster map file already exists, will delete existing file: {}", configFilePath);
      if (!configFile.delete()) {
        throw new VeniceException("Unable to delete config file: " + configFilePath);
      }
    }
    try {
      VeniceConfigLoader.storeKafkaClusterMap(
          new File(configBasePath),
          ISOLATED_INGESTION_KAFKA_CLUSTER_MAP_FILENAME,
          configLoader.getVeniceClusterConfig().getKafkaClusterMap());
    } catch (Exception e) {
      throw new VeniceException("Failed to store Kafka cluster map for isolated ingestion process", e);
    }
  }

  public static Map<String, Map<String, String>> loadForkedIngestionKafkaClusterMapConfig(String configBasePath) {
    try {
      return VeniceConfigLoader.parseKafkaClusterMap(configBasePath, ISOLATED_INGESTION_KAFKA_CLUSTER_MAP_FILENAME);
    } catch (Exception e) {
      throw new VeniceException("Failed to parse Kafka cluster map for isolated ingestion process", e);
    }
  }

  public static void saveForkedIngestionProcessMetadata(
      VeniceConfigLoader configLoader,
      ForkedJavaProcess forkedJavaProcess) {
    PropertyBuilder propertyBuilder = new PropertyBuilder();
    propertyBuilder.put(PID, forkedJavaProcess.pid());
    VeniceProperties veniceProperties = propertyBuilder.build();
    String configBasePath = configLoader.getVeniceServerConfig().getDataBasePath();
    storeVenicePropertiesToFile(configBasePath, FORKED_PROCESS_METADATA_FILENAME, veniceProperties);
  }

  public static VeniceProperties loadVenicePropertiesFromFile(String configPath) throws FileNotFoundException {
    if (!(new File(configPath).exists())) {
      throw new FileNotFoundException("Config file: " + configPath + " does not exist.");
    }
    try {
      return Utils.parseProperties(configPath);
    } catch (IOException e) {
      throw new VeniceException(e);
    }
  }

  public static VeniceProperties loadVenicePropertiesFromFile(String basePath, String fileName)
      throws FileNotFoundException {
    return loadVenicePropertiesFromFile(Paths.get(basePath, fileName).toAbsolutePath().toString());
  }

  public static Optional<SSLFactory> getSSLFactory(VeniceConfigLoader configLoader) {
    try {
      if (isolatedIngestionServerSslEnabled(configLoader)) {
        // If SSL communication is enabled but SSL configs are missing, we should fail fast here.
        if (!sslEnabled(configLoader)) {
          throw new VeniceException(
              "Ingestion isolation SSL is enabled for communication, but SSL configs are missing.");
        }
        LOGGER.info("SSL is enabled, will create SSLFactory");
        SSLConfig sslConfig = SSLConfig.buildConfig(configLoader.getCombinedProperties().toProperties());
        return Optional.of(new DefaultSSLFactory(sslConfig));
      } else {
        LOGGER.warn("SSL is not enabled");
        return Optional.empty();
      }
    } catch (Exception e) {
      throw new VeniceException("Caught exception during SSLFactory creation", e);
    }
  }

  /**
   * Create SSLFactory for D2Client in ClientConfig, which will be used by different ingestion components.
   */
  public static Optional<SSLFactory> getSSLFactoryForIngestion(VeniceConfigLoader configLoader) {
    if (sslEnabled(configLoader)) {
      try {
        LOGGER.info("SSL is enabled, will create SSLFactory");
        return Optional.of(new DefaultSSLFactory(configLoader.getCombinedProperties().toProperties()));
      } catch (Exception e) {
        throw new VeniceException("Caught exception during SSLFactory creation", e);
      }
    } else {
      return Optional.empty();
    }
  }

  private static IdentityParser getIdentityParser(VeniceConfigLoader configLoader) {
    final String identityParserClassName =
        configLoader.getCombinedProperties().getString(IDENTITY_PARSER_CLASS, DefaultIdentityParser.class.getName());
    Class<IdentityParser> identityParserClass = ReflectUtils.loadClass(identityParserClassName);
    return ReflectUtils.callConstructor(identityParserClass, new Class[0], new Object[0]);
  }

  public static Optional<IsolatedIngestionServerAclHandler> getAclHandler(VeniceConfigLoader configLoader) {
    if (isolatedIngestionServerSslEnabled(configLoader) && isolatedIngestionServerAclEnabled(configLoader)) {
      String allowedPrincipalName =
          configLoader.getCombinedProperties().getString(SERVER_INGESTION_ISOLATION_PRINCIPAL_NAME);
      IdentityParser identityParser = getIdentityParser(configLoader);
      if (StringUtils.isEmpty(allowedPrincipalName)) {
        throw new VeniceException(
            "Ingestion isolation server SSL and ACL validation are enabled, but allowed principal name is missing in config.");
      }
      LOGGER.info(
          "Isolated ingestion server request ACL validation is enabled. Creating ACL handler with allowed principal name: {}",
          allowedPrincipalName);
      return Optional.of(new IsolatedIngestionServerAclHandler(identityParser, allowedPrincipalName));
    } else {
      return Optional.empty();
    }
  }

  public static boolean isolatedIngestionServerSslEnabled(VeniceConfigLoader configLoader) {
    return configLoader.getCombinedProperties().getBoolean(SERVER_INGESTION_ISOLATION_SSL_ENABLED, false);
  }

  public static boolean isolatedIngestionServerAclEnabled(VeniceConfigLoader configLoader) {
    return configLoader.getCombinedProperties().getBoolean(SERVER_INGESTION_ISOLATION_ACL_ENABLED, false);
  }

  public static boolean sslEnabled(VeniceConfigLoader configLoader) {
    return configLoader.getCombinedProperties().getBoolean(SSL_ENABLED, false);
  }

  private static void maybePopulateServerIngestionPrincipal(
      PropertyBuilder propertyBuilder,
      VeniceConfigLoader configLoader,
      IdentityParser identityParser) {
    if (!isolatedIngestionServerSslEnabled(configLoader)) {
      LOGGER.info("Skip populating service principal name since ingestion server SSL is not enabled.");
      return;
    }

    if (!isolatedIngestionServerAclEnabled(configLoader)) {
      LOGGER.info("Skip populating service principal name since ingestion server ACL validation is not enabled.");
      return;
    }

    // Extract principal name from the KeyStore file
    try (FileInputStream is =
        new FileInputStream(configLoader.getCombinedProperties().getString(SSL_KEYSTORE_LOCATION))) {
      KeyStore keystore = KeyStore.getInstance(configLoader.getCombinedProperties().getString(SSL_KEYSTORE_TYPE));
      keystore.load(is, configLoader.getCombinedProperties().getString(SSL_KEY_PASSWORD).toCharArray());
      String keyStoreAlias = keystore.aliases().nextElement();
      X509Certificate cert = SslUtils.getX509Certificate(keystore.getCertificate(keyStoreAlias));
      propertyBuilder.put(SERVER_INGESTION_ISOLATION_PRINCIPAL_NAME, identityParser.parseIdentityFromCert(cert));
    } catch (KeyStoreException | CertificateException | IOException | NoSuchAlgorithmException e) {
      throw new VeniceException(e);
    }
  }

  private static String storeVenicePropertiesToFile(
      String basePath,
      String fileName,
      VeniceProperties veniceProperties) {
    try {
      // Make sure the base path exists so we can store the config file in the path.
      Files.createDirectories(Paths.get(basePath));
    } catch (IOException e) {
      throw new VeniceException(e);
    }
    // Generate full config file path.
    String configFilePath = Paths.get(basePath, fileName).toAbsolutePath().toString();
    File configFile = new File(configFilePath);
    if (configFile.exists()) {
      LOGGER.warn("Config file already exists, will delete existing file: {}", configFilePath);
      if (!configFile.delete()) {
        throw new VeniceException("Unable to delete config file: " + configFilePath);
      }
    }
    // Store properties into config file.
    try {
      veniceProperties.storeFlattened(configFile);
      LOGGER.info("Configs are stored into: {}", configFilePath);
    } catch (IOException e) {
      throw new VeniceException(e);
    }
    return configFilePath;
  }
}
