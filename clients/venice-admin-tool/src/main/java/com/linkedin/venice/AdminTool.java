package com.linkedin.venice;

import static com.linkedin.venice.CommonConfigKeys.SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.VeniceConstants.DEFAULT_SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.schema.AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation;
import static com.linkedin.venice.serialization.avro.AvroProtocolDefinition.SERVER_ADMIN_RESPONSE;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.davinci.listener.response.ReplicaIngestionResponse;
import com.linkedin.venice.acl.VeniceComponent;
import com.linkedin.venice.admin.protocol.response.AdminResponseRecord;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.QueryTool;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controllerapi.AclResponse;
import com.linkedin.venice.controllerapi.AdminTopicMetadataResponse;
import com.linkedin.venice.controllerapi.ChildAwareResponse;
import com.linkedin.venice.controllerapi.ClusterStaleDataAuditResponse;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerClientFactory;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.MigrationPushStrategyResponse;
import com.linkedin.venice.controllerapi.MultiNodeResponse;
import com.linkedin.venice.controllerapi.MultiNodesStatusResponse;
import com.linkedin.venice.controllerapi.MultiReplicaResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.MultiStoragePersonaResponse;
import com.linkedin.venice.controllerapi.MultiStoreInfoResponse;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.controllerapi.MultiStoreStatusResponse;
import com.linkedin.venice.controllerapi.MultiStoreTopicsResponse;
import com.linkedin.venice.controllerapi.MultiVersionResponse;
import com.linkedin.venice.controllerapi.MultiVersionStatusResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.NodeReplicasReadinessResponse;
import com.linkedin.venice.controllerapi.NodeStatusResponse;
import com.linkedin.venice.controllerapi.OwnerResponse;
import com.linkedin.venice.controllerapi.PartitionResponse;
import com.linkedin.venice.controllerapi.PubSubTopicConfigResponse;
import com.linkedin.venice.controllerapi.ReadyForDataRecoveryResponse;
import com.linkedin.venice.controllerapi.RepushJobResponse;
import com.linkedin.venice.controllerapi.RoutersClusterConfigResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoragePersonaResponse;
import com.linkedin.venice.controllerapi.StoreComparisonResponse;
import com.linkedin.venice.controllerapi.StoreHealthAuditResponse;
import com.linkedin.venice.controllerapi.StoreMigrationResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.TrackableControllerResponse;
import com.linkedin.venice.controllerapi.UpdateClusterConfigQueryParams;
import com.linkedin.venice.controllerapi.UpdateStoragePersonaQueryParams;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.controllerapi.routes.AdminCommandExecutionResponse;
import com.linkedin.venice.d2.D2ClientFactory;
import com.linkedin.venice.datarecovery.DataRecoveryClient;
import com.linkedin.venice.datarecovery.EstimateDataRecoveryTimeCommand;
import com.linkedin.venice.datarecovery.MonitorCommand;
import com.linkedin.venice.datarecovery.StoreRepushCommand;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixSchemaAccessor;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.meta.ServerAdminAction;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.metadata.payload.StorePropertiesPayloadRecord;
import com.linkedin.venice.metadata.response.MetadataResponseRecord;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.manager.TopicManagerContext;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.avro.SchemaCompatibility;
import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.RetryUtils;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.BufferedReader;
import java.io.Console;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class AdminTool {
  private static final Logger LOGGER = LogManager.getLogger(AdminTool.class);
  private static ObjectWriter jsonWriter = ObjectMapperFactory.getInstance().writerWithDefaultPrettyPrinter();
  private static final String STATUS = "status";
  private static final String ERROR = "error";
  private static final String SUCCESS = "success";
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

  private static ControllerClient controllerClient;
  private static Optional<SSLFactory> sslFactory = Optional.empty();
  private static final Map<String, Map<String, ControllerClient>> clusterControllerClientPerColoMap = new HashMap<>();

  private static final List<String> REQUIRED_ZK_SSL_SYSTEM_PROPERTIES = Arrays.asList(
      "zookeeper.client.secure",
      "zookeeper.clientCnxnSocket",
      "zookeeper.ssl.keyStore.location",
      "zookeeper.ssl.keyStore.password",
      "zookeeper.ssl.keyStore.type",
      "zookeeper.ssl.trustStore.location",
      "zookeeper.ssl.trustStore.password",
      "zookeeper.ssl.trustStore.type");
  private static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd hh:mm:ss";
  private static final String PST_TIME_ZONE = "America/Los_Angeles";

  static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();

  public static void main(String[] args) throws Exception {
    // Generate PubSubClientsFactory from java system properties, apache kafka adapter is the default one.
    PubSubClientsFactory pubSubClientsFactory = new PubSubClientsFactory(new VeniceProperties(System.getProperties()));
    CommandLine cmd = getCommandLine(args);
    try {
      Command foundCommand = ensureOnlyOneCommand(cmd);

      // Variables used within the switch case need to be defined in advance
      String veniceUrl = null;
      String clusterName = null;
      String storeName;
      String versionString;
      String topicName;

      int version;
      MultiStoreResponse storeResponse;
      ControllerResponse response;

      if (cmd.hasOption(Arg.DISABLE_LOG.toString())) {
        LogConfigurator.disableLog();
      }

      /**
       * Initialize SSL config if provided.
       */
      buildSslFactory(cmd);

      boolean hasUrlArg = Arrays.asList(foundCommand.getRequiredArgs()).contains(Arg.URL);
      if (hasUrlArg) {
        veniceUrl = getRequiredArgument(cmd, Arg.URL);

        boolean hasRequiredClusterArg = Arrays.asList(foundCommand.getRequiredArgs()).contains(Arg.CLUSTER);
        boolean hasOptionalClusterArg = Arrays.asList(foundCommand.getOptionalArgs()).contains(Arg.CLUSTER);
        String optionalClusterArgValue = getOptionalArgument(cmd, Arg.CLUSTER);
        if (hasRequiredClusterArg) {
          clusterName = getRequiredArgument(cmd, Arg.CLUSTER);
        } else if (hasOptionalClusterArg && optionalClusterArgValue != null) {
          clusterName = optionalClusterArgValue;
        } else if (Arrays.asList(foundCommand.getRequiredArgs()).contains(Arg.STORE) && hasOptionalClusterArg) {
          storeName = getRequiredArgument(cmd, Arg.STORE, Command.DESCRIBE_STORE);
          D2ServiceDiscoveryResponse serviceDiscoveryResponse =
              ControllerClient.discoverCluster(veniceUrl, storeName, sslFactory, 1);
          if (serviceDiscoveryResponse.isError()) {
            throw new VeniceException(serviceDiscoveryResponse.getError());
          }
          clusterName = serviceDiscoveryResponse.getCluster();
          System.out.printf("Store %s is discovered in cluster %s%n", storeName, clusterName);
        }

        if (clusterName != null) {
          controllerClient = ControllerClientFactory.getControllerClient(clusterName, veniceUrl, sslFactory);
        }
      }

      if (cmd.hasOption(Arg.FLAT_JSON.toString())) {
        jsonWriter = ObjectMapperFactory.getInstance().writer();
      }

      switch (foundCommand) {
        case LIST_STORES:
          storeResponse = queryStoreList(cmd);
          printObject(storeResponse);
          break;
        case CLEAN_EXECUTION_IDS:
          String cluster = getRequiredArgument(cmd, Arg.CLUSTER);
          response = controllerClient.cleanExecutionIds(cluster);
          printObject(response);
          break;
        case DESCRIBE_STORE:
          storeName = getRequiredArgument(cmd, Arg.STORE, Command.DESCRIBE_STORE);
          for (String store: storeName.split(",")) {
            printStoreDescription(store);
          }
          break;
        case DESCRIBE_STORES:
          storeResponse = queryStoreList(cmd);
          for (String store: storeResponse.getStores()) {
            printStoreDescription(store);
          }
          break;
        case JOB_STATUS:
          storeName = getRequiredArgument(cmd, Arg.STORE, Command.JOB_STATUS);
          versionString = getOptionalArgument(cmd, Arg.VERSION, null);
          if (versionString == null) {
            StoreInfo storeInfo = controllerClient.getStore(storeName).getStore();
            version = storeInfo.getLargestUsedVersionNumber();
          } else {
            version = Integer.parseInt(versionString);
          }
          topicName = Version.composeKafkaTopic(storeName, version);
          JobStatusQueryResponse jobStatus = controllerClient.queryJobStatus(topicName);
          printObject(jobStatus);
          break;
        case KILL_JOB:
          storeName = getRequiredArgument(cmd, Arg.STORE, Command.KILL_JOB);
          versionString = getRequiredArgument(cmd, Arg.VERSION, Command.KILL_JOB);
          version = Integer.parseInt(versionString);
          topicName = Version.composeKafkaTopic(storeName, version);
          response = controllerClient.killOfflinePushJob(topicName);
          printObject(response);
          break;
        case SKIP_ADMIN:
          String offset = getRequiredArgument(cmd, Arg.OFFSET, Command.SKIP_ADMIN);
          boolean skipDIV = Boolean.parseBoolean(getOptionalArgument(cmd, Arg.SKIP_DIV, "false"));
          response = controllerClient.skipAdminMessage(offset, skipDIV);
          printObject(response);
          break;
        case NEW_STORE:
          createNewStore(cmd);
          break;
        case DELETE_STORE:
          deleteStore(cmd);
          break;
        case BACKFILL_SYSTEM_STORES:
          backfillSystemStores(cmd);
          break;
        case EMPTY_PUSH:
          emptyPush(cmd);
          break;
        case DISABLE_STORE_WRITE:
          setEnableStoreWrites(cmd, false);
          break;
        case ENABLE_STORE_WRITE:
          setEnableStoreWrites(cmd, true);
          break;
        case DISABLE_STORE_READ:
          setEnableStoreReads(cmd, false);
          break;
        case ENABLE_STORE_READ:
          setEnableStoreReads(cmd, true);
          break;
        case DISABLE_STORE:
          setEnableStoreReadWrites(cmd, false);
          break;
        case ENABLE_STORE:
          setEnableStoreReadWrites(cmd, true);
          break;
        case DELETE_ALL_VERSIONS:
          deleteAllVersions(cmd);
          break;
        case DELETE_OLD_VERSION:
          deleteOldVersion(cmd);
          break;
        case SET_VERSION:
          applyVersionToStore(cmd);
          break;
        case SET_OWNER:
          setStoreOwner(cmd);
          break;
        case GET_PARTITION_ID:
          getPartitionIdForKey(cmd);
          break;
        case SET_PARTITION_COUNT:
          setStorePartition(cmd);
          break;
        case UPDATE_STORE:
          updateStore(cmd);
          break;
        case UPDATE_CLUSTER_CONFIG:
          updateClusterConfig(cmd);
          break;
        case ADD_SCHEMA:
          applyValueSchemaToStore(cmd);
          break;
        case ADD_SCHEMA_TO_ZK:
          applyValueSchemaToZK(cmd);
          break;
        case ADD_DERIVED_SCHEMA:
          applyDerivedSchemaToStore(cmd);
          break;
        case REMOVE_DERIVED_SCHEMA:
          removeDerivedSchema(cmd);
          break;
        case LIST_STORAGE_NODES:
          printStorageNodeList();
          break;
        case CLUSTER_HEALTH_INSTANCES:
          printInstancesStatuses(cmd);
          break;
        case CLUSTER_HEALTH_STORES:
          printStoresStatuses();
          break;
        case NODE_REMOVABLE:
          isNodeRemovable(cmd);
          break;
        case REMOVE_NODE:
          removeNodeFromCluster(cmd);
          break;
        case ALLOW_LIST_ADD_NODE:
          addNodeIntoAllowList(cmd);
          break;
        case ALLOW_LIST_REMOVE_NODE:
          removeNodeFromAllowList(cmd);
          break;
        case REPLICAS_OF_STORE:
          printReplicaListForStoreVersion(cmd);
          break;
        case REPLICAS_ON_STORAGE_NODE:
          printReplicaListForStorageNode(cmd);
          break;
        case QUERY:
          queryStoreForKey(cmd, veniceUrl);
          break;
        case SHOW_SCHEMAS:
          showSchemas(cmd);
          break;
        case GET_EXECUTION:
          getExecution(cmd);
          break;
        case ENABLE_THROTTLING:
          enableThrottling(true);
          break;
        case DISABLE_THROTTLING:
          enableThrottling(false);
          break;
        case ENABLE_MAX_CAPACITY_PROTECTION:
          enableMaxCapacityProtection(true);
          break;
        case DISABLE_MAX_CAPACITY_PROTECTION:
          enableMaxCapacityProtection(false);
          break;
        case ENABLE_QUTOA_REBALANCE:
          enableQuotaRebalance(cmd, true);
          break;
        case DISABLE_QUTOA_REBALANCE:
          enableQuotaRebalance(cmd, false);
          break;
        case GET_ROUTERS_CLUSTER_CONFIG:
          getRoutersClusterConfig();
          break;
        case GET_ALL_MIGRATION_PUSH_STRATEGIES:
          getAllMigrationPushStrategies();
          break;
        case GET_MIGRATION_PUSH_STRATEGY:
          getMigrationPushStrategy(cmd);
          break;
        case SET_MIGRATION_PUSH_STRATEGY:
          setMigrationPushStrategy(cmd);
          break;
        case LIST_BOOTSTRAPPING_VERSIONS:
          listBootstrappingVersions(cmd);
          break;
        case DELETE_KAFKA_TOPIC:
          deleteKafkaTopic(cmd, pubSubClientsFactory);
          break;
        case DUMP_ADMIN_MESSAGES:
          dumpAdminMessages(cmd, pubSubClientsFactory);
          break;
        case DUMP_CONTROL_MESSAGES:
          dumpControlMessages(cmd, pubSubClientsFactory);
          break;
        case DUMP_KAFKA_TOPIC:
          dumpKafkaTopic(cmd, pubSubClientsFactory);
          break;
        case QUERY_KAFKA_TOPIC:
          queryKafkaTopic(cmd, pubSubClientsFactory);
          break;
        case AUTO_MIGRATE_STORE:
          autoMigrateStore(cmd);
          break;
        case MIGRATE_STORE:
          migrateStore(cmd);
          break;
        case MIGRATION_STATUS:
          checkMigrationStatus(cmd);
          break;
        case COMPLETE_MIGRATION:
          completeMigration(cmd);
          break;
        case ABORT_MIGRATION:
          abortMigration(cmd);
          break;
        case END_MIGRATION:
          endMigration(cmd);
          break;
        case SEND_END_OF_PUSH:
          sendEndOfPush(cmd);
          break;
        case NEW_STORE_ACL:
          createNewStoreWithAcl(cmd);
          break;
        case UPDATE_STORE_ACL:
          updateStoreWithAcl(cmd);
          break;
        case GET_STORE_ACL:
          getAclForStore(cmd);
          break;
        case DELETE_STORE_ACL:
          deleteAclForStore(cmd);
          break;
        case ADD_TO_STORE_ACL:
          addToStoreAcl(cmd);
          break;
        case REMOVE_FROM_STORE_ACL:
          removeFromStoreAcl(cmd);
          break;
        case ENABLE_ACTIVE_ACTIVE_REPLICATION_FOR_CLUSTER:
          enableActiveActiveReplicationForCluster(cmd);
          break;
        case DISABLE_ACTIVE_ACTIVE_REPLICATION_FOR_CLUSTER:
          disableActiveActiveReplicationForCluster(cmd);
          break;
        case GET_DELETABLE_STORE_TOPICS:
          getDeletableStoreTopics(cmd);
          break;
        case WIPE_CLUSTER:
          wipeCluster(cmd);
          break;
        case REPLICAS_READINESS_ON_STORAGE_NODE:
          printReplicasReadinessStorageNode(cmd);
          break;
        case LIST_CLUSTER_STALE_STORES:
          listClusterStaleStores(cmd);
          break;
        case REPUSH_STORE:
          repushStore(cmd);
          break;
        case GET_DEAD_STORES:
          getDeadStores(cmd);
          break;
        case COMPARE_STORE:
          compareStore(cmd);
          break;
        case REPLICATE_META_DATA:
          copyOverStoresMetadata(cmd);
          break;
        case LIST_STORE_PUSH_INFO:
          listStorePushInfo(cmd);
          break;
        case GET_KAFKA_TOPIC_CONFIGS:
          getKafkaTopicConfigs(cmd);
          break;
        case UPDATE_KAFKA_TOPIC_LOG_COMPACTION:
          updateKafkaTopicLogCompaction(cmd);
          break;
        case UPDATE_KAFKA_TOPIC_RETENTION:
          updateKafkaTopicRetention(cmd);
          break;
        case UPDATE_KAFKA_TOPIC_MIN_IN_SYNC_REPLICA:
          updateKafkaTopicMinInSyncReplica(cmd);
          break;
        case START_FABRIC_BUILDOUT:
          startFabricBuildout(cmd);
          break;
        case CHECK_FABRIC_BUILDOUT_STATUS:
          checkFabricBuildoutStatus(cmd);
          break;
        case END_FABRIC_BUILDOUT:
          endFabricBuildout(cmd);
          break;
        case NEW_STORAGE_PERSONA:
          createNewStoragePersona(cmd);
          break;
        case GET_STORAGE_PERSONA:
          getStoragePersona(cmd);
          break;
        case DELETE_STORAGE_PERSONA:
          deleteStoragePersona(cmd);
          break;
        case UPDATE_STORAGE_PERSONA:
          updateStoragePersona(cmd);
          break;
        case GET_STORAGE_PERSONA_FOR_STORE:
          getStoragePersonaForStore(cmd);
          break;
        case LIST_CLUSTER_STORAGE_PERSONAS:
          listClusterStoragePersonas(cmd);
          break;
        case CLEANUP_INSTANCE_CUSTOMIZED_STATES:
          cleanupInstanceCustomizedStates(cmd);
          break;
        case EXECUTE_DATA_RECOVERY:
          executeDataRecovery(cmd);
          break;
        case ESTIMATE_DATA_RECOVERY_TIME:
          estimateDataRecoveryTime(cmd);
          break;
        case MONITOR_DATA_RECOVERY:
          monitorDataRecovery(cmd);
          break;
        case REQUEST_BASED_METADATA:
          getRequestBasedMetadata(cmd);
          break;
        case REQUEST_BASED_STORE_PROPERTIES:
          getRequestBasedStoreProperties(cmd);
          break;
        case DUMP_INGESTION_STATE:
          dumpIngestionState(cmd);
          break;
        case CONFIGURE_STORE_VIEW:
          configureStoreView(cmd);
          break;
        case BACKUP_STORE_METADATA_FROM_GRAVEYARD:
          backupStoreMetadataFromGraveyard(cmd);
          break;
        case RECOVER_STORE_METADATA:
          recoverStoreMetadata(cmd, pubSubClientsFactory);
          break;
        case DUMP_TOPIC_PARTITION_INGESTION_CONTEXT:
          dumpTopicPartitionIngestionContext(cmd);
          break;
        case MIGRATE_VENICE_ZK_PATHS:
          migrateVeniceZKPaths(cmd);
          break;
        case EXTRACT_VENICE_ZK_PATHS:
          extractVeniceZKPaths(cmd);
          break;
        case AGGREGATED_HEALTH_STATUS:
          getAggregatedHealthStatus(cmd);
          break;
        case DUMP_HOST_HEARTBEAT:
          dumpHostHeartbeat(cmd);
          break;
        case CLUSTER_BATCH_TASK:
          clusterBatchTask(cmd);
          break;
        case UPDATE_ADMIN_OPERATION_PROTOCOL_VERSION:
          updateAdminOperationProtocolVersion(cmd);
          break;
        default:
          StringJoiner availableCommands = new StringJoiner(", ");
          for (Command c: Command.values()) {
            availableCommands.add("--" + c.toString());
          }
          throw new VeniceException("Must supply one of the following commands: " + availableCommands.toString());
      }
      clusterControllerClientPerColoMap.forEach((key, map) -> map.values().forEach(Utils::closeQuietlyWithErrorLogged));
    } catch (Exception e) {
      printErrAndThrow(e, e.getMessage(), null);
    }
  }

  public static CommandLine getCommandLine(String[] args) throws ParseException, IOException {
    /**
     * Command Options are split up for help text formatting, see printUsageAndExit()
     *
     * Gather all the commands we have in "commandGroup"
     **/
    OptionGroup commandGroup = new OptionGroup();
    for (Command c: Command.values()) {
      createCommandOpt(c, commandGroup);
    }

    /**
     * Gather all the options we have in "options"
     */
    Options options = new Options();
    for (Arg arg: Arg.values()) {
      createOpt(arg, arg.isParameterized(), arg.getHelpText(), options);
    }

    Options parameterOptionsForHelp = new Options();
    for (Object obj: options.getOptions()) {
      Option o = (Option) obj;
      parameterOptionsForHelp.addOption(o);
    }

    options.addOptionGroup(commandGroup);

    CommandLineParser parser = new DefaultParser(false);
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption(Arg.HELP.first())) {
      printUsageAndExit(commandGroup, parameterOptionsForHelp, cmd);
    } else if (cmd.hasOption(Command.CONVERT_VSON_SCHEMA.toString())) {
      convertVsonSchemaAndExit(cmd);
    }

    // SSl config path is mandatory
    if (!cmd.hasOption(Arg.SSL_CONFIG_PATH.first())) {
      /**
       * Don't throw exception yet until all controllers are deployed with SSL support and the script
       * that automatically generates SSL config file is provided.
       */
      System.out.println("[WARN] Running admin tool without SSL.");
    }
    return cmd;
  }

  private static Command ensureOnlyOneCommand(CommandLine cmd) {
    String foundCommand = null;
    for (Command c: Command.values()) {
      if (cmd.hasOption(c.toString())) {
        if (foundCommand == null) {
          foundCommand = c.toString();
        } else {
          throw new VeniceException("Can only specify one of --" + foundCommand + " and --" + c);
        }
      }
    }
    return Command.getCommand(foundCommand, cmd);
  }

  private static void buildSslFactory(CommandLine cmd) throws IOException {
    if (cmd.hasOption(Arg.SSL_CONFIG_PATH.first())) {
      String sslConfigPath = getOptionalArgument(cmd, Arg.SSL_CONFIG_PATH);
      Properties sslProperties = SslUtils.loadSSLConfig(sslConfigPath);
      String sslFactoryClassName = sslProperties.getProperty(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME);
      sslFactory = Optional.of(SslUtils.getSSLFactory(sslProperties, sslFactoryClassName));
    }
  }

  private static MultiStoreResponse queryStoreList(CommandLine cmd) {
    boolean includeSystemStores = Boolean.parseBoolean(getOptionalArgument(cmd, Arg.INCLUDE_SYSTEM_STORES));
    Optional<String> configNameFilter = Optional.ofNullable(getOptionalArgument(cmd, Arg.STORE_CONFIG_NAME_FILTER));
    Optional<String> configValueFilter = Optional.ofNullable(getOptionalArgument(cmd, Arg.STORE_CONFIG_VALUE_FILTER));
    return controllerClient.queryStoreList(includeSystemStores, configNameFilter, configValueFilter);
  }

  private static void queryStoreForKey(CommandLine cmd, String veniceUrl) throws Exception {
    String store = getRequiredArgument(cmd, Arg.STORE);
    String keyString = getRequiredArgument(cmd, Arg.KEY);
    String sslConfigFileStr = getOptionalArgument(cmd, Arg.VENICE_CLIENT_SSL_CONFIG_FILE);
    boolean isVsonStore = Boolean.parseBoolean(getOptionalArgument(cmd, Arg.VSON_STORE, "false"));
    Optional<String> sslConfigFile =
        StringUtils.isEmpty(sslConfigFileStr) ? Optional.empty() : Optional.of(sslConfigFileStr);
    printObject(QueryTool.queryStoreForKey(store, keyString, veniceUrl, isVsonStore, sslConfigFile));
  }

  private static void showSchemas(CommandLine cmd) {
    String store = getRequiredArgument(cmd, Arg.STORE);
    SchemaResponse keySchema = controllerClient.getKeySchema(store);
    printObject(keySchema);
    MultiSchemaResponse valueSchemas = controllerClient.getAllValueSchema(store);
    printObject(valueSchemas);
  }

  private static void executeDataRecovery(CommandLine cmd) {
    String recoveryCommand = getRequiredArgument(cmd, Arg.RECOVERY_COMMAND);
    String destFabric = getRequiredArgument(cmd, Arg.DEST_FABRIC);
    String sourceFabric = getRequiredArgument(cmd, Arg.SOURCE_FABRIC);
    String timestamp = getRequiredArgument(cmd, Arg.DATETIME);
    String parentUrl = getRequiredArgument(cmd, Arg.URL);

    String extraCommandArgs = getOptionalArgument(cmd, Arg.EXTRA_COMMAND_ARGS);
    String stores = getOptionalArgument(cmd, Arg.STORES);
    String cluster = getOptionalArgument(cmd, Arg.CLUSTER);
    boolean isDebuggingEnabled = cmd.hasOption(Arg.DEBUG.toString());
    boolean isNonInteractive = cmd.hasOption(Arg.NON_INTERACTIVE.toString());

    StoreRepushCommand.Params.Builder builder = new StoreRepushCommand.Params.Builder().setCommand(recoveryCommand)
        .setDestFabric(destFabric)
        .setSourceFabric(sourceFabric)
        .setTimestamp(LocalDateTime.parse(timestamp, DateTimeFormatter.ISO_LOCAL_DATE_TIME))
        .setSSLFactory(sslFactory)
        .setUrl(parentUrl);
    if (extraCommandArgs != null) {
      builder.setExtraCommandArgs(extraCommandArgs);
    }
    builder.setDebug(isDebuggingEnabled);

    Set<String> storeSet = calculateRecoveryStoreNames(stores, cluster, parentUrl);
    DataRecoveryClient dataRecoveryClient = new DataRecoveryClient();
    DataRecoveryClient.DataRecoveryParams params = new DataRecoveryClient.DataRecoveryParams(storeSet);
    params.setNonInteractive(isNonInteractive);

    try (ControllerClient cli = new ControllerClient("*", parentUrl, sslFactory)) {
      builder.setPCtrlCliWithoutCluster(cli);
      StoreRepushCommand.Params cmdParams = builder.build();
      dataRecoveryClient.execute(params, cmdParams);
    }
  }

  private static void estimateDataRecoveryTime(CommandLine cmd) {
    String destFabric = getRequiredArgument(cmd, Arg.DEST_FABRIC);
    String parentUrl = getRequiredArgument(cmd, Arg.URL);

    String stores = getOptionalArgument(cmd, Arg.STORES);
    String cluster = getOptionalArgument(cmd, Arg.CLUSTER);

    Set<String> storeSet = calculateRecoveryStoreNames(stores, cluster, parentUrl);
    DataRecoveryClient dataRecoveryClient = new DataRecoveryClient();
    DataRecoveryClient.DataRecoveryParams params = new DataRecoveryClient.DataRecoveryParams(storeSet);

    Long total;

    try (ControllerClient cli = new ControllerClient("*", parentUrl, sslFactory)) {
      EstimateDataRecoveryTimeCommand.Params.Builder builder =
          new EstimateDataRecoveryTimeCommand.Params.Builder(destFabric, cli, parentUrl, sslFactory);
      EstimateDataRecoveryTimeCommand.Params cmdParams = builder.build();
      total = dataRecoveryClient.estimateRecoveryTime(params, cmdParams);
    }

    if (total <= 0) {
      printObject("00:00:00");
    } else {
      int hours = (int) (total / 3600);
      int minutes = (int) ((total % 3600) / 60);
      int seconds = (int) (total % 60);

      printObject(String.format("TOTAL RECOVERY TIME FOR ALL STORES = %02d:%02d:%02d", hours, minutes, seconds));
    }
  }

  private static void monitorDataRecovery(CommandLine cmd) {
    String parentUrl = getRequiredArgument(cmd, Arg.URL);
    String destFabric = getRequiredArgument(cmd, Arg.DEST_FABRIC);
    String dateTimeStr = getRequiredArgument(cmd, Arg.DATETIME);

    String intervalStr = getOptionalArgument(cmd, Arg.INTERVAL);
    String stores = getOptionalArgument(cmd, Arg.STORES);
    String cluster = getOptionalArgument(cmd, Arg.CLUSTER);

    MonitorCommand.Params.Builder builder = new MonitorCommand.Params.Builder().setTargetRegion(destFabric)
        .setParentUrl(parentUrl)
        .setSSLFactory(sslFactory);

    try {
      builder.setDateTime(LocalDateTime.parse(dateTimeStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    } catch (DateTimeParseException e) {
      throw new VeniceException(
          String.format(
              "Can not parse: %s, supported format: %s",
              e.getParsedString(),
              DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    }

    Set<String> storeSet = calculateRecoveryStoreNames(stores, cluster, parentUrl);
    DataRecoveryClient dataRecoveryClient = new DataRecoveryClient();
    DataRecoveryClient.DataRecoveryParams params = new DataRecoveryClient.DataRecoveryParams(storeSet);
    if (intervalStr != null) {
      params.setInterval(Integer.parseInt(intervalStr));
    }

    try (ControllerClient parentCtrlCli = new ControllerClient("*", parentUrl, sslFactory)) {
      builder.setPCtrlCliWithoutCluster(parentCtrlCli);
      MonitorCommand.Params monitorParams = builder.build();
      dataRecoveryClient.monitor(params, monitorParams);
    }
  }

  private static Set<String> calculateRecoveryStoreNames(String multiStores, String cluster, String parentUrl) {
    if (!StringUtils.isEmpty(multiStores) && !StringUtils.isEmpty(cluster)) {
      System.out.println(
          String.format("[WARN] --%s overwrites --%s value.", Arg.STORES.getArgName(), Arg.CLUSTER.getArgName()));
    }

    if (!StringUtils.isEmpty(multiStores)) {
      return Utils.parseCommaSeparatedStringToSet(multiStores);
    }

    if (!StringUtils.isEmpty(cluster)) {
      try (ControllerClient parentCtrlCli = new ControllerClient(cluster, parentUrl, sslFactory)) {
        MultiStoreResponse response = parentCtrlCli.queryStoreList(false);
        if (response.isError()) {
          throw new VeniceClientException("Error in getting store list for " + cluster + ": " + response.getError());
        }
        if (response.getStores() == null) {
          return null;
        }
        return new HashSet<>(Arrays.asList(response.getStores()));
      }
    }
    return null;
  }

  private static void createNewStore(CommandLine cmd) throws Exception {
    String store = getRequiredArgument(cmd, Arg.STORE, Command.NEW_STORE);
    String keySchemaFile = getRequiredArgument(cmd, Arg.KEY_SCHEMA, Command.NEW_STORE);
    String keySchema = readFile(keySchemaFile);
    String valueSchemaFile = getRequiredArgument(cmd, Arg.VALUE_SCHEMA, Command.NEW_STORE);
    String valueSchema = readFile(valueSchemaFile);
    String owner = getOptionalArgument(cmd, Arg.OWNER, "");
    boolean isVsonStore = Utils.parseBooleanOrThrow(getOptionalArgument(cmd, Arg.VSON_STORE, "false"), "isVsonStore");
    if (isVsonStore) {
      keySchema = VsonAvroSchemaAdapter.parse(keySchema).toString();
      valueSchema = VsonAvroSchemaAdapter.parse(valueSchema).toString();
    }
    verifyValidSchema(keySchema);
    verifyValidSchema(valueSchema);
    verifyStoreExistence(store, false);
    NewStoreResponse response = controllerClient.createNewStore(store, owner, keySchema, valueSchema);
    printObject(response);
  }

  private static void deleteStore(CommandLine cmd) throws IOException {
    String store = getRequiredArgument(cmd, Arg.STORE, Command.DELETE_STORE);
    verifyStoreExistence(store, true);
    TrackableControllerResponse response = controllerClient.deleteStore(store);
    printObject(response);
  }

  private static void clusterBatchTask(CommandLine cmd) {
    String clusterName = getRequiredArgument(cmd, Arg.CLUSTER, Command.CLUSTER_BATCH_TASK);
    String task = getRequiredArgument(cmd, Arg.TASK_NAME, Command.CLUSTER_BATCH_TASK);
    String checkpointFile = getRequiredArgument(cmd, Arg.CHECKPOINT_FILE, Command.CLUSTER_BATCH_TASK);
    int parallelism = Integer.parseInt(getOptionalArgument(cmd, Arg.THREAD_COUNT, "1"));
    String storeFilterFile = getOptionalArgument(cmd, Arg.STORE_FILTER_FILE, "");
    int kafkaTopicMinISR = Integer.parseInt(getOptionalArgument(cmd, Arg.KAFKA_TOPIC_MIN_IN_SYNC_REPLICA));
    int kafkaTopicRtMinISR = Integer.parseInt(getOptionalArgument(cmd, Arg.KAFKA_RT_TOPICS_MIN_IN_SYNC_REPLICAS));
    Set<String> interestedStoresSet = new HashSet<>();
    LOGGER.info(
        "[**** Cluster Command Params ****] Cluster: {}, Task: {}, Checkpoint: {}, Parallelism: {}, Store filter: {}",
        clusterName,
        task,
        checkpointFile,
        parallelism,
        storeFilterFile);
    // Create child data center controller client map.
    ChildAwareResponse childAwareResponse = controllerClient.listChildControllers(clusterName);
    Map<String, ControllerClient> controllerClientMap = getControllerClientMap(clusterName, childAwareResponse);

    // Load store filter file.
    if (!storeFilterFile.isEmpty()) {
      try {
        Path storeFilterFilePath = Paths.get(storeFilterFile);
        if (!Files.exists(storeFilterFilePath.toAbsolutePath())) {
          throw new VeniceException("Invalid store filter file path");
        } else {
          List<String> fileLines = Files.readAllLines(storeFilterFilePath);
          interestedStoresSet.addAll(fileLines);
        }
      } catch (IOException e) {
        throw new VeniceException(e);
      }
    }

    // Fetch list cluster store list from parent region.
    Map<String, Boolean> progressMap = new VeniceConcurrentHashMap<>();
    MultiStoreResponse clusterStoreResponse = controllerClient.queryStoreList(false);
    if (clusterStoreResponse.isError()) {
      throw new VeniceException("Unable to fetch cluster store list: " + clusterStoreResponse.getError());
    }
    for (String storeName: clusterStoreResponse.getStores()) {
      if (storeFilterFile.isEmpty()) {
        progressMap.put(storeName, Boolean.FALSE);
      } else {
        // For now the default behavior is to only perform cluster operation on the intersection of the store filter
        // file and stores in the cluster.
        if (interestedStoresSet.contains(storeName)) {
          progressMap.put(storeName, Boolean.FALSE);
        }
      }
    }

    // Load progress from checkpoint file. If file does not exist, it will create new one during checkpointing.
    try {
      Path checkpointFilePath = Paths.get(checkpointFile);
      if (!Files.exists(checkpointFilePath.toAbsolutePath())) {
        LOGGER.info(
            "Checkpoint file path does not exist, will create a new checkpoint file: {}",
            checkpointFilePath.toAbsolutePath());
      } else {
        List<String> fileLines = Files.readAllLines(checkpointFilePath);
        for (String line: fileLines) {
          String storeName = line.split(",")[0];
          if (!progressMap.containsKey(storeName)) {
            // The store is either filtered out or does not belong to this cluster.
            continue;
          }
          // For now, it is boolean to start with, we can add more states to support retry.
          boolean status = false;
          if (line.split(",").length > 1) {
            status = Boolean.parseBoolean(line.split(",")[1]);
          }
          progressMap.put(storeName, status);
        }
      }
    } catch (IOException e) {
      throw new VeniceException(e);
    }
    List<String> taskList =
        progressMap.entrySet().stream().filter(e -> !e.getValue()).map(Map.Entry::getKey).collect(Collectors.toList());

    // Validate task type. For now, we only has one task, if we have more task in the future, we can extend this logic.
    Supplier<Function<String, Boolean>> functionSupplier = null;
    if (SystemStorePushTask.TASK_NAME.equals(task)) {
      String systemStoreType = getOptionalArgument(cmd, Arg.SYSTEM_STORE_TYPE);
      if (systemStoreType != null) {
        if (!(systemStoreType.equalsIgnoreCase(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.toString())
            || systemStoreType.equalsIgnoreCase(VeniceSystemStoreType.META_STORE.toString()))) {
          printErrAndExit("System store type: " + systemStoreType + " is not supported.");
        }
      }
      System.out.println(
          functionSupplier = () -> new SystemStorePushTask(
              controllerClient,
              controllerClientMap,
              clusterName,
              systemStoreType == null ? Optional.empty() : Optional.of(systemStoreType)));
    } else if (BackfillMinIsrTask.TASK_NAME.equals(task)) {
      System.out.println(
          functionSupplier = () -> new BackfillMinIsrTask(controllerClientMap, kafkaTopicMinISR, kafkaTopicRtMinISR));
    } else {
      printErrAndExit("Undefined task: " + task);
    }

    // Create thread pool and start parallel processing.
    ExecutorService executorService = Executors.newFixedThreadPool(parallelism);
    List<Future> futureList = new ArrayList<>();
    for (int i = 0; i < parallelism; i++) {
      BatchMaintenanceTaskRunner batchMaintenanceTaskRunner =
          new BatchMaintenanceTaskRunner(progressMap, checkpointFile, taskList, functionSupplier.get());
      futureList.add(executorService.submit(batchMaintenanceTaskRunner));
    }
    for (int i = 0; i < parallelism; i++) {
      try {
        futureList.get(i).get();
        LOGGER.info("Cluster task completed for thread : {}", i);
      } catch (InterruptedException | ExecutionException e) {
        LOGGER.warn(e.getMessage());
        executorService.shutdownNow();
      }
    }
    executorService.shutdownNow();
  }

  private static void backfillSystemStores(CommandLine cmd) {
    String clusterName = getRequiredArgument(cmd, Arg.CLUSTER, Command.BACKFILL_SYSTEM_STORES);
    String systemStoreType = getRequiredArgument(cmd, Arg.SYSTEM_STORE_TYPE, Command.BACKFILL_SYSTEM_STORES);
    if (!(systemStoreType.equalsIgnoreCase(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.toString())
        || systemStoreType.equalsIgnoreCase(VeniceSystemStoreType.META_STORE.toString()))) {
      printErrAndExit("System store type: " + systemStoreType + " is not supported.");
    }
    String[] stores = controllerClient.queryStoreList(false).getStores();
    List<String> failedToGetStoreInfoStoreList = new ArrayList<>();
    List<String> alreadyHaveSystemStoreList = new ArrayList<>();
    List<String> emptyPushFailedStoreList = new ArrayList<>();
    List<String> writeDisabledStores = new ArrayList<>();
    // store size param is used to determine how many partitions to create; however, for system store
    // it is determined by shared ZK based store. Hence, it is not really relevant.
    long storeSize = 32 * 1024 * 1024;
    System.out.println(
        "Cluster: " + clusterName + " has " + stores.length + " stores in it. Running" + systemStoreType
            + " backfill task...");
    for (String storeName: stores) {
      try {
        StoreResponse storeResponse = controllerClient.getStore(storeName);
        if (storeResponse == null || storeResponse.isError()) {
          failedToGetStoreInfoStoreList.add(storeName);
          continue;
        }
        StoreInfo storeInfo = storeResponse.getStore();
        if (hasGivenSystemStore(storeInfo, systemStoreType)) {
          alreadyHaveSystemStoreList.add(storeName);
          continue;
        }
        if (!storeInfo.isEnableStoreWrites()) {
          writeDisabledStores.add(storeName);
          continue;
        }
        String systemStoreName = getSystemStoreName(storeInfo, systemStoreType);
        String pushId = "BACKFILL_" + systemStoreName;
        System.out.println("Running empty push for: " + systemStoreName);
        if (!executeEmptyPush(systemStoreName, pushId, storeSize)) {
          emptyPushFailedStoreList.add(storeName);
          System.err.println("Empty push failed for: " + systemStoreName);
        }
      } catch (Exception e) {
        emptyPushFailedStoreList.add(storeName);
      }
    }
    System.out.println("Finished backfill task.\nStats - ");
    System.out.println("Failed to get store details for the stores: " + failedToGetStoreInfoStoreList);
    System.out.println("Skipping stores that already have system store of a given type: " + alreadyHaveSystemStoreList);
    System.out.println("Skipping stores that have disabled writes: " + writeDisabledStores);
    System.out.println("Empty push failed for stores: " + emptyPushFailedStoreList);
  }

  private static boolean hasGivenSystemStore(StoreInfo storeInfo, String systemStoreType) {
    if (systemStoreType.equalsIgnoreCase(VeniceSystemStoreType.META_STORE.toString())) {
      return storeInfo.isStoreMetaSystemStoreEnabled();
    }
    if (systemStoreType.equalsIgnoreCase(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.toString())) {
      return storeInfo.isDaVinciPushStatusStoreEnabled();
    }
    throw new IllegalArgumentException(systemStoreType + " is not a valid system store type.");
  }

  private static String getSystemStoreName(StoreInfo storeInfo, String systemStoreType) {
    if (systemStoreType.equalsIgnoreCase(VeniceSystemStoreType.META_STORE.toString())) {
      return VeniceSystemStoreUtils.getMetaStoreName(storeInfo.getName());
    }
    if (systemStoreType.equalsIgnoreCase(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.toString())) {
      return VeniceSystemStoreUtils.getDaVinciPushStatusStoreName(storeInfo.getName());
    }
    throw new IllegalArgumentException(systemStoreType + " is not a valid system store type.");
  }

  private static void emptyPush(CommandLine cmd) {
    String store = getRequiredArgument(cmd, Arg.STORE, Command.EMPTY_PUSH);
    String pushId = getRequiredArgument(cmd, Arg.PUSH_ID, Command.EMPTY_PUSH);
    String storeSizeString = getRequiredArgument(cmd, Arg.STORE_SIZE, Command.EMPTY_PUSH);
    long storeSize = Utils.parseLongFromString(storeSizeString, Arg.STORE_SIZE.name());
    executeEmptyPush(store, pushId, storeSize);
  }

  // returns false on error otherwise true
  private static boolean executeEmptyPush(String store, String pushId, long storeSize) {
    verifyStoreExistence(store, true);
    VersionCreationResponse versionCreationResponse = controllerClient.emptyPush(store, pushId, storeSize);
    printObject(versionCreationResponse);
    if (versionCreationResponse.isError()) {
      return false;
    }
    // Kafka topic name in the above response is null, and it will be fixed with this code change.
    String topicName = Version.composeKafkaTopic(store, versionCreationResponse.getVersion());
    // Polling job status to make sure the empty push hits every child colo
    while (true) {
      JobStatusQueryResponse jobStatusQueryResponse =
          controllerClient.retryableRequest(3, controllerClient -> controllerClient.queryJobStatus(topicName));
      printObject(jobStatusQueryResponse);
      if (jobStatusQueryResponse.isError()) {
        return false;
      }
      ExecutionStatus executionStatus = ExecutionStatus.valueOf(jobStatusQueryResponse.getStatus());
      if (executionStatus.isTerminal()) {
        break;
      }
      Utils.sleep(TimeUnit.SECONDS.toMillis(5));
    }
    return true;
  }

  private static void setEnableStoreWrites(CommandLine cmd, boolean enableWrites) {
    String store =
        getRequiredArgument(cmd, Arg.STORE, enableWrites ? Command.ENABLE_STORE_WRITE : Command.DISABLE_STORE_WRITE);
    ControllerResponse response = controllerClient.enableStoreWrites(store, enableWrites);
    printSuccess(response);
  }

  private static void setEnableStoreReads(CommandLine cmd, boolean enableReads) {
    String store =
        getRequiredArgument(cmd, Arg.STORE, enableReads ? Command.ENABLE_STORE_READ : Command.DISABLE_STORE_READ);
    ControllerResponse response = controllerClient.enableStoreReads(store, enableReads);
    printSuccess(response);
  }

  private static void setEnableStoreReadWrites(CommandLine cmd, boolean enableReadWrites) {
    String store = getRequiredArgument(cmd, Arg.STORE, enableReadWrites ? Command.ENABLE_STORE : Command.DISABLE_STORE);
    ControllerResponse response = controllerClient.enableStoreReadWrites(store, enableReadWrites);
    printSuccess(response);
  }

  private static void applyVersionToStore(CommandLine cmd) {
    String storeName = getRequiredArgument(cmd, Arg.STORE, Command.SET_VERSION);
    String version = getRequiredArgument(cmd, Arg.VERSION, Command.SET_VERSION);
    int intVersion = Utils.parseIntFromString(version, Arg.VERSION.name());
    boolean versionExists = false;
    StoreResponse storeResponse = controllerClient.getStore(storeName);
    if (storeResponse.isError()) {
      throw new VeniceException("Error querying versions for store: " + storeName + " -- " + storeResponse.getError());
    }
    int[] versionNumbers = storeResponse.getStore().getVersions().stream().mapToInt(v -> v.getNumber()).toArray();
    for (int v: versionNumbers) {
      if (v == intVersion) {
        versionExists = true;
        break;
      }
    }
    if (!versionExists) {
      throw new VeniceException(
          "Version " + version + " does not exist for store " + storeName + ".  Store only has versions: "
              + Arrays.toString(versionNumbers));
    }
    VersionResponse response = controllerClient.overrideSetActiveVersion(storeName, intVersion);
    printSuccess(response);
  }

  private static void setStoreOwner(CommandLine cmd) {
    String storeName = getRequiredArgument(cmd, Arg.STORE, Command.SET_OWNER);
    String owner = getRequiredArgument(cmd, Arg.OWNER, Command.SET_OWNER);
    OwnerResponse response = controllerClient.setStoreOwner(storeName, owner);
    printSuccess(response);
  }

  private static void setStorePartition(CommandLine cmd) {
    String storeName = getRequiredArgument(cmd, Arg.STORE, Command.SET_PARTITION_COUNT);
    String partitionNum = getRequiredArgument(cmd, Arg.PARTITION_COUNT, Command.SET_PARTITION_COUNT);
    PartitionResponse response = controllerClient.setStorePartitionCount(storeName, partitionNum);
    printSuccess(response);
  }

  private static void getPartitionIdForKey(CommandLine cmd) {
    String storeName = getRequiredArgument(cmd, Arg.STORE, Command.GET_PARTITION_ID);
    String key = getRequiredArgument(cmd, Arg.KEY, Command.GET_PARTITION_ID);
    int version = Integer.parseInt(getOptionalArgument(cmd, Arg.VERSION, "-1"));
    String keySchemaStr = controllerClient.getKeySchema(storeName).getSchemaStr();
    TopicMessageFinder.findPartitionIdForKey(controllerClient, storeName, version, key, keySchemaStr);
  }

  private static void integerParam(CommandLine cmd, Arg param, Consumer<Integer> setter, Set<Arg> argSet) {
    genericParam(cmd, param, s -> Utils.parseIntFromString(s, param.toString()), setter, argSet);
  }

  private static void longParam(CommandLine cmd, Arg param, Consumer<Long> setter, Set<Arg> argSet) {
    genericParam(cmd, param, s -> Utils.parseLongFromString(s, param.toString()), setter, argSet);
  }

  private static void booleanParam(CommandLine cmd, Arg param, Consumer<Boolean> setter, Set<Arg> argSet) {
    genericParam(cmd, param, s -> Utils.parseBooleanOrThrow(s, param.toString()), setter, argSet);
  }

  private static void stringMapParam(
      CommandLine cmd,
      Arg param,
      Consumer<Map<String, String>> setter,
      Set<Arg> argSet) {
    genericParam(cmd, param, s -> Utils.parseJsonMapFromString(s, param.toString()), setter, argSet);
  }

  private static void stringSetParam(CommandLine cmd, Arg param, Consumer<Set<String>> setter, Set<Arg> argSet) {
    genericParam(cmd, param, s -> Utils.parseCommaSeparatedStringToSet(s), setter, argSet);
  }

  private static <TYPE> void genericParam(
      CommandLine cmd,
      Arg param,
      Function<String, TYPE> parser,
      Consumer<TYPE> setter,
      Set<Arg> argSet) {
    if (!argSet.contains(param)) {
      throw new VeniceException(" Argument does not exist in command doc: " + param);
    }
    String paramStr = getOptionalArgument(cmd, param);
    if (paramStr != null) {
      setter.accept(parser.apply(paramStr));
    }
  }

  private static void updateStore(CommandLine cmd) {
    UpdateStoreQueryParams params = getUpdateStoreQueryParams(cmd);
    String storeName = getRequiredArgument(cmd, Arg.STORE, Command.UPDATE_STORE);
    ControllerResponse response = controllerClient.updateStore(storeName, params);
    printSuccess(response);
  }

  private static void updateClusterConfig(CommandLine cmd) {
    UpdateClusterConfigQueryParams params = getUpdateClusterConfigQueryParams(cmd);

    ControllerResponse response = controllerClient.updateClusterConfig(params);
    printSuccess(response);
  }

  static UpdateStoreQueryParams getConfigureStoreViewQueryParams(CommandLine cmd) {
    Set<Arg> argSet = new HashSet<>(Arrays.asList(Command.CONFIGURE_STORE_VIEW.getOptionalArgs()));
    argSet.addAll(new HashSet<>(Arrays.asList(Command.CONFIGURE_STORE_VIEW.getRequiredArgs())));
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    params.setViewName(getRequiredArgument(cmd, Arg.VIEW_NAME));
    if (cmd.hasOption(Arg.REMOVE_VIEW.toString())) {
      params.setDisableStoreView();
    } else {
      // If configuring a view, view class name is required.
      params.setViewClassName(getRequiredArgument(cmd, Arg.VIEW_CLASS));
    }
    stringMapParam(cmd, Arg.VIEW_PARAMS, p -> params.setViewClassParams(p), argSet);
    return params;
  }

  static UpdateStoreQueryParams getUpdateStoreQueryParams(CommandLine cmd) {
    Set<Arg> argSet = new HashSet<>(Arrays.asList(Command.UPDATE_STORE.getOptionalArgs()));
    argSet.addAll(new HashSet<>(Arrays.asList(Command.UPDATE_STORE.getRequiredArgs())));

    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    genericParam(cmd, Arg.OWNER, s -> s, p -> params.setOwner(p), argSet);
    integerParam(cmd, Arg.PARTITION_COUNT, p -> params.setPartitionCount(p), argSet);
    genericParam(cmd, Arg.PARTITIONER_CLASS, s -> s, p -> params.setPartitionerClass(p), argSet);
    stringMapParam(cmd, Arg.PARTITIONER_PARAMS, p -> params.setPartitionerParams(p), argSet);
    integerParam(cmd, Arg.VERSION, p -> params.setCurrentVersion(p), argSet);
    integerParam(cmd, Arg.LARGEST_USED_VERSION_NUMBER, p -> params.setLargestUsedVersionNumber(p), argSet);
    integerParam(cmd, Arg.LARGEST_USED_RT_VERSION_NUMBER, p -> params.setLargestUsedRTVersionNumber(p), argSet);
    booleanParam(cmd, Arg.READABILITY, p -> params.setEnableReads(p), argSet);
    booleanParam(cmd, Arg.WRITEABILITY, p -> params.setEnableWrites(p), argSet);
    longParam(cmd, Arg.STORAGE_QUOTA, p -> params.setStorageQuotaInByte(p), argSet);
    booleanParam(cmd, Arg.STORAGE_NODE_READ_QUOTA_ENABLED, p -> params.setStorageNodeReadQuotaEnabled(p), argSet);
    booleanParam(cmd, Arg.HYBRID_STORE_OVERHEAD_BYPASS, p -> params.setHybridStoreOverheadBypass(p), argSet);
    longParam(cmd, Arg.READ_QUOTA, p -> params.setReadQuotaInCU(p), argSet);
    longParam(cmd, Arg.HYBRID_REWIND_SECONDS, p -> params.setHybridRewindSeconds(p), argSet);
    longParam(cmd, Arg.HYBRID_OFFSET_LAG, p -> params.setHybridOffsetLagThreshold(p), argSet);
    longParam(cmd, Arg.HYBRID_TIME_LAG, p -> params.setHybridTimeLagThreshold(p), argSet);
    genericParam(
        cmd,
        Arg.HYBRID_DATA_REPLICATION_POLICY,
        s -> DataReplicationPolicy.valueOf(s),
        p -> params.setHybridDataReplicationPolicy(p),
        argSet);
    genericParam(
        cmd,
        Arg.HYBRID_BUFFER_REPLAY_POLICY,
        s -> BufferReplayPolicy.valueOf(s),
        p -> params.setHybridBufferReplayPolicy(p),
        argSet);
    booleanParam(cmd, Arg.ACCESS_CONTROL, p -> params.setAccessControlled(p), argSet);
    genericParam(
        cmd,
        Arg.COMPRESSION_STRATEGY,
        s -> CompressionStrategy.valueOf(s),
        p -> params.setCompressionStrategy(p),
        argSet);
    booleanParam(cmd, Arg.CLIENT_DECOMPRESSION_ENABLED, p -> params.setClientDecompressionEnabled(p), argSet);
    booleanParam(cmd, Arg.CHUNKING_ENABLED, p -> params.setChunkingEnabled(p), argSet);
    booleanParam(cmd, Arg.RMD_CHUNKING_ENABLED, p -> params.setRmdChunkingEnabled(p), argSet);
    integerParam(cmd, Arg.BATCH_GET_LIMIT, p -> params.setBatchGetLimit(p), argSet);
    integerParam(cmd, Arg.NUM_VERSIONS_TO_PRESERVE, p -> params.setNumVersionsToPreserve(p), argSet);
    booleanParam(cmd, Arg.INCREMENTAL_PUSH_ENABLED, p -> params.setIncrementalPushEnabled(p), argSet);
    booleanParam(cmd, Arg.SEPARATE_REALTIME_TOPIC_ENABLED, p -> params.setSeparateRealTimeTopicEnabled(p), argSet);
    booleanParam(cmd, Arg.WRITE_COMPUTATION_ENABLED, p -> params.setWriteComputationEnabled(p), argSet);
    booleanParam(cmd, Arg.READ_COMPUTATION_ENABLED, p -> params.setReadComputationEnabled(p), argSet);
    booleanParam(cmd, Arg.ENABLE_STORE_MIGRATION, p -> params.setStoreMigration(p), argSet);
    integerParam(
        cmd,
        Arg.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOUR,
        p -> params.setBootstrapToOnlineTimeoutInHours(p),
        argSet);
    genericParam(cmd, Arg.BACKUP_STRATEGY, s -> BackupStrategy.valueOf(s), p -> params.setBackupStrategy(p), argSet);
    booleanParam(cmd, Arg.AUTO_SCHEMA_REGISTER_FOR_PUSHJOB_ENABLED, p -> params.setAutoSchemaPushJobEnabled(p), argSet);
    booleanParam(cmd, Arg.HYBRID_STORE_DISK_QUOTA_ENABLED, p -> params.setHybridStoreDiskQuotaEnabled(p), argSet);
    booleanParam(cmd, Arg.REGULAR_VERSION_ETL_ENABLED, p -> params.setRegularVersionETLEnabled(p), argSet);
    booleanParam(cmd, Arg.FUTURE_VERSION_ETL_ENABLED, p -> params.setFutureVersionETLEnabled(p), argSet);
    genericParam(cmd, Arg.ETLED_PROXY_USER_ACCOUNT, s -> s, p -> params.setEtledProxyUserAccount(p), argSet);
    booleanParam(cmd, Arg.NATIVE_REPLICATION_ENABLED, p -> params.setNativeReplicationEnabled(p), argSet);
    genericParam(cmd, Arg.PUSH_STREAM_SOURCE_ADDRESS, s -> s, p -> params.setPushStreamSourceAddress(p), argSet);
    stringMapParam(cmd, Arg.STORE_VIEW_CONFIGS, p -> params.setStoreViews(p), argSet);
    longParam(
        cmd,
        Arg.BACKUP_VERSION_RETENTION_DAY,
        p -> params.setBackupVersionRetentionMs(p * Time.MS_PER_DAY),
        argSet);
    integerParam(cmd, Arg.REPLICATION_FACTOR, p -> params.setReplicationFactor(p), argSet);
    genericParam(
        cmd,
        Arg.NATIVE_REPLICATION_SOURCE_FABRIC,
        s -> s,
        p -> params.setNativeReplicationSourceFabric(p),
        argSet);
    booleanParam(cmd, Arg.ACTIVE_ACTIVE_REPLICATION_ENABLED, p -> params.setActiveActiveReplicationEnabled(p), argSet);
    genericParam(cmd, Arg.REGIONS_FILTER, s -> s, p -> params.setRegionsFilter(p), argSet);
    genericParam(cmd, Arg.STORAGE_PERSONA, s -> s, p -> params.setStoragePersona(p), argSet);
    integerParam(cmd, Arg.LATEST_SUPERSET_SCHEMA_ID, p -> params.setLatestSupersetSchemaId(p), argSet);
    booleanParam(cmd, Arg.ENABLE_COMPACTION, p -> params.setCompactionEnabled(p), argSet);
    longParam(cmd, Arg.COMPACTION_THRESHOLD_MILLISECONDS, p -> params.setCompactionThresholdMilliseconds(p), argSet);
    longParam(cmd, Arg.MIN_COMPACTION_LAG_SECONDS, p -> params.setMinCompactionLagSeconds(p), argSet);
    longParam(cmd, Arg.MAX_COMPACTION_LAG_SECONDS, p -> params.setMaxCompactionLagSeconds(p), argSet);
    integerParam(cmd, Arg.MAX_RECORD_SIZE_BYTES, params::setMaxRecordSizeBytes, argSet);
    integerParam(cmd, Arg.MAX_NEARLINE_RECORD_SIZE_BYTES, params::setMaxNearlineRecordSizeBytes, argSet);
    booleanParam(cmd, Arg.UNUSED_SCHEMA_DELETION_ENABLED, p -> params.setUnusedSchemaDeletionEnabled(p), argSet);
    booleanParam(cmd, Arg.BLOB_TRANSFER_ENABLED, p -> params.setBlobTransferEnabled(p), argSet);
    booleanParam(
        cmd,
        Arg.NEARLINE_PRODUCER_COMPRESSION_ENABLED,
        p -> params.setNearlineProducerCompressionEnabled(p),
        argSet);
    integerParam(cmd, Arg.NEARLINE_PRODUCER_COUNT_PER_WRITER, p -> params.setNearlineProducerCountPerWriter(p), argSet);
    genericParam(cmd, Arg.TARGET_SWAP_REGION, s -> s, p -> params.setTargetRegionSwap(p), argSet);
    integerParam(cmd, Arg.TARGET_SWAP_REGION_WAIT_TIME, p -> params.setTargetRegionSwapWaitTime(p), argSet);
    booleanParam(cmd, Arg.DAVINCI_HEARTBEAT_REPORTED, p -> params.setIsDavinciHeartbeatReported(p), argSet);
    booleanParam(cmd, Arg.GLOBAL_RT_DIV_ENABLED, params::setGlobalRtDivEnabled, argSet);

    /**
     * {@link Arg#REPLICATE_ALL_CONFIGS} doesn't require parameters; once specified, it means true.
     */
    boolean replicateAllConfigs = cmd.hasOption(Arg.REPLICATE_ALL_CONFIGS.toString());
    params.setReplicateAllConfigs(replicateAllConfigs);

    if (cmd.hasOption(Arg.DISABLE_META_STORE.toString())) {
      params.setDisableMetaStore();
    }
    if (cmd.hasOption(Arg.DISABLE_DAVINCI_PUSH_STATUS_STORE.toString())) {
      params.setDisableDavinciPushStatusStore();
    }

    /**
     * By default when SRE updates storage quota using AdminTool, we will set the bypass as true,
     * i.e. hybrid store storage quota will not be added overhead ratio automatically.
     */
    if (params.getStorageQuotaInByte().isPresent() && !params.getHybridStoreOverheadBypass().isPresent()) {
      params.setHybridStoreOverheadBypass(true);
    }
    return params;
  }

  protected static UpdateClusterConfigQueryParams getUpdateClusterConfigQueryParams(CommandLine cmd) {
    UpdateClusterConfigQueryParams params = new UpdateClusterConfigQueryParams();

    String fabric = getOptionalArgument(cmd, Arg.FABRIC);

    String quotaForFabricStr = getOptionalArgument(cmd, Arg.SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND);
    if (quotaForFabricStr != null) {
      params.setServerKafkaFetchQuotaRecordsPerSecondForRegion(fabric, Long.parseLong(quotaForFabricStr));
    }

    String storeMigrationAllowed = getOptionalArgument(cmd, Arg.ALLOW_STORE_MIGRATION);
    if (storeMigrationAllowed != null) {
      params.setStoreMigrationAllowed(Boolean.parseBoolean(storeMigrationAllowed));
    }

    String adminTopicConsumptionEnabled =
        getOptionalArgument(cmd, Arg.CHILD_CONTROLLER_ADMIN_TOPIC_CONSUMPTION_ENABLED);
    if (adminTopicConsumptionEnabled != null) {
      params.setChildControllerAdminTopicConsumptionEnabled(Boolean.parseBoolean(adminTopicConsumptionEnabled));
    }

    return params;
  }

  private static void applyValueSchemaToStore(CommandLine cmd) throws Exception {
    String store = getRequiredArgument(cmd, Arg.STORE, Command.ADD_SCHEMA);
    String valueSchemaFile = getRequiredArgument(cmd, Arg.VALUE_SCHEMA, Command.ADD_SCHEMA);
    String valueSchema = readFile(valueSchemaFile);
    verifyValidSchema(valueSchema);
    SchemaResponse valueResponse = controllerClient.addValueSchema(store, valueSchema);
    if (valueResponse.isError()) {
      throw new VeniceException("Error updating store with schema: " + valueResponse.getError());
    }
    printObject(valueResponse);
  }

  private static ZkClient readZKConfigAndBuildZKClient(String veniceZookeeperUrl, String zkSSLFile) throws Exception {
    if (!zkSSLFile.isEmpty()) {
      Properties systemProperties = System.getProperties();
      try (BufferedReader br =
          new BufferedReader(new InputStreamReader(new FileInputStream(zkSSLFile), StandardCharsets.UTF_8))) {
        String newLine = br.readLine();
        while (newLine != null) {
          String[] tokens = newLine.split("=");
          if (tokens.length != 2) {
            throw new VeniceException(
                "ZK SSL config file format is incorrect: " + newLine
                    + "\nZK SSL config file content example: zookeeper.client.secure=true");
          }
          systemProperties.put(tokens[0], tokens[1]);
          newLine = br.readLine();
        }
      }
      // Verified all required ZK SSL configs are present
      for (String requiredZKSSLProperty: REQUIRED_ZK_SSL_SYSTEM_PROPERTIES) {
        if (!systemProperties.containsKey(requiredZKSSLProperty)) {
          throw new VeniceException("Missing required ZK SSL property: " + requiredZKSSLProperty);
        }
      }
      System.setProperties(systemProperties);
    }
    return ZkClientFactory.newZkClient(veniceZookeeperUrl);
  }

  private static void backupStoreMetadataFromGraveyard(CommandLine cmd) throws Exception {
    String backupFolderPath = getRequiredArgument(cmd, Arg.BACKUP_FOLDER, Command.BACKUP_STORE_METADATA_FROM_GRAVEYARD);
    // Construct ZK client
    String veniceZookeeperUrl =
        getRequiredArgument(cmd, Arg.VENICE_ZOOKEEPER_URL, Command.BACKUP_STORE_METADATA_FROM_GRAVEYARD);
    String graveyardClusters = getRequiredArgument(cmd, Arg.GRAVEYARD_CLUSTERS, "").trim();
    if (graveyardClusters.isEmpty()) {
      throw new VeniceException("Graveyard clusters argument shouldn't be empty");
    }
    List<String> graveyardClusterList =
        Arrays.stream(graveyardClusters.split(",")).map(s -> s.trim()).collect(Collectors.toList());
    String zkSSLFile = getRequiredArgument(cmd, Arg.ZK_SSL_CONFIG_FILE, Command.BACKUP_STORE_METADATA_FROM_GRAVEYARD);

    ZkClient zkClient = readZKConfigAndBuildZKClient(veniceZookeeperUrl, zkSSLFile);
    try {
      RecoverStoreMetadata.backupStoreGraveyard(zkClient, graveyardClusterList, backupFolderPath);
    } finally {
      zkClient.close();
    }
  }

  private static void recoverStoreMetadata(CommandLine cmd, PubSubClientsFactory pubSubClientsFactory)
      throws Exception {
    String store = getRequiredArgument(cmd, Arg.STORE, Command.RECOVER_STORE_METADATA);
    String url = getRequiredArgument(cmd, Arg.URL, Command.RECOVER_STORE_METADATA);
    boolean skipLastStoreCreation =
        Boolean.parseBoolean(getOptionalArgument(cmd, Arg.SKIP_LAST_STORE_CREATION, "false"));
    boolean doRepair = Boolean.parseBoolean(getOptionalArgument(cmd, Arg.REPAIR, "false"));
    String graveyardClusters = getRequiredArgument(cmd, Arg.GRAVEYARD_CLUSTERS, Command.RECOVER_STORE_METADATA).trim();
    if (graveyardClusters.isEmpty()) {
      throw new VeniceException("Graveyard clusters argument shouldn't be empty");
    }
    List<String> graveyardClusterList =
        Arrays.stream(graveyardClusters.split(",")).map(s -> s.trim()).collect(Collectors.toList());
    String recoverCluster = getOptionalArgument(cmd, Arg.RECOVER_CLUSTER, "");

    // Construct ZK client
    String veniceZookeeperUrl = getRequiredArgument(cmd, Arg.VENICE_ZOOKEEPER_URL, Command.RECOVER_STORE_METADATA);
    // Check SSL configs in JVM system arguments for ZK
    String zkSSLFile = getOptionalArgument(cmd, Arg.ZK_SSL_CONFIG_FILE, "");
    ZkClient zkClient = readZKConfigAndBuildZKClient(veniceZookeeperUrl, zkSSLFile);

    String consumerConfigFile = getOptionalArgument(cmd, Arg.KAFKA_CONSUMER_CONFIG_FILE, "");
    // Construct consumer to dump admin message
    Properties consumerProperties =
        consumerConfigFile.isEmpty() ? new Properties() : loadProperties(cmd, Arg.KAFKA_CONSUMER_CONFIG_FILE);
    String pubSubBrokerUrl = getRequiredArgument(cmd, Arg.KAFKA_BOOTSTRAP_SERVERS, Command.RECOVER_STORE_METADATA);
    consumerProperties = DumpAdminMessages.getPubSubConsumerProperties(pubSubBrokerUrl, consumerProperties);
    PubSubConsumerAdapter consumer = getConsumer(consumerProperties, pubSubClientsFactory);

    try {
      RecoverStoreMetadata.recover(
          zkClient,
          consumer,
          sslFactory,
          url,
          store,
          skipLastStoreCreation,
          doRepair,
          graveyardClusterList,
          recoverCluster);
    } finally {
      consumer.close();
      zkClient.close();
    }
  }

  private static void applyValueSchemaToZK(CommandLine cmd) throws Exception {
    String store = getRequiredArgument(cmd, Arg.STORE, Command.ADD_SCHEMA_TO_ZK);
    String cluster = getRequiredArgument(cmd, Arg.CLUSTER, Command.ADD_SCHEMA_TO_ZK);
    String veniceZookeeperUrl = getRequiredArgument(cmd, Arg.VENICE_ZOOKEEPER_URL, Command.ADD_SCHEMA_TO_ZK);
    String valueSchemaFile = getRequiredArgument(cmd, Arg.VALUE_SCHEMA, Command.ADD_SCHEMA_TO_ZK);
    int valueSchemaId = Utils.parseIntFromString(
        getRequiredArgument(cmd, Arg.VALUE_SCHEMA_ID, Command.ADD_SCHEMA_TO_ZK),
        Arg.VALUE_SCHEMA_ID.toString());
    // Check SSL configs in JVM system arguments for ZK
    String zkSSLFile = getRequiredArgument(cmd, Arg.ZK_SSL_CONFIG_FILE, Command.ADD_SCHEMA_TO_ZK);

    String valueSchemaStr = readFile(valueSchemaFile);
    verifyValidSchema(valueSchemaStr);
    Schema newValueSchema = Schema.parse(valueSchemaStr);

    ZkClient zkClient = readZKConfigAndBuildZKClient(veniceZookeeperUrl, zkSSLFile);
    try {
      HelixSchemaAccessor schemaAccessor = new HelixSchemaAccessor(zkClient, new HelixAdapterSerializer(), cluster);
      if (schemaAccessor.getValueSchema(store, String.valueOf(valueSchemaId)) != null) {
        System.err.println(
            "Schema version " + valueSchemaId + " is already registered in ZK for store " + store + ", do nothing!");
        return;
      }

      // Check backward compatibility?
      List<SchemaEntry> allValueSchemas = schemaAccessor.getAllValueSchemas(store);
      for (SchemaEntry schemaEntry: allValueSchemas) {
        SchemaCompatibility.SchemaPairCompatibility backwardCompatibility =
            SchemaCompatibility.checkReaderWriterCompatibility(newValueSchema, schemaEntry.getSchema());
        if (!backwardCompatibility.getType().equals(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE)) {
          System.err.println(
              "New value schema for store " + store + " is not backward compatible with a previous schema version "
                  + schemaEntry.getId() + ". Abort.");
          return;
        }
      }

      // Register it
      schemaAccessor.addValueSchema(store, new SchemaEntry(valueSchemaId, newValueSchema));
    } finally {
      zkClient.close();
    }
  }

  private static void applyDerivedSchemaToStore(CommandLine cmd) throws Exception {
    String store = getRequiredArgument(cmd, Arg.STORE, Command.ADD_DERIVED_SCHEMA);
    String derivedSchemaFile = getRequiredArgument(cmd, Arg.DERIVED_SCHEMA, Command.ADD_DERIVED_SCHEMA);
    int valueSchemaId = Utils.parseIntFromString(
        getRequiredArgument(cmd, Arg.VALUE_SCHEMA_ID, Command.ADD_DERIVED_SCHEMA),
        "value schema id");

    String derivedSchemaStr = readFile(derivedSchemaFile);
    verifyValidSchema(derivedSchemaStr);
    SchemaResponse valueResponse = controllerClient.addDerivedSchema(store, valueSchemaId, derivedSchemaStr);
    if (valueResponse.isError()) {
      throw new VeniceException("Error updating store with schema: " + valueResponse.getError());
    }
    printObject(valueResponse);
  }

  private static void removeDerivedSchema(CommandLine cmd) {
    String store = getRequiredArgument(cmd, Arg.STORE, Command.REMOVE_DERIVED_SCHEMA);
    int valueSchemaId = Utils.parseIntFromString(
        getRequiredArgument(cmd, Arg.VALUE_SCHEMA_ID, Command.REMOVE_DERIVED_SCHEMA),
        "value schema id");
    int derivedSchemaId = Utils.parseIntFromString(
        getRequiredArgument(cmd, Arg.DERIVED_SCHEMA_ID, Command.REMOVE_DERIVED_SCHEMA),
        "derived schema id");

    SchemaResponse derivedSchemaResponse = controllerClient.removeDerivedSchema(store, valueSchemaId, derivedSchemaId);
    if (derivedSchemaResponse.isError()) {
      throw new VeniceException("Error removing derived schema. " + derivedSchemaResponse.getError());
    }
    printObject(derivedSchemaResponse);
  }

  private static void printStoreDescription(String storeName) {
    StoreResponse response = controllerClient.getStore(storeName);
    printObject(response);
  }

  private static void printStorageNodeList() {
    MultiNodeResponse nodeResponse = controllerClient.listStorageNodes();
    printObject(nodeResponse);
  }

  private static void printInstancesStatuses(CommandLine cmd) {
    String enableReplicas = getOptionalArgument(cmd, Arg.ENABLE_DISABLED_REPLICA, "false");
    MultiNodesStatusResponse nodeResponse = controllerClient.listInstancesStatuses(enableReplicas.equals("true"));
    printObject(nodeResponse);
  }

  private static void printStoresStatuses() {
    MultiStoreStatusResponse storeResponse = controllerClient.listStoresStatuses();
    printObject(storeResponse);
  }

  private static void printReplicaListForStoreVersion(CommandLine cmd) {
    String store = getRequiredArgument(cmd, Arg.STORE, Command.REPLICAS_OF_STORE);
    int version = Utils
        .parseIntFromString(getRequiredArgument(cmd, Arg.VERSION, Command.REPLICAS_OF_STORE), Arg.VERSION.toString());
    MultiReplicaResponse response = controllerClient.listReplicas(store, version);
    printObject(response);
  }

  private static void printReplicaListForStorageNode(CommandLine cmd) {
    String storageNodeId = getRequiredArgument(cmd, Arg.STORAGE_NODE);
    MultiReplicaResponse response = controllerClient.listStorageNodeReplicas(storageNodeId);
    printObject(response);
  }

  private static void isNodeRemovable(CommandLine cmd) {
    String storageNodeId = getRequiredArgument(cmd, Arg.STORAGE_NODE);
    NodeStatusResponse response = controllerClient.isNodeRemovable(storageNodeId);
    printObject(response);
  }

  private static void addNodeIntoAllowList(CommandLine cmd) {
    String storageNodeId = getRequiredArgument(cmd, Arg.STORAGE_NODE);
    ControllerResponse response = controllerClient.addNodeIntoAllowList(storageNodeId);
    printSuccess(response);
  }

  private static void removeNodeFromAllowList(CommandLine cmd) {
    String storageNodeId = getRequiredArgument(cmd, Arg.STORAGE_NODE);
    ControllerResponse response = controllerClient.removeNodeFromAllowList(storageNodeId);
    printSuccess(response);
  }

  private static void removeNodeFromCluster(CommandLine cmd) {
    String storageNodeId = getRequiredArgument(cmd, Arg.STORAGE_NODE);
    ControllerResponse response = controllerClient.removeNodeFromCluster(storageNodeId);
    printSuccess(response);
  }

  private static void enableThrottling(boolean enable) {
    ControllerResponse response = controllerClient.enableThrottling(enable);
    printSuccess(response);
  }

  private static void enableMaxCapacityProtection(boolean enable) {
    ControllerResponse response = controllerClient.enableMaxCapacityProtection(enable);
    printSuccess(response);
  }

  private static void enableQuotaRebalance(CommandLine cmd, boolean enable) {
    int expectedRouterCount = 0;
    if (!enable) {
      expectedRouterCount = Integer.parseInt(getRequiredArgument(cmd, Arg.EXPECTED_ROUTER_COUNT));
    }
    ControllerResponse response = controllerClient.enableQuotaRebalanced(enable, expectedRouterCount);
    printSuccess(response);
  }

  private static void getRoutersClusterConfig() {
    RoutersClusterConfigResponse response = controllerClient.getRoutersClusterConfig();
    printObject(response);
  }

  private static void getAllMigrationPushStrategies() {
    MigrationPushStrategyResponse response = controllerClient.getMigrationPushStrategies();
    printObject(response);
  }

  private static void getMigrationPushStrategy(CommandLine cmd) {
    String voldemortStoreName = getRequiredArgument(cmd, Arg.VOLDEMORT_STORE);
    MigrationPushStrategyResponse response = controllerClient.getMigrationPushStrategies();
    if (response.isError()) {
      printObject(response);
    } else {
      Map<String, String> resultMap = new HashMap<>();
      Map<String, String> migrationStrategies = response.getStrategies();
      String pushStrategy = "Unknown in Venice";
      if (migrationStrategies.containsKey(voldemortStoreName)) {
        pushStrategy = migrationStrategies.get(voldemortStoreName);
      }
      resultMap.put("voldemortStoreName", voldemortStoreName);
      resultMap.put("pushStrategy", pushStrategy);
      printObject(resultMap);
    }
  }

  private static void setMigrationPushStrategy(CommandLine cmd) {
    String voldemortStoreName = getRequiredArgument(cmd, Arg.VOLDEMORT_STORE);
    String pushStrategy = getRequiredArgument(cmd, Arg.MIGRATION_PUSH_STRATEGY);
    ControllerResponse response = controllerClient.setMigrationPushStrategy(voldemortStoreName, pushStrategy);
    printSuccess(response);
  }

  private static void convertVsonSchemaAndExit(CommandLine cmd) throws IOException {
    String keySchemaStr = readFile(getRequiredArgument(cmd, Arg.KEY_SCHEMA));
    String valueSchemaStr = readFile(getRequiredArgument(cmd, Arg.VALUE_SCHEMA));

    System.out.println(
        String.format(
            "{\n  \"Avro key schema\": \"%s\",\n  \"Avro value schema\": \"%s\"\n}",
            VsonAvroSchemaAdapter.parse(keySchemaStr).toString(),
            VsonAvroSchemaAdapter.parse(valueSchemaStr).toString()));

    Utils.exit("convertVsonSchemaAndExit");
  }

  private static void listBootstrappingVersions(CommandLine cmd) {
    MultiVersionStatusResponse response = controllerClient.listBootstrappingVersions();
    printObject(response);
  }

  private static void deleteKafkaTopic(CommandLine cmd, PubSubClientsFactory pubSubClientsFactory) throws Exception {
    long startTime = System.currentTimeMillis();
    String kafkaBootstrapServer = getRequiredArgument(cmd, Arg.KAFKA_BOOTSTRAP_SERVERS);
    Properties properties = loadProperties(cmd, Arg.KAFKA_CONSUMER_CONFIG_FILE);
    properties.put(KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServer);
    VeniceProperties veniceProperties = new VeniceProperties(properties);
    int kafkaTimeOut = 30 * Time.MS_PER_SECOND;
    int topicDeletionStatusPollingInterval = 2 * Time.MS_PER_SECOND;
    if (cmd.hasOption(Arg.KAFKA_OPERATION_TIMEOUT.toString())) {
      kafkaTimeOut = Integer.parseInt(getRequiredArgument(cmd, Arg.KAFKA_OPERATION_TIMEOUT)) * Time.MS_PER_SECOND;
    }

    TopicManagerContext topicManagerContext =
        new TopicManagerContext.Builder().setPubSubPropertiesSupplier(k -> veniceProperties)
            .setPubSubOperationTimeoutMs(kafkaTimeOut)
            .setTopicDeletionStatusPollIntervalMs(topicDeletionStatusPollingInterval)
            .setTopicMinLogCompactionLagMs(0L)
            .setPubSubConsumerAdapterFactory(pubSubClientsFactory.getConsumerAdapterFactory())
            .setPubSubAdminAdapterFactory(pubSubClientsFactory.getAdminAdapterFactory())
            .setPubSubTopicRepository(TOPIC_REPOSITORY)
            .setPubSubPositionTypeRegistry(PubSubPositionTypeRegistry.fromPropertiesOrDefault(veniceProperties))
            .setTopicMetadataFetcherConsumerPoolSize(1)
            .setTopicMetadataFetcherThreadPoolSize(1)
            .setVeniceComponent(VeniceComponent.ADMIN_TOOL)
            .build();

    try (TopicManager topicManager =
        new TopicManagerRepository(topicManagerContext, kafkaBootstrapServer).getLocalTopicManager()) {
      String topicName = getRequiredArgument(cmd, Arg.KAFKA_TOPIC_NAME);
      try {
        topicManager.ensureTopicIsDeletedAndBlock(TOPIC_REPOSITORY.getTopic(topicName));
        long runTime = System.currentTimeMillis() - startTime;
        printObject("Topic '" + topicName + "' is deleted. Run time: " + runTime + " ms.");
      } catch (PubSubOpTimeoutException e) {
        printErrAndThrow(e, "Topic deletion timed out for: '" + topicName + "' after " + kafkaTimeOut + " ms.", null);
      } catch (VeniceException e) {
        printErrAndThrow(e, "Topic deletion failed due to ExecutionException", null);
      }
    }
  }

  private static void dumpAdminMessages(CommandLine cmd, PubSubClientsFactory pubSubClientsFactory) {
    Properties consumerProperties = loadProperties(cmd, Arg.KAFKA_CONSUMER_CONFIG_FILE);
    String pubSubBrokerUrl = getRequiredArgument(cmd, Arg.KAFKA_BOOTSTRAP_SERVERS);
    consumerProperties = DumpAdminMessages.getPubSubConsumerProperties(pubSubBrokerUrl, consumerProperties);
    PubSubConsumerAdapter consumer = getConsumer(consumerProperties, pubSubClientsFactory);
    List<DumpAdminMessages.AdminOperationInfo> adminMessages = DumpAdminMessages.dumpAdminMessages(
        consumer,
        getRequiredArgument(cmd, Arg.CLUSTER),
        Long.parseLong(getRequiredArgument(cmd, Arg.STARTING_OFFSET)),
        Integer.parseInt(getRequiredArgument(cmd, Arg.MESSAGE_COUNT)));
    printObject(adminMessages);
  }

  private static void dumpControlMessages(CommandLine cmd, PubSubClientsFactory pubSubClientsFactory) {
    Properties consumerProps = loadProperties(cmd, Arg.KAFKA_CONSUMER_CONFIG_FILE);
    String kafkaUrl = getRequiredArgument(cmd, Arg.KAFKA_BOOTSTRAP_SERVERS);

    consumerProps.setProperty(KAFKA_BOOTSTRAP_SERVERS, kafkaUrl);
    // This is a temporary fix for the issue described here
    // https://stackoverflow.com/questions/37363119/kafka-producer-org-apache-kafka-common-serialization-stringserializer-could-no
    // In our case "com.linkedin.venice.serialization.KafkaKeySerializer" class can not be found
    // because class loader has no venice-common in class path. This can be only reproduced on JDK11
    // Trying to avoid class loading via Kafka's ConfigDef class
    consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class);
    consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, KafkaValueSerializer.class);

    String kafkaTopic = getRequiredArgument(cmd, Arg.KAFKA_TOPIC_NAME);
    int partitionNumber = Integer.parseInt(getRequiredArgument(cmd, Arg.KAFKA_TOPIC_PARTITION));
    int startingOffset = Integer.parseInt(getRequiredArgument(cmd, Arg.STARTING_OFFSET));
    int messageCount = Integer.parseInt(getRequiredArgument(cmd, Arg.MESSAGE_COUNT));
    try (PubSubConsumerAdapter consumer = getConsumer(consumerProps, pubSubClientsFactory)) {
      new ControlMessageDumper(consumer, kafkaTopic, partitionNumber, startingOffset, messageCount).fetch().display();
    }
  }

  private static void queryKafkaTopic(CommandLine cmd, PubSubClientsFactory pubSubClientsFactory)
      throws java.text.ParseException {
    Properties consumerProps = loadProperties(cmd, Arg.KAFKA_CONSUMER_CONFIG_FILE);
    String kafkaUrl = getRequiredArgument(cmd, Arg.KAFKA_BOOTSTRAP_SERVERS);
    consumerProps.setProperty(KAFKA_BOOTSTRAP_SERVERS, kafkaUrl);

    String kafkaTopic = getRequiredArgument(cmd, Arg.KAFKA_TOPIC_NAME);
    String startDateInPST = getRequiredArgument(cmd, Arg.START_DATE);
    String endDateInPST = getOptionalArgument(cmd, Arg.END_DATE);
    String progressInterval = getOptionalArgument(cmd, Arg.PROGRESS_INTERVAL);
    String keyString = getRequiredArgument(cmd, Arg.KEY);

    try (PubSubConsumerAdapter consumer = getConsumer(consumerProps, pubSubClientsFactory)) {
      TopicMessageFinder.find(
          controllerClient,
          consumer,
          kafkaTopic,
          keyString,
          Utils.parseDateTimeToEpoch(startDateInPST, DEFAULT_DATE_FORMAT, PST_TIME_ZONE),
          endDateInPST == null
              ? Long.MAX_VALUE
              : Utils.parseDateTimeToEpoch(endDateInPST, DEFAULT_DATE_FORMAT, PST_TIME_ZONE),
          progressInterval == null ? 1000000 : Long.parseLong(progressInterval));
    }
  }

  private static void dumpKafkaTopic(CommandLine cmd, PubSubClientsFactory pubSubClientsFactory)
      throws java.text.ParseException {
    Properties consumerProps = loadProperties(cmd, Arg.KAFKA_CONSUMER_CONFIG_FILE);
    String kafkaUrl = getRequiredArgument(cmd, Arg.KAFKA_BOOTSTRAP_SERVERS);

    consumerProps.setProperty(KAFKA_BOOTSTRAP_SERVERS, kafkaUrl);
    consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class);
    consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, KafkaValueSerializer.class);

    String kafkaTopic = getRequiredArgument(cmd, Arg.KAFKA_TOPIC_NAME);
    // optional arguments
    int partitionNumber = (getOptionalArgument(cmd, Arg.KAFKA_TOPIC_PARTITION) == null)
        ? -1
        : Integer.parseInt(getOptionalArgument(cmd, Arg.KAFKA_TOPIC_PARTITION));
    long startingOffset = (getOptionalArgument(cmd, Arg.STARTING_OFFSET) == null)
        ? -1
        : Long.parseLong(getOptionalArgument(cmd, Arg.STARTING_OFFSET));
    int messageCount = (getOptionalArgument(cmd, Arg.MESSAGE_COUNT) == null)
        ? -1
        : Integer.parseInt(getOptionalArgument(cmd, Arg.MESSAGE_COUNT));
    String parentDir = "./";
    if (getOptionalArgument(cmd, Arg.PARENT_DIRECTORY) != null) {
      parentDir = getOptionalArgument(cmd, Arg.PARENT_DIRECTORY);
    }
    int maxConsumeAttempts = 3;
    if (getOptionalArgument(cmd, Arg.MAX_POLL_ATTEMPTS) != null) {
      maxConsumeAttempts = Integer.parseInt(getOptionalArgument(cmd, Arg.MAX_POLL_ATTEMPTS));
    }
    String startDatetime = getOptionalArgument(cmd, Arg.START_DATE);
    long startTimestamp =
        startDatetime == null ? -1 : Utils.parseDateTimeToEpoch(startDatetime, DEFAULT_DATE_FORMAT, PST_TIME_ZONE);
    if (startTimestamp != -1 && startingOffset != -1) {
      throw new VeniceException("Only one of start date and starting offset can be specified");
    }

    String endDatetime = getOptionalArgument(cmd, Arg.END_DATE);
    long endTimestamp =
        endDatetime == null ? -1 : Utils.parseDateTimeToEpoch(endDatetime, DEFAULT_DATE_FORMAT, PST_TIME_ZONE);

    boolean logMetadata = cmd.hasOption(Arg.LOG_METADATA.toString());
    boolean logDataRecord = cmd.hasOption(Arg.LOG_DATA_RECORD.toString());
    boolean logRmdRecord = cmd.hasOption(Arg.LOG_RMD_RECORD.toString());
    boolean logTsRecord = cmd.hasOption(Arg.LOG_TS_RECORD.toString());
    try (PubSubConsumerAdapter consumer = getConsumer(consumerProps, pubSubClientsFactory)) {
      PubSubTopicPartition topicPartition =
          new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic(kafkaTopic), partitionNumber);
      long startOffset =
          KafkaTopicDumper.calculateStartingOffset(consumer, topicPartition, startingOffset, startTimestamp);
      long endOffset = KafkaTopicDumper.calculateEndingOffset(consumer, topicPartition, endTimestamp);
      if (messageCount <= 0) {
        messageCount = (int) (endOffset - startOffset);
      }
      LOGGER.info(
          "TopicPartition: {} Start offset: {}, End offset: {}, Message count: {}",
          topicPartition,
          startOffset,
          endOffset,
          messageCount);
      try (KafkaTopicDumper ktd = new KafkaTopicDumper(
          controllerClient,
          consumer,
          topicPartition,
          parentDir,
          maxConsumeAttempts,
          logMetadata,
          logDataRecord,
          logRmdRecord,
          logTsRecord)) {
        ktd.fetchAndProcess(startOffset, endOffset, messageCount);
      } catch (Exception e) {
        System.err.println("Something went wrong during topic dump");
        e.printStackTrace();
      }
    }
  }

  private static void checkWhetherStoreMigrationIsAllowed(ControllerClient controllerClient) {
    StoreMigrationResponse response = controllerClient.isStoreMigrationAllowed();
    if (response.isError()) {
      throw new VeniceException("Could not check whether store migration is allowed " + response.getError());
    }
    if (!response.isStoreMigrationAllowed()) {
      throw new VeniceException(
          "Cluster " + controllerClient.getClusterName() + " does not allow store migration operations!");
    }
  }

  private static void checkPreconditionForStoreMigration(ControllerClient srcClient, ControllerClient destClient) {
    checkWhetherStoreMigrationIsAllowed(srcClient);
    checkWhetherStoreMigrationIsAllowed(destClient);
  }

  private static void assertStoreNotMigrating(ControllerClient controllerClient, String storeName) {
    StoreResponse storeResponse = controllerClient.getStore(storeName);
    if (storeResponse.isError()) {
      printObject(storeResponse);
    } else {
      // Store migration should not be started already.
      if (storeResponse.getStore().isMigrating()) {
        System.err.println(
            String.format(
                "ERROR: store %s is migrating. Finish the current migration before starting a new one.",
                storeName));
        throw new VeniceException(
            String.format("Store %s is migrating. Finish the current migration before starting a new one.", storeName));
      }
    }
  }

  private static void migrateStore(CommandLine cmd) {
    String veniceUrl = getRequiredArgument(cmd, Arg.URL);
    String storeName = getRequiredArgument(cmd, Arg.STORE);
    String srcClusterName = getRequiredArgument(cmd, Arg.CLUSTER_SRC);
    String destClusterName = getRequiredArgument(cmd, Arg.CLUSTER_DEST);
    if (srcClusterName.equals(destClusterName)) {
      throw new VeniceException("Source cluster and destination cluster cannot be the same!");
    }

    ControllerClient srcControllerClient = new ControllerClient(srcClusterName, veniceUrl, sslFactory);
    ControllerClient destControllerClient = new ControllerClient(destClusterName, veniceUrl, sslFactory);
    checkPreconditionForStoreMigration(srcControllerClient, destControllerClient);
    assertStoreNotMigrating(srcControllerClient, storeName);

    StoreMigrationResponse storeMigrationResponse = srcControllerClient.migrateStore(storeName, destClusterName);
    printObject(storeMigrationResponse);

    if (storeMigrationResponse.isError()) {
      System.err.println("ERROR: Store migration failed!");
      return;
    }

    // Logging to stderr means users can see it but it gets ignored
    // if you (for example) pipe the output to jq.
    // This maintains the ability to easily write scripts around the admin tool that do parsing of the output.
    System.err.println(
        "\nThe migration request has been submitted successfully.\n"
            + "Make sure at least one version is online before deleting the original store.\n"
            + "You can check the migration process using admin-tool command --migration-status.\n"
            + "To complete migration fabric by fabric, use admin-tool command --complete-migration.");
  }

  @FunctionalInterface
  public interface PrintFunction {
    void apply(String message);
  }

  private static void printMigrationStatus(ControllerClient controller, String storeName, PrintFunction printFunction) {
    StoreInfo store = controller.getStore(storeName).getStore();

    printFunction.apply("\n" + controller.getClusterName() + "\t" + controller.getLeaderControllerUrl());

    if (store == null) {
      printFunction.apply(storeName + " DOES NOT EXIST in this cluster " + controller.getClusterName());
    } else {
      printFunction.apply(storeName + " exists in this cluster " + controller.getClusterName());
      printFunction.apply("\t" + storeName + ".isMigrating = " + store.isMigrating());
      printFunction.apply("\t" + storeName + ".largestUsedVersion = " + store.getLargestUsedVersionNumber());
      printFunction.apply("\t" + storeName + ".currentVersion = " + store.getCurrentVersion());
      printFunction.apply("\t" + storeName + ".versions = ");
      store.getVersions().stream().forEach(version -> printFunction.apply("\t\t" + version.toString()));
    }

    printFunction.apply(
        "\t" + storeName + " belongs to cluster " + controller.discoverCluster(storeName).getCluster()
            + " according to cluster discovery");
  }

  private static void printSystemStoreMigrationStatus(
      ControllerClient controller,
      String storeName,
      PrintFunction printFunction) {
    MultiStoreResponse clusterStores = controller.queryStoreList();

    for (String currStoreName: clusterStores.getStores()) {
      VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(currStoreName);

      if (systemStoreType != null && systemStoreType.extractRegularStoreName(currStoreName).equals(storeName)) {
        printMigrationStatus(controller, currStoreName, printFunction);
      }
    }
  }

  private static void checkMigrationStatus(CommandLine cmd) {
    checkMigrationStatus(cmd, System.err::println);
  }

  public static void checkMigrationStatus(CommandLine cmd, PrintFunction printFunction) {
    checkMigrationStatus(
        cmd,
        printFunction,
        new ControllerClient(getRequiredArgument(cmd, Arg.CLUSTER_SRC), getRequiredArgument(cmd, Arg.URL), sslFactory),
        new ControllerClient(
            getRequiredArgument(cmd, Arg.CLUSTER_DEST),
            getRequiredArgument(cmd, Arg.URL),
            sslFactory));
  }

  public static void checkMigrationStatus(
      CommandLine cmd,
      PrintFunction printFunction,
      ControllerClient srcControllerClient,
      ControllerClient destControllerClient) {
    String storeName = getRequiredArgument(cmd, Arg.STORE);
    String srcClusterName = getRequiredArgument(cmd, Arg.CLUSTER_SRC);
    String destClusterName = getRequiredArgument(cmd, Arg.CLUSTER_DEST);
    if (srcClusterName.equals(destClusterName)) {
      throw new VeniceException("Source cluster and destination cluster cannot be the same!");
    }

    ChildAwareResponse response = srcControllerClient.listChildControllers(srcClusterName);

    if (response.getChildDataCenterControllerUrlMap() == null && response.getChildDataCenterControllerD2Map() == null) {
      // This is a controller in single datacenter setup
      printMigrationStatus(srcControllerClient, storeName, printFunction);
      printMigrationStatus(destControllerClient, storeName, printFunction);

      printSystemStoreMigrationStatus(srcControllerClient, storeName, printFunction);
      printSystemStoreMigrationStatus(destControllerClient, storeName, printFunction);
    } else {
      // This is a parent controller
      printFunction.apply("\n=================== Parent Controllers ====================");
      printMigrationStatus(srcControllerClient, storeName, printFunction);
      printMigrationStatus(destControllerClient, storeName, printFunction);

      printSystemStoreMigrationStatus(srcControllerClient, storeName, printFunction);
      printSystemStoreMigrationStatus(destControllerClient, storeName, printFunction);

      Map<String, ControllerClient> srcChildControllerClientMap = getControllerClientMap(srcClusterName, response);
      Map<String, ControllerClient> destChildControllerClientMap = getControllerClientMap(destClusterName, response);

      for (Map.Entry<String, ControllerClient> entry: srcChildControllerClientMap.entrySet()) {
        printFunction.apply("\n\n=================== Child Datacenter " + entry.getKey() + " ====================");

        ControllerClient srcChildController = entry.getValue();
        ControllerClient destChildController = destChildControllerClientMap.get(entry.getKey());

        printMigrationStatus(srcChildController, storeName, printFunction);
        printMigrationStatus(destChildController, storeName, printFunction);

        printSystemStoreMigrationStatus(srcChildController, storeName, printFunction);
        printSystemStoreMigrationStatus(destChildController, storeName, printFunction);
      }
    }
  }

  private static void completeMigration(CommandLine cmd) {
    String veniceUrl = getRequiredArgument(cmd, Arg.URL);
    String storeName = getRequiredArgument(cmd, Arg.STORE);
    String srcClusterName = getRequiredArgument(cmd, Arg.CLUSTER_SRC);
    String destClusterName = getRequiredArgument(cmd, Arg.CLUSTER_DEST);
    String fabric = getRequiredArgument(cmd, Arg.FABRIC);

    ControllerClient srcControllerClient = new ControllerClient(srcClusterName, veniceUrl, sslFactory);
    ControllerClient destControllerClient = new ControllerClient(destClusterName, veniceUrl, sslFactory);
    checkPreconditionForStoreMigration(srcControllerClient, destControllerClient);

    ChildAwareResponse response = destControllerClient.listChildControllers(destClusterName);
    if (response.getChildDataCenterControllerUrlMap() == null && response.getChildDataCenterControllerD2Map() == null) {
      // This is a controller in single datacenter setup
      System.out.println("WARN: fabric option is ignored on child controller.");
      if (isClonedStoreOnline(srcControllerClient, destControllerClient, storeName)) {
        System.err.println(
            "Cloned store is ready in dest cluster " + destClusterName + ". Updating cluster discovery info...");
        srcControllerClient.completeMigration(storeName, destClusterName);
      } else {
        System.err
            .println("Cloned store is not ready in dest cluster " + destClusterName + ". Please try again later.");
      }
    } else {
      // This is a parent controller
      Map<String, ControllerClient> destChildControllerClientMap = getControllerClientMap(destClusterName, response);
      if (!destChildControllerClientMap.containsKey(fabric)) {
        System.err.println("ERROR: parent controller does not know the controller url or d2 of" + fabric);
        return;
      }
      ControllerClient destChildController = destChildControllerClientMap.get(fabric);
      ControllerClient srcChildController = getControllerClientMap(srcClusterName, response).get(fabric);

      if (destChildController.discoverCluster(storeName).getCluster().equals(destClusterName)) {
        System.out.println(
            "WARN: " + storeName + " already belongs to dest cluster " + destClusterName + " in fabric " + fabric);
      } else {
        if (isClonedStoreOnline(srcChildController, destChildController, storeName)) {
          System.err.println(
              "Cloned store is ready in " + fabric + " dest cluster " + destClusterName
                  + ". Updating cluster discovery info...");
          srcChildController.completeMigration(storeName, destClusterName);
        } else {
          System.err.println(
              "Cloned store is not ready in " + fabric + " dest cluster " + destClusterName
                  + ". Please try again later.");
          return;
        }
      }

      // Update parent cluster discovery info if all child clusters are online.
      for (ControllerClient controllerClient: destChildControllerClientMap.values()) {
        if (!controllerClient.discoverCluster(storeName).getCluster().equals(destClusterName)) {
          // No need to update cluster discovery in parent if one child is not ready.
          return;
        }
      }
      System.err.println("\nCloned store is ready in all child clusters. Updating cluster discovery info in parent...");
      srcControllerClient.completeMigration(storeName, destClusterName);
    }
  }

  protected static boolean isClonedStoreOnline(
      ControllerClient srcControllerClient,
      ControllerClient destControllerClient,
      String storeName) {
    StoreResponse storeResponse = srcControllerClient.getStore(storeName);
    StoreInfo srcStore = storeResponse.getStore();
    if (srcStore == null) {
      throw new VeniceException("Store " + storeName + " does not exist in the original cluster!");
    }

    StoreInfo destStore = destControllerClient.getStore(storeName).getStore();
    if (destStore == null) {
      System.err.println("WARN: Cloned store has not been created in the destination cluster!");
      return false;
    }

    List<Version> srcVersions = srcStore.getVersions();
    List<Version> destVersions = destStore.getVersions();

    int srcLatestOnlineVersion = getLatestOnlineVersionNum(srcVersions);
    int destLatestOnlineVersion = getLatestOnlineVersionNum(destVersions);

    System.err.println(destControllerClient.getLeaderControllerUrl());
    if (srcLatestOnlineVersion == -1) {
      System.err.println("Original store doesn't have online version");
    } else {
      destVersions.stream().forEach(System.err::println);
    }

    /**
     * The following logic is to check whether the corresponding meta system store is fully migrated or not.
     */
    boolean destMetaStoreOnline = true;
    if (srcStore.isStoreMetaSystemStoreEnabled()) {
      String metaSystemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
      StoreInfo srcMetaSystemStore = srcControllerClient.getStore(metaSystemStoreName).getStore();
      StoreInfo destMetaSystemStore = destControllerClient.getStore(metaSystemStoreName).getStore();
      int srcLatestOnlineVersionOfMetaSystemStore = getLatestOnlineVersionNum(srcMetaSystemStore.getVersions());
      int destLatestOnlineVersionOfMetaSystemStore = getLatestOnlineVersionNum(destMetaSystemStore.getVersions());
      destMetaStoreOnline = destLatestOnlineVersionOfMetaSystemStore >= srcLatestOnlineVersionOfMetaSystemStore;
      if (!destMetaStoreOnline) {
        System.err.println(
            "Meta system store is not ready. Online version in dest cluster: "
                + destLatestOnlineVersionOfMetaSystemStore + ". Online version in src cluster: "
                + srcLatestOnlineVersionOfMetaSystemStore);
      }
    }
    boolean destDaVinciPushStatusStoreOnline = true;
    if (srcStore.isDaVinciPushStatusStoreEnabled()) {
      String daVinciPushStatusSystemStoreName =
          VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(storeName);
      StoreInfo srcDaVinciPushStatusSystemStore =
          srcControllerClient.getStore(daVinciPushStatusSystemStoreName).getStore();
      StoreInfo destDaVinciPushStatusSystemStore =
          destControllerClient.getStore(daVinciPushStatusSystemStoreName).getStore();
      int srcLatestOnlineVersionOfDaVinciPushStatusSystemStore =
          getLatestOnlineVersionNum(srcDaVinciPushStatusSystemStore.getVersions());
      int destLatestOnlineVersionOfDaVinciPushStatusSystemStore =
          getLatestOnlineVersionNum(destDaVinciPushStatusSystemStore.getVersions());
      destDaVinciPushStatusStoreOnline =
          destLatestOnlineVersionOfDaVinciPushStatusSystemStore >= srcLatestOnlineVersionOfDaVinciPushStatusSystemStore;
      if (!destDaVinciPushStatusStoreOnline) {
        System.err.println(
            "DaVinci push status system store is not ready. Online version in dest cluster: "
                + destLatestOnlineVersionOfDaVinciPushStatusSystemStore + ". Online version in src cluster: "
                + srcLatestOnlineVersionOfDaVinciPushStatusSystemStore);
      }
    }
    if (destLatestOnlineVersion < srcLatestOnlineVersion) {
      System.err.println(
          "User store is not ready. Online version in dest cluster: " + destLatestOnlineVersion
              + ".  Online version in src cluster: " + srcLatestOnlineVersion);
    }
    return (destLatestOnlineVersion >= srcLatestOnlineVersion) && destMetaStoreOnline
        && destDaVinciPushStatusStoreOnline;
  }

  private static Map<String, ControllerClient> getControllerClientMap(String clusterName, ChildAwareResponse response) {
    return clusterControllerClientPerColoMap.computeIfAbsent(clusterName, cn -> {
      Map<String, ControllerClient> controllerClientMap = new HashMap<>();
      if (response.getChildDataCenterControllerUrlMap() != null) {
        response.getChildDataCenterControllerUrlMap()
            .forEach(
                (key, value) -> controllerClientMap.put(key, new ControllerClient(clusterName, value, sslFactory)));
      }
      if (response.getChildDataCenterControllerD2Map() != null) {
        // TODO: D2Client
        response.getChildDataCenterControllerD2Map()
            .forEach(
                (key, value) -> controllerClientMap.put(
                    key,
                    new D2ControllerClient(
                        response.getD2ServiceName(),
                        clusterName,
                        D2ClientFactory.getD2Client(value, sslFactory))));
      }
      return controllerClientMap;
    });
  }

  private static int getLatestOnlineVersionNum(List<Version> versions) {
    if (versions.size() == 0) {
      return -1;
    }

    if (versions.stream().filter(v -> v.getStatus().equals(VersionStatus.ONLINE)).count() == 0) {
      return -1;
    }

    return versions.stream()
        .filter(v -> v.getStatus().equals(VersionStatus.ONLINE))
        .sorted(Comparator.comparingInt(Version::getNumber).reversed())
        .collect(Collectors.toList())
        .get(0)
        .getNumber();
  }

  private static void abortMigration(CommandLine cmd) {
    String veniceUrl = getRequiredArgument(cmd, Arg.URL);
    String storeName = getRequiredArgument(cmd, Arg.STORE);
    String srcClusterName = getRequiredArgument(cmd, Arg.CLUSTER_SRC);
    String destClusterName = getRequiredArgument(cmd, Arg.CLUSTER_DEST);
    boolean force = cmd.hasOption(Arg.FORCE.toString());

    abortMigration(veniceUrl, storeName, srcClusterName, destClusterName, force, new boolean[0]);
  }

  /**
   * @param promptsOverride is an array of boolean used to replace/override user's responses to various possible prompts
   *                        when calling abort migration programmatically. The corresponding response for each index is
   *                        defined as follows:
   *                        [0] Continue to execute abort migration even if the store doesn't appear to be migrating.
   *                        [1] Continue to reset store migration flag, storeConfig and cluster discovery mapping.
   *                        [2] Continue to delete the cloned store in the destination cluster.
   */
  public static void abortMigration(
      String veniceUrl,
      String storeName,
      String srcClusterName,
      String destClusterName,
      boolean force,
      boolean[] promptsOverride) {
    if (srcClusterName.equals(destClusterName)) {
      throw new VeniceException("Source cluster and destination cluster cannot be the same!");
    }
    boolean terminate = false;
    ControllerClient srcControllerClient =
        ControllerClient.constructClusterControllerClient(srcClusterName, veniceUrl, sslFactory);
    ControllerClient destControllerClient =
        ControllerClient.constructClusterControllerClient(destClusterName, veniceUrl, sslFactory);
    checkPreconditionForStoreMigration(srcControllerClient, destControllerClient);

    // Check arguments
    if (srcControllerClient.getStore(storeName).getStore() == null) {
      System.err.println("ERROR: Store " + storeName + " does not exist in src cluster " + srcClusterName);
      return;
    } else if (!srcControllerClient.getStore(storeName).getStore().isMigrating()) {
      System.err.println("WARNING: Store " + storeName + " is not in migration state in src cluster " + srcClusterName);
      if (promptsOverride.length > 0) {
        terminate = !promptsOverride[0];
      } else {
        terminate = !userGivesPermission("Do you still want to proceed");
      }
      if (terminate) {
        return;
      }
    }

    ControllerResponse discoveryResponse = srcControllerClient.discoverCluster(storeName);
    if (!discoveryResponse.getCluster().equals(srcClusterName)) {
      if (!force) {
        System.err.println(
            "WARNING: Either store migration has completed, or the internal states are messed up.\n"
                + "You can force execute this command with --" + Arg.FORCE.toString() + " / -" + Arg.FORCE.first()
                + ", but make sure your src and dest cluster names are correct.");
        return;
      }
    }

    // Reset original store, storeConfig, and cluster discovery
    if (promptsOverride.length > 1) {
      terminate = !promptsOverride[1];

    } else {
      terminate = !userGivesPermission(
          "Next step is to reset store migration flag, storeConfig and cluster "
              + "discovery mapping. Do you want to proceed?");
    }
    if (terminate) {
      return;
    }
    StoreMigrationResponse abortMigrationResponse = srcControllerClient.abortMigration(storeName, destClusterName);
    if (abortMigrationResponse.isError()) {
      throw new VeniceException(abortMigrationResponse.getError());
    } else {
      printObject(abortMigrationResponse);
    }

    // Delete cloned store
    if (promptsOverride.length > 2) {
      terminate = !promptsOverride[2];
    } else {
      terminate = !userGivesPermission(
          "Next step is to delete the cloned store in dest cluster " + destClusterName + ". " + storeName + " in "
              + destClusterName + " will be deleted irreversibly."
              + " Please verify there is no reads/writes to the cloned store. Do you want to proceed?");
    }
    if (terminate) {
      return;
    }

    // Cluster discovery should point to original cluster
    discoveryResponse = srcControllerClient.discoverCluster(storeName);
    if (!discoveryResponse.getCluster().equals(srcClusterName)) {
      System.err.println("ERROR: Incorrect cluster discovery result");
      return;
    }

    if (destControllerClient.getStore(storeName).getStore() != null) {
      // If multi-colo, both parent and child dest controllers will consume the delete store message
      System.err.println(
          "Deleting cloned store " + storeName + " in " + destControllerClient.getLeaderControllerUrl() + " ...");
      destControllerClient
          .updateStore(storeName, new UpdateStoreQueryParams().setEnableReads(false).setEnableWrites(false));
      TrackableControllerResponse deleteResponse = destControllerClient.deleteStore(storeName, true);
      printObject(deleteResponse);
      if (deleteResponse.isError()) {
        System.err.println("ERROR: failed to delete store " + storeName + " in the dest cluster " + destClusterName);
      }
    } else {
      System.err.println(
          "Store " + storeName + " is not found in the dest cluster " + destClusterName
              + ". Please use --migration-status to check the current status.");
    }
  }

  static boolean userGivesPermission(String prompt) {
    Console console = System.console();
    String response = console.readLine(prompt + " (y/n): ").toLowerCase();
    while (!response.equals("y") && !response.equals("n")) {
      response = console.readLine("Enter y or n: ").toLowerCase();
    }

    if (response.equals("y")) {
      return true;
    } else if (response.equals("n")) {
      return false;
    } else {
      throw new VeniceException("Cannot interpret user response \"" + response + "\" for question " + prompt);
    }
  }

  private static void endMigration(CommandLine cmd) {
    String veniceUrl = getRequiredArgument(cmd, Arg.URL);
    String storeName = getRequiredArgument(cmd, Arg.STORE);
    String srcClusterName = getRequiredArgument(cmd, Arg.CLUSTER_SRC);
    String destClusterName = getRequiredArgument(cmd, Arg.CLUSTER_DEST);
    if (srcClusterName.equals(destClusterName)) {
      throw new VeniceException("Source cluster and destination cluster cannot be the same!");
    }

    ControllerClient srcControllerClient = new ControllerClient(srcClusterName, veniceUrl, sslFactory);
    ControllerClient destControllerClient = new ControllerClient(destClusterName, veniceUrl, sslFactory);
    checkPreconditionForStoreMigration(srcControllerClient, destControllerClient);

    // Make sure destClusterName does agree with the cluster discovery result
    String clusterDiscovered = destControllerClient.discoverCluster(storeName).getCluster();
    if (!clusterDiscovered.equals(destClusterName)) {
      System.err.println(
          "ERROR: store " + storeName + " belongs to cluster " + clusterDiscovered
              + ", which is different from the dest cluster name " + destClusterName + " in your command!");
      return;
    }

    // Skip original store deletion if it has already been deleted
    StoreResponse srcStoreResponse = srcControllerClient.getStore(storeName);
    if (srcStoreResponse.isError() && srcStoreResponse.getErrorType() != ErrorType.STORE_NOT_FOUND) {
      System.err.println(
          "ERROR: failed to check store " + storeName + " existence in original cluster " + srcClusterName
              + " due to error: " + srcStoreResponse.getError());
    }

    if (srcStoreResponse.getErrorType() != ErrorType.STORE_NOT_FOUND) {
      // Delete original store
      srcControllerClient
          .updateStore(storeName, new UpdateStoreQueryParams().setEnableReads(false).setEnableWrites(false));
      TrackableControllerResponse deleteResponse = srcControllerClient.deleteStore(storeName);
      printObject(deleteResponse);
      if (deleteResponse.isError()) {
        System.err.println("ERROR: failed to delete store " + storeName + " in the original cluster " + srcClusterName);
        return;
      }
    }

    // If this is a parent controller, verify that original store is deleted in all child fabrics
    ChildAwareResponse response = srcControllerClient.listChildControllers(srcClusterName);
    Map<String, ControllerClient> srcChildControllerClientMap = getControllerClientMap(srcClusterName, response);
    for (Map.Entry<String, ControllerClient> entry: srcChildControllerClientMap.entrySet()) {
      StoreResponse childSrcStoreResponse = entry.getValue().getStore(storeName);
      if (childSrcStoreResponse.isError() && childSrcStoreResponse.getErrorType() != ErrorType.STORE_NOT_FOUND) {
        System.err.println(
            "ERROR: failed to check store " + storeName + " existence in original cluster " + srcClusterName
                + " in fabric " + entry.getKey() + " due to error: " + childSrcStoreResponse.getError());
      }
      if (childSrcStoreResponse.getErrorType() != ErrorType.STORE_NOT_FOUND) {
        System.err.println(
            "ERROR: store " + storeName + " still exists in source cluster " + srcClusterName + " in fabric "
                + entry.getKey() + ". Please try again later.");
      }
    }

    // Reset migration flags
    System.err.println("\nOriginal store does not exist. Resetting migration flags...");
    ControllerResponse controllerResponse = destControllerClient.updateStore(
        storeName,
        new UpdateStoreQueryParams().setStoreMigration(false).setMigrationDuplicateStore(false));
    printObject(controllerResponse);
  }

  public static void autoMigrateStore(CommandLine cmd) {
    String veniceUrl = getRequiredArgument(cmd, Arg.URL);
    String storeName = getRequiredArgument(cmd, Arg.STORE);
    String srcClusterName = getRequiredArgument(cmd, Arg.CLUSTER_SRC);
    String destClusterName = getRequiredArgument(cmd, Arg.CLUSTER_DEST);
    Optional<Boolean> abortOnFailure =
        Optional.ofNullable(getOptionalArgument(cmd, Arg.ABORT_ON_FAILURE)).map(Boolean::parseBoolean);
    Optional<Integer> currStep = Optional.ofNullable(getOptionalArgument(cmd, Arg.INITIAL_STEP)).map(Integer::parseInt);

    if (srcClusterName.equals(destClusterName)) {
      throw new VeniceException("Source and destination cluster cannot be the same!");
    }

    ControllerClient srcControllerClient =
        ControllerClientFactory.getControllerClient(srcClusterName, veniceUrl, sslFactory);
    ControllerClient destControllerClient =
        ControllerClientFactory.getControllerClient(destClusterName, veniceUrl, sslFactory);
    checkPreconditionForStoreMigration(srcControllerClient, destControllerClient);
    assertStoreNotMigrating(srcControllerClient, storeName);

    StoreMigrationResponse storeMigrationResponse =
        srcControllerClient.autoMigrateStore(storeName, destClusterName, currStep, abortOnFailure);
    printObject(storeMigrationResponse);

    if (storeMigrationResponse.isError()) {
      System.err.println("ERROR: Auto store migration failed!");
      return;
    }
    System.err.println("\nThe auto store migration request has been submitted successfully.\n");
  }

  private static void sendEndOfPush(CommandLine cmd) {
    String storeName = getRequiredArgument(cmd, Arg.STORE);
    String version = getRequiredArgument(cmd, Arg.VERSION);

    // Check if version is a valid integer
    int intVersion;
    try {
      intVersion = Integer.parseInt(version);
    } catch (Exception e) {
      System.err.println("ERROR: " + version + " is not a valid integer");
      return;
    }

    ControllerResponse response = controllerClient.writeEndOfPush(storeName, intVersion);
    printObject(response);
  }

  /* Things that are not commands */

  private static void printUsageAndExit(OptionGroup commandGroup, Options options, CommandLine cmd) {
    /**
     * Get the first command if it is available, otherwise print all commands.
     */
    Command foundCommand = null;
    for (Command c: Command.values()) {
      if (cmd.hasOption(c.toString())) {
        foundCommand = c;
      }
    }
    Command[] commands = Command.values();
    if (foundCommand != null) {
      commands = new Command[] { foundCommand };
      commandGroup = new OptionGroup();
      createCommandOpt(foundCommand, commandGroup);

      /**
       * Gather all the options belonging to the found command.
       */
      options = new Options();
      for (Arg arg: foundCommand.getRequiredArgs()) {
        createOpt(arg, arg.isParameterized(), arg.getHelpText(), options);
      }
      for (Arg arg: foundCommand.getOptionalArgs()) {
        createOpt(arg, arg.isParameterized(), arg.getHelpText(), options);
      }
    }

    /* Commands */
    String command = "java -jar "
        + new java.io.File(AdminTool.class.getProtectionDomain().getCodeSource().getLocation().getPath()).getName();

    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.setWidth(140);
    helpFormatter
        .printHelp(command + " --<command> [parameters]\n\nCommands:", new Options().addOptionGroup(commandGroup));

    /* Parameters */
    helpFormatter.printHelp("Parameters: ", options);

    /* Examples */
    System.out.println("\nExamples:");
    Arrays.sort(commands, Command.commandComparator);
    for (Command c: commands) {
      StringJoiner exampleArgs = new StringJoiner(" ");
      for (Arg a: c.getRequiredArgs()) {
        exampleArgs.add("--" + a.toString());
        if (a.isParameterized()) {
          exampleArgs.add("<" + a + ">");
        }
      }
      for (Arg a: c.getOptionalArgs()) {
        exampleArgs.add("[--" + a.toString());
        String param = "";
        if (a.isParameterized()) {
          param += "<" + a + ">";
        }
        exampleArgs.add(param + "]");
      }

      System.out.println(command + " --" + c + " " + exampleArgs);
    }
    Utils.exit("printUsageAndExit");
  }

  private static String getRequiredArgument(CommandLine cmd, Arg arg) {
    return getRequiredArgument(cmd, arg, "");
  }

  private static String getRequiredArgument(CommandLine cmd, Arg arg, Command command) {
    return getRequiredArgument(cmd, arg, "when using --" + command.toString());
  }

  private static String getRequiredArgument(CommandLine cmd, Arg arg, String errorClause) {
    if (!cmd.hasOption(arg.first())) {
      printErrAndExit(arg.toString() + " is a required argument " + errorClause);
    }
    return cmd.getOptionValue(arg.first());
  }

  private static String getOptionalArgument(CommandLine cmd, Arg arg) {
    return getOptionalArgument(cmd, arg, null);
  }

  private static String getOptionalArgument(CommandLine cmd, Arg arg, String defaultArgValue) {
    if (!cmd.hasOption(arg.first())) {
      return defaultArgValue;
    } else {
      return cmd.getOptionValue(arg.first());
    }
  }

  public static Properties loadProperties(CommandLine cmd, Arg arg) throws VeniceException {
    Properties properties = new Properties();
    if (cmd.hasOption(arg.toString())) {
      String configFilePath = getRequiredArgument(cmd, arg);
      try (FileInputStream fis = new FileInputStream(configFilePath)) {
        properties.load(fis);
      } catch (IOException e) {
        throw new VeniceException("Cannot read file: " + configFilePath + " specified by: " + arg.toString());
      }
    }
    return properties;
  }

  private static void verifyStoreExistence(String storename, boolean desiredExistence) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storename);
    String zkStoreName = storename;
    ControllerClient queryingControllerClient = controllerClient;
    if (systemStoreType != null && systemStoreType.isStoreZkShared()) {
      if (!desiredExistence) {
        throw new UnsupportedOperationException(
            "This method should not be used to verify if a zk shared system store doesn't exist");
      }
      zkStoreName = systemStoreType.getZkSharedStoreNameInCluster(controllerClient.getClusterName());
      if (systemStoreType.equals(VeniceSystemStoreType.META_STORE)
          || systemStoreType.equals(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE)) {
        /**
         * The ZK shared schema store only exists in system schema store cluster, which might be different
         * from the customer's store.
         */
        D2ServiceDiscoveryResponse discoveryResponse = controllerClient.discoverCluster(zkStoreName);
        if (discoveryResponse.isError()) {
          throw new VeniceException("Failed to discover cluster for store: " + zkStoreName);
        }
        String systemStoreCluster = discoveryResponse.getCluster();
        queryingControllerClient = new ControllerClient(
            systemStoreCluster,
            controllerClient.getControllerDiscoveryUrls().iterator().next(),
            sslFactory);
      }
    }
    MultiStoreResponse storeResponse = queryingControllerClient.queryStoreList(true);
    if (storeResponse.isError()) {
      throw new VeniceException("Error verifying store exists: " + storeResponse.getError());
    }
    boolean storeExists = false;
    for (String s: storeResponse.getStores()) {
      if (s.equals(zkStoreName)) {
        storeExists = true;
        break;
      }
    }
    if (storeExists != desiredExistence) {
      throw new VeniceException("Store " + storename + (storeExists ? " already exists" : " does not exist"));
    }
  }

  private static void verifyValidSchema(String schema) throws Exception {
    try {
      Schema.parse(schema);
    } catch (Exception e) {
      Map<String, String> errMap = new HashMap<>();
      errMap.put("schema", schema);
      printErrAndThrow(e, "Invalid Schema: " + e.getMessage(), errMap);
    }
  }

  private static void deleteAllVersions(CommandLine cmd) {
    String store = getRequiredArgument(cmd, Arg.STORE, Command.DELETE_ALL_VERSIONS);
    MultiVersionResponse response = controllerClient.deleteAllVersions(store);
    printObject(response);
  }

  private static void deleteOldVersion(CommandLine cmd) {
    String store = getRequiredArgument(cmd, Arg.STORE, Command.DELETE_OLD_VERSION);
    int version = Integer.parseInt(getRequiredArgument(cmd, Arg.VERSION, Command.DELETE_OLD_VERSION));
    VersionResponse response = controllerClient.deleteOldVersion(store, version);
    printObject(response);
  }

  private static void getExecution(CommandLine cmd) {
    long executionId = Long.parseLong(getRequiredArgument(cmd, Arg.EXECUTION, Command.GET_EXECUTION));
    AdminCommandExecutionResponse response = controllerClient.getAdminCommandExecution(executionId);
    printObject(response);
  }

  private static void createNewStoreWithAcl(CommandLine cmd) throws Exception {
    String store = getRequiredArgument(cmd, Arg.STORE, Command.NEW_STORE);
    String keySchemaFile = getRequiredArgument(cmd, Arg.KEY_SCHEMA, Command.NEW_STORE);
    String keySchema = readFile(keySchemaFile);
    String valueSchemaFile = getRequiredArgument(cmd, Arg.VALUE_SCHEMA, Command.NEW_STORE);
    String valueSchema = readFile(valueSchemaFile);
    String aclPerms = getRequiredArgument(cmd, Arg.ACL_PERMS, Command.NEW_STORE);
    String owner = getOptionalArgument(cmd, Arg.OWNER, "");
    boolean isVsonStore = Utils.parseBooleanOrThrow(getOptionalArgument(cmd, Arg.VSON_STORE, "false"), "isVsonStore");
    if (isVsonStore) {
      keySchema = VsonAvroSchemaAdapter.parse(keySchema).toString();
      valueSchema = VsonAvroSchemaAdapter.parse(valueSchema).toString();
    }
    verifyValidSchema(keySchema);
    verifyValidSchema(valueSchema);
    verifyStoreExistence(store, false);
    NewStoreResponse response = controllerClient.createNewStore(store, owner, keySchema, valueSchema, aclPerms);
    printObject(response);
  }

  private static void updateStoreWithAcl(CommandLine cmd) throws Exception {
    String store = getRequiredArgument(cmd, Arg.STORE, Command.UPDATE_STORE_ACL);
    String aclPerms = getRequiredArgument(cmd, Arg.ACL_PERMS, Command.UPDATE_STORE_ACL);
    verifyStoreExistence(store, true);
    AclResponse response = controllerClient.updateAclForStore(store, aclPerms);
    printObject(response);
  }

  private static void getAclForStore(CommandLine cmd) throws Exception {
    String store = getRequiredArgument(cmd, Arg.STORE, Command.UPDATE_STORE_ACL);
    verifyStoreExistence(store, true);
    AclResponse response = controllerClient.getAclForStore(store);
    printObject(response);
  }

  private static void deleteAclForStore(CommandLine cmd) throws Exception {
    String store = getRequiredArgument(cmd, Arg.STORE, Command.DELETE_STORE_ACL);
    verifyStoreExistence(store, true);
    AclResponse response = controllerClient.deleteAclForStore(store);
    printObject(response);
  }

  private static void addToStoreAcl(CommandLine cmd) throws Exception {
    String store = getRequiredArgument(cmd, Arg.STORE, Command.DELETE_STORE_ACL);
    String principal = getRequiredArgument(cmd, Arg.PRINCIPAL, Command.ADD_TO_STORE_ACL);
    boolean addReadPermissions = Boolean.parseBoolean(getOptionalArgument(cmd, Arg.READABILITY, "false"));
    boolean addWritePermissions = Boolean.parseBoolean(getOptionalArgument(cmd, Arg.WRITEABILITY, "false"));

    if (!addReadPermissions && !addWritePermissions) {
      printErrAndExit("Both Readabilty and Writeabilty can not be false or empty.");
    }

    verifyStoreExistence(store, true);

    AclResponse storeAclResponse = controllerClient.getAclForStore(store);
    if (storeAclResponse == null) {
      printErrAndExit("Failed to get existing ACLs.");
    } else if (storeAclResponse.isError()) {
      printErrAndExit(storeAclResponse.getError());
    } else {
      String oldAcls = storeAclResponse.getAccessPermissions();

      JsonNodeFactory factory = JsonNodeFactory.instance;
      ObjectNode newRoot = factory.objectNode();
      ObjectNode newPerms = factory.objectNode();
      ArrayNode newReadP = factory.arrayNode();
      ArrayNode newWriteP = factory.arrayNode();

      Iterator<JsonNode> readPermissions = null;
      Iterator<JsonNode> writePermissions = null;
      ObjectMapper mapper = ObjectMapperFactory.getInstance();
      try {
        JsonNode root = mapper.readTree(oldAcls);
        JsonNode perms = root.path("AccessPermissions");
        if (perms.has("Read")) {
          readPermissions = perms.path("Read").elements();
        }
        if (perms.has("Write")) {
          writePermissions = perms.path("Write").elements();
        }
      } catch (Exception e) {
        printErrAndThrow(e, "ACLProvisioning: invalid accessPermission schema for store:" + store, null);
      }

      if (readPermissions != null) {
        while (readPermissions.hasNext()) {
          String existingPrincipal = readPermissions.next().textValue();
          if (existingPrincipal.equals(principal)) {
            addReadPermissions = false;
          }
          newReadP.add(existingPrincipal);
        }
      }

      if (writePermissions != null) {
        while (writePermissions.hasNext()) {
          String existingPrincipal = writePermissions.next().textValue();
          if (existingPrincipal.equals(principal)) {
            addWritePermissions = false;
          }
          newWriteP.add(existingPrincipal);
        }
      }

      if (addReadPermissions) {
        newReadP.add(principal);
      }

      if (addWritePermissions) {
        newWriteP.add(principal);
      }

      if (addReadPermissions || addWritePermissions) {
        newPerms.put("Read", newReadP);
        newPerms.put("Write", newWriteP);
        newRoot.put("AccessPermissions", newPerms);

        String newAcls = mapper.writeValueAsString(newRoot);
        AclResponse response = controllerClient.updateAclForStore(store, newAcls);
        printObject(response);
      } else {
        System.out.println("No change in ACLs");
      }
    }
  }

  private static void removeFromStoreAcl(CommandLine cmd) throws Exception {
    String store = getRequiredArgument(cmd, Arg.STORE, Command.DELETE_STORE_ACL);
    String principal = getRequiredArgument(cmd, Arg.PRINCIPAL, Command.ADD_TO_STORE_ACL);
    boolean removeReadPermissions = Boolean.parseBoolean(getOptionalArgument(cmd, Arg.READABILITY, "false"));
    boolean removeWritePermissions = Boolean.parseBoolean(getOptionalArgument(cmd, Arg.WRITEABILITY, "false"));

    if (!removeReadPermissions && !removeWritePermissions) {
      printErrAndExit("Both Readabilty and Writeabilty can not be false or empty.");
    }

    verifyStoreExistence(store, true);

    AclResponse storeAclResponse = controllerClient.getAclForStore(store);
    if (storeAclResponse == null) {
      printErrAndExit("Failed to get existing ACLs.");
    } else if (storeAclResponse.isError()) {
      printErrAndExit(storeAclResponse.getError());
    } else {
      String oldAcls = storeAclResponse.getAccessPermissions();

      JsonNodeFactory factory = JsonNodeFactory.instance;
      ObjectNode newRoot = factory.objectNode();
      ObjectNode newPerms = factory.objectNode();
      ArrayNode newReadP = factory.arrayNode();
      ArrayNode newWriteP = factory.arrayNode();

      Iterator<JsonNode> readPermissions = null;
      Iterator<JsonNode> writePermissions = null;
      ObjectMapper mapper = ObjectMapperFactory.getInstance();
      try {
        JsonNode root = mapper.readTree(oldAcls);
        JsonNode perms = root.path("AccessPermissions");
        if (perms.has("Read")) {
          readPermissions = perms.path("Read").elements();
        }
        if (perms.has("Write")) {
          writePermissions = perms.path("Write").elements();
        }
      } catch (Exception e) {
        printErrAndThrow(e, "ACLProvisioning: invalid accessPermission schema for store:" + store, null);
      }

      boolean changed = false;
      if (readPermissions != null) {
        while (readPermissions.hasNext()) {
          String existingPrincipal = readPermissions.next().textValue();
          if (removeReadPermissions && existingPrincipal.equals(principal)) {
            changed = true;
            continue;
          }
          newReadP.add(existingPrincipal);
        }
      }

      if (writePermissions != null) {
        while (writePermissions.hasNext()) {
          String existingPrincipal = writePermissions.next().textValue();
          if (removeWritePermissions && existingPrincipal.equals(principal)) {
            changed = true;
            continue;
          }
          newWriteP.add(existingPrincipal);
        }
      }

      if (changed) {
        newPerms.put("Read", newReadP);
        newPerms.put("Write", newWriteP);
        newRoot.put("AccessPermissions", newPerms);

        String newAcls = mapper.writeValueAsString(newRoot);
        AclResponse response = controllerClient.updateAclForStore(store, newAcls);
        printObject(response);
      } else {
        System.out.println("No change in ACLs");
      }
    }
  }

  private static void enableActiveActiveReplicationForCluster(CommandLine cmd) {
    String storeType = getRequiredArgument(cmd, Arg.STORE_TYPE);
    String regionsFilterParam = getOptionalArgument(cmd, Arg.REGIONS_FILTER);
    Optional<String> regionsFilter =
        StringUtils.isEmpty(regionsFilterParam) ? Optional.empty() : Optional.of(regionsFilterParam);

    ControllerResponse response =
        controllerClient.configureActiveActiveReplicationForCluster(true, storeType, regionsFilter);
    printObject(response);
  }

  private static void disableActiveActiveReplicationForCluster(CommandLine cmd) {
    String storeType = getRequiredArgument(cmd, Arg.STORE_TYPE);
    String regionsFilterParam = getOptionalArgument(cmd, Arg.REGIONS_FILTER);
    Optional<String> regionsFilter =
        StringUtils.isEmpty(regionsFilterParam) ? Optional.empty() : Optional.of(regionsFilterParam);

    ControllerResponse response =
        controllerClient.configureActiveActiveReplicationForCluster(false, storeType, regionsFilter);
    printObject(response);
  }

  private static void getDeletableStoreTopics(CommandLine cmd) {
    MultiStoreTopicsResponse response = controllerClient.getDeletableStoreTopics();
    printObject(response);
  }

  private static void wipeCluster(CommandLine cmd) {
    String fabric = getRequiredArgument(cmd, Arg.FABRIC);
    Optional<String> storeName = Optional.ofNullable(getOptionalArgument(cmd, Arg.STORE));
    Optional<Integer> versionNum = Optional.ofNullable(getOptionalArgument(cmd, Arg.VERSION)).map(Integer::parseInt);
    ControllerResponse response = controllerClient.wipeCluster(fabric, storeName, versionNum);
    printObject(response);
  }

  private static void listClusterStaleStores(CommandLine cmd) {
    String clusterParam = getRequiredArgument(cmd, Arg.CLUSTER);
    String urlParam = getRequiredArgument(cmd, Arg.URL);
    ClusterStaleDataAuditResponse response = controllerClient.getClusterStaleStores(clusterParam, urlParam);
    printObject(response);
  }

  private static void repushStore(CommandLine cmd) {
    String storeName = getRequiredArgument(cmd, Arg.STORE);
    RepushJobResponse response = controllerClient.repushStore(storeName);
    printObject(response);
  }

  private static void getDeadStores(CommandLine cmd) {
    String clusterName = getRequiredArgument(cmd, Arg.CLUSTER);
    Optional<String> storeName = Optional.ofNullable(getOptionalArgument(cmd, Arg.STORE));
    boolean includeSystemStores = Boolean.parseBoolean(getOptionalArgument(cmd, Arg.INCLUDE_SYSTEM_STORES));

    MultiStoreInfoResponse response = controllerClient.getDeadStores(clusterName, includeSystemStores, storeName);
    printObject(response);
  }

  private static void listStorePushInfo(CommandLine cmd) {
    String storeParam = getRequiredArgument(cmd, Arg.STORE);
    boolean isPartitionDetailEnabled = Optional.ofNullable(getOptionalArgument(cmd, Arg.PARTITION_DETAIL_ENABLED))
        .map(Boolean::parseBoolean)
        .orElse(false);

    StoreHealthAuditResponse response = controllerClient.listStorePushInfo(storeParam, isPartitionDetailEnabled);
    printObject(response);
  }

  private static void compareStore(CommandLine cmd) {
    String storeName = getRequiredArgument(cmd, Arg.STORE);
    String fabricA = getRequiredArgument(cmd, Arg.FABRIC_A);
    String fabricB = getRequiredArgument(cmd, Arg.FABRIC_B);

    StoreComparisonResponse response = controllerClient.compareStore(storeName, fabricA, fabricB);
    if (response.isError()) {
      throw new VeniceException("Error comparing store " + storeName + ". Error: " + response.getError());
    }
    printObject(response);
  }

  private static void copyOverStoresMetadata(CommandLine cmd) {
    String sourceFabric = getRequiredArgument(cmd, Arg.SOURCE_FABRIC);
    String destFabric = getRequiredArgument(cmd, Arg.DEST_FABRIC);
    String storeName = getRequiredArgument(cmd, Arg.STORE);
    StoreResponse response = controllerClient.copyOverStoreMetadata(sourceFabric, destFabric, storeName);
    printObject(response);
  }

  private static void getKafkaTopicConfigs(CommandLine cmd) {
    String veniceControllerUrls = getRequiredArgument(cmd, Arg.URL);
    String kafkaTopicName = getRequiredArgument(cmd, Arg.KAFKA_TOPIC_NAME);
    String clusterName = null;
    if (AdminTopicUtils.isAdminTopic(kafkaTopicName)) {
      clusterName = AdminTopicUtils.getClusterNameFromTopicName(kafkaTopicName);
    } else {
      String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopicName);
      if (storeName.isEmpty()) {
        throw new VeniceException("Please either provide a valid topic name.");
      }
      D2ServiceDiscoveryResponse clusterDiscovery =
          ControllerClient.discoverCluster(veniceControllerUrls, storeName, sslFactory, 3);
      clusterName = clusterDiscovery.getCluster();
    }
    try (ControllerClient tmpControllerClient = new ControllerClient(clusterName, veniceControllerUrls, sslFactory)) {
      PubSubTopicConfigResponse response = tmpControllerClient.getKafkaTopicConfigs(kafkaTopicName);
      printObject(response);
    }
  }

  private static void updateKafkaTopicLogCompaction(CommandLine cmd) {
    updateKafkaTopicConfig(cmd, client -> {
      String kafkaTopicName = getRequiredArgument(cmd, Arg.KAFKA_TOPIC_NAME);
      boolean enableKafkaLogCompaction =
          Boolean.parseBoolean(getRequiredArgument(cmd, Arg.KAFKA_TOPIC_LOG_COMPACTION_ENABLED));
      return client.updateKafkaTopicLogCompaction(kafkaTopicName, enableKafkaLogCompaction);
    });
  }

  private static void updateKafkaTopicRetention(CommandLine cmd) {
    updateKafkaTopicConfig(cmd, client -> {
      String kafkaTopicName = getRequiredArgument(cmd, Arg.KAFKA_TOPIC_NAME);
      long kafkaTopicRetentionTimeInMs = Long.parseLong(getRequiredArgument(cmd, Arg.KAFKA_TOPIC_RETENTION_IN_MS));
      return client.updateKafkaTopicRetention(kafkaTopicName, kafkaTopicRetentionTimeInMs);
    });
  }

  private static void updateKafkaTopicMinInSyncReplica(CommandLine cmd) {
    updateKafkaTopicConfig(cmd, client -> {
      String kafkaTopicName = getRequiredArgument(cmd, Arg.KAFKA_TOPIC_NAME);
      int kafkaTopicMinISR = Integer.parseInt(getRequiredArgument(cmd, Arg.KAFKA_TOPIC_MIN_IN_SYNC_REPLICA));
      return client.updateKafkaTopicMinInSyncReplica(kafkaTopicName, kafkaTopicMinISR);
    });
  }

  private static void updateKafkaTopicConfig(CommandLine cmd, UpdateTopicConfigFunction updateTopicConfigFunction) {
    String veniceControllerUrls = getRequiredArgument(cmd, Arg.URL);
    String kafkaTopicName = getRequiredArgument(cmd, Arg.KAFKA_TOPIC_NAME);
    /**
     * cluster name is optional; if cluster name is specified, no need to do cluster discovery; this option is helpful
     * to create a ControllerClient with a default cluster, in order to modify a Kafka topic that doesn't belong to
     * any existing Venice store resource
     */
    String clusterName = getOptionalArgument(cmd, Arg.CLUSTER);
    if (clusterName == null) {
      if (AdminTopicUtils.isAdminTopic(kafkaTopicName)) {
        clusterName = AdminTopicUtils.getClusterNameFromTopicName(kafkaTopicName);
      } else {
        String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopicName);
        if (storeName.isEmpty()) {
          throw new VeniceException("Please either provide a valid topic name or a cluster name.");
        }
        D2ServiceDiscoveryResponse clusterDiscovery =
            ControllerClient.discoverCluster(veniceControllerUrls, storeName, sslFactory, 3);
        clusterName = clusterDiscovery.getCluster();
      }
    }

    try (ControllerClient tmpControllerClient = new ControllerClient(clusterName, veniceControllerUrls, sslFactory)) {
      ControllerResponse response = updateTopicConfigFunction.apply(tmpControllerClient);
      printObject(response);
    }
  }

  @FunctionalInterface
  interface UpdateTopicConfigFunction {
    ControllerResponse apply(ControllerClient controllerClient);
  }

  private static void startFabricBuildout(CommandLine cmd) {
    String clusterName = getRequiredArgument(cmd, Arg.CLUSTER);
    String srcFabric = getRequiredArgument(cmd, Arg.SOURCE_FABRIC);
    String destFabric = getRequiredArgument(cmd, Arg.DEST_FABRIC);
    boolean retry = cmd.hasOption(Arg.RETRY.toString());
    String latestStep = "";

    try {
      latestStep = "constructing child controller clients";
      Map<String, ControllerClient> map = getAndCheckChildControllerClientMap(clusterName, srcFabric, destFabric);
      ControllerClient srcFabricChildControllerClient = map.get(srcFabric);
      ControllerClient destFabricChildControllerClient = map.get(destFabric);

      latestStep = "step1: disallowing store migration from/to cluster " + clusterName;
      System.out.println(latestStep);
      checkControllerResponse(
          controllerClient.updateClusterConfig(new UpdateClusterConfigQueryParams().setStoreMigrationAllowed(false)));

      latestStep = "step2: disabling " + clusterName + " admin topic consumption in dest fabric child controller";
      System.out.println(latestStep);
      checkControllerResponse(
          destFabricChildControllerClient.updateClusterConfig(
              new UpdateClusterConfigQueryParams().setChildControllerAdminTopicConsumptionEnabled(false)));

      if (!retry) {
        latestStep = "step3: wiping " + clusterName + " in dest fabric";
        System.out.println(latestStep);
        checkControllerResponse(controllerClient.wipeCluster(destFabric, Optional.empty(), Optional.empty()));

        latestStep = "step4: copying cluster-level admin topic execution id and offsets";
        System.out.println(latestStep);
        AdminTopicMetadataResponse response =
            checkControllerResponse(srcFabricChildControllerClient.getAdminTopicMetadata(Optional.empty()));
        checkControllerResponse(
            destFabricChildControllerClient.updateAdminTopicMetadata(
                response.getExecutionId(),
                Optional.empty(),
                Optional.of(response.getOffset()),
                Optional.of(response.getUpstreamOffset())));
      }

      latestStep = "step5: copying store metadata and starting data recovery for non-existent stores in dest fabric";
      System.out.println(latestStep);
      List<String> failedStores = copyStoreMetadataAndStartDataRecovery(
          srcFabric,
          destFabric,
          srcFabricChildControllerClient,
          destFabricChildControllerClient);

      if (failedStores.isEmpty()) {
        latestStep = "step6: enabling admin consumption in dest fabric child controller";
        System.out.println(latestStep);
        checkControllerResponse(
            destFabricChildControllerClient.updateClusterConfig(
                new UpdateClusterConfigQueryParams().setChildControllerAdminTopicConsumptionEnabled(true)));
        System.out.println("Command succeeded. Please run check-fabric-buildout-status to track buildout progress");
      } else {
        System.err.println(
            "Command failed for some stores " + failedStores
                + " Please investigate and rerun start-fabric-buildout with --retry option");
      }
    } catch (Exception e) {
      System.err.println("Command failed during " + latestStep + ". Exception: " + e);
    }
  }

  private static List<String> copyStoreMetadataAndStartDataRecovery(
      String srcFabric,
      String destFabric,
      ControllerClient srcFabricChildControllerClient,
      ControllerClient destFabricChildControllerClient) {
    List<String> failedStores = new ArrayList<>();
    int maxAttempts = 3;
    long delayInMillis = 10000;
    MultiStoreResponse multiStoreResponse =
        checkControllerResponse(srcFabricChildControllerClient.queryStoreList(false));

    for (String storeName: multiStoreResponse.getStores()) {
      if (destFabricChildControllerClient.getStore(storeName).getStore() == null) {
        System.out.println("Start copying store " + storeName + " metadata and data from src to dest fabric...");
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
          try {
            StoreInfo storeInfo =
                checkControllerResponse(controllerClient.copyOverStoreMetadata(srcFabric, destFabric, storeName))
                    .getStore();
            for (Version version: storeInfo.getVersions()) {
              checkControllerResponse(
                  controllerClient
                      .prepareDataRecovery(srcFabric, destFabric, storeName, version.getNumber(), Optional.empty()));
              RetryUtils.executeWithMaxAttempt(() -> {
                ReadyForDataRecoveryResponse response = checkControllerResponse(
                    controllerClient.isStoreVersionReadyForDataRecovery(
                        srcFabric,
                        destFabric,
                        storeName,
                        version.getNumber(),
                        Optional.empty()));
                if (!response.isReady()) {
                  throw new VeniceException(
                      "Store " + storeName + " version " + version.getNumber() + " is not ready for data recovery: "
                          + response.getReason());
                }
              }, maxAttempts, Duration.ofMillis(delayInMillis), Collections.singletonList(VeniceException.class));
              checkControllerResponse(
                  controllerClient.dataRecovery(
                      srcFabric,
                      destFabric,
                      storeName,
                      version.getNumber(),
                      false,
                      true,
                      Optional.empty()));
            }
            break;
          } catch (Exception e) {
            System.err.println(
                "Failed to copy store " + storeName + " from src to dest fabric, attempt=" + attempt + "/" + maxAttempts
                    + ". Exception: " + e);
            if (attempt == maxAttempts) {
              failedStores.add(storeName);
              System.err.println("Wiping store " + storeName + " in dest fabric. Store copy over failed after retries");
              checkControllerResponse(
                  controllerClient.wipeCluster(destFabric, Optional.of(storeName), Optional.empty()));
            } else {
              System.err.println(
                  "Wiping store " + storeName + " in dest fabric. Will retry store copy over in " + delayInMillis
                      + " ms");
              checkControllerResponse(
                  controllerClient.wipeCluster(destFabric, Optional.of(storeName), Optional.empty()));
              Utils.sleep(delayInMillis);
            }
          }
        }
      }
    }
    return failedStores;
  }

  private static void checkFabricBuildoutStatus(CommandLine cmd) {
    String clusterName = getRequiredArgument(cmd, Arg.CLUSTER);
    String srcFabric = getRequiredArgument(cmd, Arg.SOURCE_FABRIC);
    String destFabric = getRequiredArgument(cmd, Arg.DEST_FABRIC);

    Map<String, ControllerClient> map = getAndCheckChildControllerClientMap(clusterName, srcFabric, null);
    ControllerClient srcFabricChildControllerClient = map.get(srcFabric);

    MultiStoreResponse multiStoreResponse =
        checkControllerResponse(srcFabricChildControllerClient.queryStoreList(false));
    double numberOfStores = multiStoreResponse.getStores().length;
    List<String> completedStores = new ArrayList<>();
    List<String> pendingStores = new ArrayList<>();
    List<String> failedStores = new ArrayList<>();
    List<String> unknownStores = new ArrayList<>();
    for (String storeName: multiStoreResponse.getStores()) {
      try {
        StoreComparisonResponse response =
            checkControllerResponse(controllerClient.compareStore(storeName, srcFabric, destFabric));
        if (response.getVersionStateDiff().isEmpty() && response.getPropertyDiff().isEmpty()
            && response.getSchemaDiff().isEmpty()) {
          completedStores.add(storeName);
        } else if (!response.getVersionStateDiff().isEmpty()) {
          Map<Integer, VersionStatus> destFabricVersionStateDiffMap =
              response.getVersionStateDiff().getOrDefault(destFabric, Collections.emptyMap());
          for (Map.Entry<Integer, VersionStatus> entry: destFabricVersionStateDiffMap.entrySet()) {
            if (VersionStatus.ERROR.equals(entry.getValue())) {
              failedStores.add(storeName);
              break;
            }
          }
          if (!failedStores.contains(storeName)) {
            pendingStores.add(storeName);
          }
        } else {
          pendingStores.add(storeName);
        }
      } catch (Exception e) {
        unknownStores.add(storeName);
      }
    }

    System.out.println("=================== Fabric Buildout Report ====================");
    System.out.println(
        completedStores.size() / numberOfStores * 100 + "% stores in dest fabric are ready : " + completedStores);
    System.out.println(
        pendingStores.size() / numberOfStores * 100 + "% stores in dest fabric are still in progress: "
            + pendingStores);
    System.out.println(
        failedStores.size() / numberOfStores * 100 + "% stores in dest fabric have ingestion error: " + failedStores);
    System.out.println(
        unknownStores.size() / numberOfStores * 100 + "% stores are not comparable"
            + " (stores do not exist in dest fabric or compare-store requests fail): " + unknownStores);
  }

  private static void endFabricBuildout(CommandLine cmd) {
    String clusterName = getRequiredArgument(cmd, Arg.CLUSTER);
    try {
      ChildAwareResponse response = checkControllerResponse(controllerClient.listChildControllers(clusterName));
      if (response.getChildDataCenterControllerUrlMap() == null
          && response.getChildDataCenterControllerD2Map() == null) {
        throw new VeniceException("ERROR: Child controller could not run fabric buildout commands");
      }
      System.out.println("Enabling store migration from/to cluster " + clusterName);
      checkControllerResponse(
          controllerClient.updateClusterConfig(new UpdateClusterConfigQueryParams().setStoreMigrationAllowed(true)));
      System.out.println("Command succeeded. Fabric buildout ended.");
    } catch (Exception e) {
      System.err.println("Command failed. Exception: " + e);
    }
  }

  private static void createNewStoragePersona(CommandLine cmd) {
    String personaName = getRequiredArgument(cmd, Arg.STORAGE_PERSONA);
    long quota = Utils.parseLongFromString(getRequiredArgument(cmd, Arg.STORAGE_QUOTA), Arg.STORAGE_QUOTA.name());
    Set<String> storesToEnforce = Utils.parseCommaSeparatedStringToSet(getRequiredArgument(cmd, Arg.STORE));
    Set<String> owners = Utils.parseCommaSeparatedStringToSet(getRequiredArgument(cmd, Arg.OWNER));

    ControllerResponse response = controllerClient.createStoragePersona(personaName, quota, storesToEnforce, owners);
    printObject(response);
  }

  private static void getStoragePersona(CommandLine cmd) {
    String personaName = getRequiredArgument(cmd, Arg.STORAGE_PERSONA);
    StoragePersonaResponse response = controllerClient.getStoragePersona(personaName);
    printObject(response);
  }

  private static void deleteStoragePersona(CommandLine cmd) {
    String personaName = getRequiredArgument(cmd, Arg.STORAGE_PERSONA);
    ControllerResponse response = controllerClient.deleteStoragePersona(personaName);
    printObject(response);
  }

  private static void updateStoragePersona(CommandLine cmd) {
    String personaName = getRequiredArgument(cmd, Arg.STORAGE_PERSONA);
    UpdateStoragePersonaQueryParams queryParams = getUpdateStoragePersonaQueryParams(cmd);
    ControllerResponse response = controllerClient.updateStoragePersona(personaName, queryParams);
    printObject(response);
  }

  private static UpdateStoragePersonaQueryParams getUpdateStoragePersonaQueryParams(CommandLine cmd) {
    Set<Arg> argSet = new HashSet<>(Arrays.asList(Command.UPDATE_STORAGE_PERSONA.getOptionalArgs()));

    UpdateStoragePersonaQueryParams params = new UpdateStoragePersonaQueryParams();
    longParam(cmd, Arg.STORAGE_QUOTA, p -> params.setQuota(p), argSet);
    stringSetParam(cmd, Arg.STORE, p -> params.setStoresToEnforce(p), argSet);
    stringSetParam(cmd, Arg.OWNER, p -> params.setOwners(p), argSet);
    return params;
  }

  private static void getStoragePersonaForStore(CommandLine cmd) {
    String storeName = getRequiredArgument(cmd, Arg.STORE);
    StoragePersonaResponse response = controllerClient.getStoragePersonaAssociatedWithStore(storeName);
    printObject(response);
  }

  private static void listClusterStoragePersonas(CommandLine cmd) {
    MultiStoragePersonaResponse response = controllerClient.getClusterStoragePersonas();
    printObject(response);
  }

  private static void cleanupInstanceCustomizedStates(CommandLine cmd) {
    MultiStoreTopicsResponse multiStoreTopicsResponse = controllerClient.cleanupInstanceCustomizedStates();
    printObject(multiStoreTopicsResponse);
  }

  private static void getRequestBasedMetadata(CommandLine cmd) throws JsonProcessingException {
    String url = getRequiredArgument(cmd, Arg.URL);
    String serverUrl = getRequiredArgument(cmd, Arg.SERVER_URL);
    String storeName = getRequiredArgument(cmd, Arg.STORE);
    TransportClient transportClient = null;
    try {
      transportClient = getTransportClientForServer(storeName, serverUrl);
      getAndPrintRequestBasedMetadata(
          transportClient,
          () -> ControllerClientFactory.discoverAndConstructControllerClient(
              AvroProtocolDefinition.SERVER_METADATA_RESPONSE.getSystemStoreName(),
              url,
              sslFactory,
              1),
          serverUrl,
          storeName);
    } finally {
      Utils.closeQuietlyWithErrorLogged(transportClient);
    }
  }

  private static void getRequestBasedStoreProperties(CommandLine cmd) throws JsonProcessingException {
    String url = getRequiredArgument(cmd, Arg.URL);
    String serverUrl = getRequiredArgument(cmd, Arg.SERVER_URL);
    String storeName = getRequiredArgument(cmd, Arg.STORE);
    TransportClient transportClient = null;
    try {
      transportClient = getTransportClientForServer(storeName, serverUrl);
      getAndPrintRequestBasedStoreProperties(
          transportClient,
          () -> ControllerClientFactory.discoverAndConstructControllerClient(
              AvroProtocolDefinition.SERVER_STORE_PROPERTIES_PAYLOAD.getSystemStoreName(),
              url,
              sslFactory,
              1),
          serverUrl,
          storeName);
    } finally {
      Utils.closeQuietlyWithErrorLogged(transportClient);
    }
  }

  private static TransportClient getTransportClientForServer(String storeName, String serverUrl) {
    ClientConfig clientConfig = ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(serverUrl);
    if (clientConfig.isHttps()) {
      if (!sslFactory.isPresent()) {
        throw new VeniceException("HTTPS url requires admin tool to be executed with cert");
      }
      clientConfig.setSslFactory(sslFactory.get());
    }
    return ClientFactory.getTransportClient(clientConfig);
  }

  private static void dumpIngestionState(CommandLine cmd) throws Exception {
    TransportClient transportClient = null;
    try {
      transportClient =
          getTransportClientForServer(getRequiredArgument(cmd, Arg.STORE), getRequiredArgument(cmd, Arg.SERVER_URL));
      dumpIngestionState(
          transportClient,
          getRequiredArgument(cmd, Arg.STORE),
          getRequiredArgument(cmd, Arg.VERSION),
          getOptionalArgument(cmd, Arg.PARTITION));
    } finally {
      Utils.closeQuietlyWithErrorLogged(transportClient);
    }
  }

  private static void dumpTopicPartitionIngestionContext(CommandLine cmd) throws Exception {
    TransportClient transportClient = null;
    try {
      transportClient =
          getTransportClientForServer(getRequiredArgument(cmd, Arg.STORE), getRequiredArgument(cmd, Arg.SERVER_URL));
      dumpTopicPartitionIngestionContext(
          transportClient,
          getRequiredArgument(cmd, Arg.STORE),
          getRequiredArgument(cmd, Arg.VERSION),
          getRequiredArgument(cmd, Arg.KAFKA_TOPIC_NAME),
          getRequiredArgument(cmd, Arg.KAFKA_TOPIC_PARTITION));
    } finally {
      Utils.closeQuietlyWithErrorLogged(transportClient);
    }
  }

  static void dumpHostHeartbeatLag(
      TransportClient transportClient,
      String topicFilter,
      String partitionFilter,
      String lagFilter) throws Exception {
    String topicName = topicFilter == null ? "" : topicFilter;
    String partition = partitionFilter == null ? "-1" : partitionFilter;
    String filterLag = lagFilter == null ? "false" : lagFilter;
    StringBuilder sb = new StringBuilder(QueryAction.HOST_HEARTBEAT_LAG.toString().toLowerCase()).append("/")
        .append(topicName)
        .append("/")
        .append(partition)
        .append("/")
        .append(filterLag);
    String requestUrl = sb.toString();
    byte[] responseBody;
    TransportClientResponse transportClientResponse = transportClient.get(requestUrl).get();
    responseBody = transportClientResponse.getBody();
    ReplicaIngestionResponse currentVersionResponse =
        OBJECT_MAPPER.readValue(responseBody, ReplicaIngestionResponse.class);
    System.out.println(new String(currentVersionResponse.getPayload()));
  }

  private static void dumpHostHeartbeat(CommandLine cmd) throws Exception {
    TransportClient transportClient = null;
    try {
      transportClient = getTransportClientForServer("dummy", getRequiredArgument(cmd, Arg.SERVER_URL));
      dumpHostHeartbeatLag(
          transportClient,
          getRequiredArgument(cmd, Arg.KAFKA_TOPIC_NAME),
          getOptionalArgument(cmd, Arg.PARTITION),
          getOptionalArgument(cmd, Arg.LAG_FILTER_ENABLED));
    } finally {
      Utils.closeQuietlyWithErrorLogged(transportClient);
    }
  }

  private static void updateAdminOperationProtocolVersion(CommandLine cmd) throws Exception {
    String clusterName = getRequiredArgument(cmd, Arg.CLUSTER, Command.UPDATE_ADMIN_OPERATION_PROTOCOL_VERSION);
    String protocolVersionInString =
        getRequiredArgument(cmd, Arg.ADMIN_OPERATION_PROTOCOL_VERSION, Command.UPDATE_ADMIN_OPERATION_PROTOCOL_VERSION);
    long protocolVersion =
        Utils.parseLongFromString(protocolVersionInString, Arg.ADMIN_OPERATION_PROTOCOL_VERSION.name());
    AdminTopicMetadataResponse response =
        controllerClient.updateAdminOperationProtocolVersion(clusterName, protocolVersion);
    printObject(response);
  }

  private static void migrateVeniceZKPaths(CommandLine cmd) throws Exception {
    Set<String> clusterNames = Utils.parseCommaSeparatedStringToSet(getRequiredArgument(cmd, Arg.CLUSTER_LIST));
    String srcZKUrl = getRequiredArgument(cmd, Arg.SRC_ZOOKEEPER_URL);
    String srcZKSSLConfigs = getRequiredArgument(cmd, Arg.SRC_ZK_SSL_CONFIG_FILE);
    String destZKUrl = getRequiredArgument(cmd, Arg.DEST_ZOOKEEPER_URL);
    String destZKSSLConfigs = getRequiredArgument(cmd, Arg.DEST_ZK_SSL_CONFIG_FILE);
    ZkClient srcZkClient = readZKConfigAndBuildZKClient(srcZKUrl, srcZKSSLConfigs);
    ZkClient destZkClient = readZKConfigAndBuildZKClient(destZKUrl, destZKSSLConfigs);
    try {
      ZkCopier.migrateVenicePaths(srcZkClient, destZkClient, clusterNames, getRequiredArgument(cmd, Arg.BASE_PATH));
    } finally {
      srcZkClient.close();
      destZkClient.close();
    }
  }

  private static void extractVeniceZKPaths(CommandLine cmd) {
    Set<String> clusterNames = Utils.parseCommaSeparatedStringToSet(getRequiredArgument(cmd, Arg.CLUSTER_LIST));
    ZkCopier.extractVenicePaths(
        getRequiredArgument(cmd, Arg.INFILE),
        getRequiredArgument(cmd, Arg.OUTFILE),
        clusterNames,
        getRequiredArgument(cmd, Arg.BASE_PATH));
  }

  private static void getAggregatedHealthStatus(CommandLine cmd) throws JsonProcessingException {
    String clusterName = getRequiredArgument(cmd, Arg.CLUSTER);
    String instances = getRequiredArgument(cmd, Arg.INSTANCES);
    String toBeStoppedNodes = getRequiredArgument(cmd, Arg.TO_BE_STOPPED_NODES);
    ControllerResponse response = controllerClient.getAggregatedHealthStatus(
        clusterName,
        Utils.parseCommaSeparatedStringToList(instances),
        Utils.parseCommaSeparatedStringToList(toBeStoppedNodes));
    printObject(response);

  }

  private static void configureStoreView(CommandLine cmd) {
    UpdateStoreQueryParams params = getConfigureStoreViewQueryParams(cmd);
    String storeName = getRequiredArgument(cmd, Arg.STORE, Command.CONFIGURE_STORE_VIEW);
    ControllerResponse response = controllerClient.updateStore(storeName, params);
    printObject(response);
  }

  static void dumpIngestionState(TransportClient transportClient, String storeName, String version, String partition)
      throws Exception {
    StringBuilder sb = new StringBuilder(QueryAction.ADMIN.toString().toLowerCase()).append("/")
        .append(Version.composeKafkaTopic(storeName, Integer.parseInt(version)))
        .append("/")
        .append(ServerAdminAction.DUMP_INGESTION_STATE.toString().toLowerCase());
    if (partition != null) {
      sb.append("/").append(partition);
    }
    String requestUrl = sb.toString();
    TransportClientResponse transportClientResponse = transportClient.get(requestUrl).get();
    int writerSchemaId = transportClientResponse.getSchemaId();
    if (writerSchemaId == 1) {
      /**
       * The bug in {@link AvroProtocolDefinition} will let the current Venice Server return schema id `1`, while
       * the specific record is generated from schema: 2.
       */
      writerSchemaId = 2;
    }
    InternalAvroSpecificSerializer<AdminResponseRecord> serializer = SERVER_ADMIN_RESPONSE.getSerializer();
    /**
     * Here, this tool doesn't handle schema evolution, and if the writer schema is not known in the admin tool,
     * the following deserialization will fail, and we need to rebuild the admin tool.
     */
    AdminResponseRecord responseRecord = serializer.deserialize(transportClientResponse.getBody(), writerSchemaId);
    // Using the jsonWriter to print Avro objects directly does not handle the collection types (List and Map) well.
    // Use the Avro record's toString() instead and pretty print it.
    Object printObject = ObjectMapperFactory.getInstance().readValue(responseRecord.toString(), Object.class);
    System.out.println(jsonWriter.writeValueAsString(printObject));
  }

  static void dumpTopicPartitionIngestionContext(
      TransportClient transportClient,
      String storeName,
      String version,
      String topicName,
      String partition) throws Exception {
    StringBuilder sb =
        new StringBuilder(QueryAction.TOPIC_PARTITION_INGESTION_CONTEXT.toString().toLowerCase()).append("/")
            .append(Version.composeKafkaTopic(storeName, Integer.parseInt(version)))
            .append("/")
            .append(topicName)
            .append("/")
            .append(partition);
    String requestUrl = sb.toString();
    byte[] responseBody;
    TransportClientResponse transportClientResponse = transportClient.get(requestUrl).get();
    responseBody = transportClientResponse.getBody();
    ReplicaIngestionResponse currentVersionResponse =
        OBJECT_MAPPER.readValue(responseBody, ReplicaIngestionResponse.class);
    System.out.println(new String(currentVersionResponse.getPayload()));
  }

  static void getAndPrintRequestBasedMetadata(
      TransportClient transportClient,
      Supplier<ControllerClient> controllerClientSupplier,
      String serverUrl,
      String storeName) throws JsonProcessingException {
    String requestBasedMetadataURL = QueryAction.METADATA.toString().toLowerCase() + "/" + storeName;
    byte[] body;
    int writerSchemaId;
    try {
      TransportClientResponse transportClientResponse = transportClient.get(requestBasedMetadataURL).get();
      writerSchemaId = transportClientResponse.getSchemaId();
      body = transportClientResponse.getBody();
    } catch (Exception e) {
      throw new VeniceException(
          "Encountered exception while trying to send metadata request to: " + serverUrl + "/"
              + requestBasedMetadataURL,
          e);
    }
    Schema writerSchema;
    if (writerSchemaId != AvroProtocolDefinition.SERVER_METADATA_RESPONSE.getCurrentProtocolVersion()) {
      SchemaResponse schemaResponse = controllerClientSupplier.get()
          .getValueSchema(AvroProtocolDefinition.SERVER_METADATA_RESPONSE.getSystemStoreName(), writerSchemaId);
      if (schemaResponse.isError()) {
        throw new VeniceException(
            "Failed to fetch metadata response schema from controller, error: " + schemaResponse.getError());
      }
      writerSchema = parseSchemaFromJSONLooseValidation(schemaResponse.getSchemaStr());
    } else {
      writerSchema = MetadataResponseRecord.SCHEMA$;
    }
    RecordDeserializer<GenericRecord> metadataResponseDeserializer =
        FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(writerSchema, writerSchema);
    GenericRecord metadataResponse = metadataResponseDeserializer.deserialize(body);
    // Using the jsonWriter to print Avro objects directly does not handle the collection types (List and Map) well.
    // Use the Avro record's toString() instead and pretty print it.
    Object printObject = ObjectMapperFactory.getInstance().readValue(metadataResponse.toString(), Object.class);
    System.out.println(jsonWriter.writeValueAsString(printObject));
  }

  static void getAndPrintRequestBasedStoreProperties(
      TransportClient transportClient,
      Supplier<ControllerClient> controllerClientSupplier,
      String serverUrl,
      String storeName) throws JsonProcessingException {
    String requestBasedStorePropertiesURL = QueryAction.STORE_PROPERTIES.toString().toLowerCase() + "/" + storeName;
    byte[] body;
    int writerSchemaId;
    try {
      TransportClientResponse transportClientResponse = transportClient.get(requestBasedStorePropertiesURL).get();
      writerSchemaId = transportClientResponse.getSchemaId();
      body = transportClientResponse.getBody();
    } catch (Exception e) {
      throw new VeniceException(
          "Encountered exception while trying to send store properties request to: " + serverUrl + "/"
              + requestBasedStorePropertiesURL,
          e);
    }
    Schema writerSchema;
    if (writerSchemaId != AvroProtocolDefinition.SERVER_STORE_PROPERTIES_PAYLOAD.getCurrentProtocolVersion()) {
      SchemaResponse schemaResponse = controllerClientSupplier.get()
          .getValueSchema(AvroProtocolDefinition.SERVER_STORE_PROPERTIES_PAYLOAD.getSystemStoreName(), writerSchemaId);
      if (schemaResponse.isError()) {
        throw new VeniceException(
            "Failed to fetch store properties response schema from controller, error: " + schemaResponse.getError());
      }
      writerSchema = parseSchemaFromJSONLooseValidation(schemaResponse.getSchemaStr());
    } else {
      writerSchema = StorePropertiesPayloadRecord.SCHEMA$;
    }
    RecordDeserializer<GenericRecord> storePropertiesPayloadDeserializer =
        FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(writerSchema, writerSchema);
    GenericRecord storePropertiesPayload = storePropertiesPayloadDeserializer.deserialize(body);
    // Using the jsonWriter to print Avro objects directly does not handle the collection types (List and Map) well.
    // Use the Avro record's toString() instead and pretty print it.
    Object printObject = ObjectMapperFactory.getInstance().readValue(storePropertiesPayload.toString(), Object.class);
    System.out.println(jsonWriter.writeValueAsString(printObject));
  }

  private static Map<String, ControllerClient> getAndCheckChildControllerClientMap(
      String clusterName,
      String srcFabric,
      String destFabric) {
    ChildAwareResponse response = checkControllerResponse(controllerClient.listChildControllers(clusterName));
    if (response.getChildDataCenterControllerUrlMap() == null && response.getChildDataCenterControllerD2Map() == null) {
      throw new VeniceException("ERROR: Child controller could not run fabric buildout commands");
    }
    Map<String, ControllerClient> childControllerClientMap = getControllerClientMap(clusterName, response);
    if (srcFabric != null && !childControllerClientMap.containsKey(srcFabric)) {
      throw new VeniceException("ERROR: Parent controller does not know the src fabric controller url or d2 zk host");
    }
    if (destFabric != null && !childControllerClientMap.containsKey(destFabric)) {
      throw new VeniceException("ERROR: Parent controller does not know the dest fabric controller url or d2 zk host");
    }
    return childControllerClientMap;
  }

  private static <T extends ControllerResponse> T checkControllerResponse(T controllerResponse) {
    if (controllerResponse.isError()) {
      throw new VeniceException("ControllerResponse has error " + controllerResponse.getError());
    }
    return controllerResponse;
  }

  private static void printErrAndExit(String err) {
    Map<String, String> errMap = new HashMap<>();
    printErrAndExit(err, errMap);
  }

  private static void createOpt(Arg name, boolean hasArg, String help, Options options) {
    options.addOption(new Option(name.first(), name.toString(), hasArg, help));
  }

  private static void createCommandOpt(Command command, OptionGroup group) {
    group.addOption(OptionBuilder.withLongOpt(command.toString()).withDescription(command.getDesc()).create());
  }

  static String readFile(String path) throws IOException {
    String fullPath = path.replace("~", System.getProperty("user.home"));
    byte[] encoded = Files.readAllBytes(Paths.get(fullPath));
    return new String(encoded, StandardCharsets.UTF_8).trim();
  }

  private static void printReplicasReadinessStorageNode(CommandLine cmd) {
    String storageNodeId = getRequiredArgument(cmd, Arg.STORAGE_NODE);
    NodeReplicasReadinessResponse response = controllerClient.nodeReplicasReadiness(storageNodeId);
    printObject(response);
  }

  ///// Print Output ////
  private static void printObject(Object response) {
    printObject(response, System.out::print);
  }

  protected static void printObject(Object response, Consumer<String> printFunction) {
    try {
      printFunction.accept(jsonWriter.writeValueAsString(response));
      printFunction.accept("\n");
    } catch (IOException e) {
      printFunction.accept("{\"" + ERROR + "\":\"" + e.getMessage() + "\"}");
      Utils.exit("printObject");
    }
  }

  static void printSuccess(ControllerResponse response) {
    if (response.isError()) {
      printErrAndExit(response.getError());
    } else {
      System.out.println("{\"" + STATUS + "\":\"" + SUCCESS + "\"}");
    }
  }

  private static void printErrAndExit(String errorMessage, Map<String, String> customMessages) {
    printErr(errorMessage, customMessages);
    Utils.exit("venice-admin-tool encountered and error, exiting now.");
  }

  private static void printErrAndThrow(Exception e, String errorMessage, Map<String, String> customMessages)
      throws Exception {
    printErr(errorMessage, customMessages);
    throw e;
  }

  private static void printErr(String errorMessage, Map<String, String> customMessages) {
    Map<String, String> errMap = new HashMap<>();
    if (customMessages != null) {
      for (Map.Entry<String, String> messagePair: customMessages.entrySet()) {
        errMap.put(messagePair.getKey(), messagePair.getValue());
      }
    }
    if (errMap.keySet().contains(ERROR)) {
      errMap.put(ERROR, errMap.get(ERROR) + " " + errorMessage);
    } else {
      errMap.put(ERROR, errorMessage);
    }
    try {
      System.out.println(jsonWriter.writeValueAsString(errMap));
    } catch (IOException e) {
      System.out.println("{\"" + ERROR + "\":\"" + e.getMessage() + "\"}");
    }
  }

  private static PubSubConsumerAdapter getConsumer(
      Properties consumerProps,
      PubSubClientsFactory pubSubClientsFactory) {
    VeniceProperties veniceProperties = new VeniceProperties(consumerProps);
    PubSubPositionTypeRegistry pubSubPositionTypeRegistry =
        PubSubPositionTypeRegistry.fromPropertiesOrDefault(veniceProperties);

    return pubSubClientsFactory.getConsumerAdapterFactory()
        .create(
            new PubSubConsumerAdapterContext.Builder()
                .setPubSubMessageDeserializer(PubSubMessageDeserializer.createOptimizedDeserializer())
                .setPubSubPositionTypeRegistry(pubSubPositionTypeRegistry)
                .setVeniceProperties(veniceProperties)
                .setConsumerName("admin-tool-topic-dumper")
                .build());
  }

}
