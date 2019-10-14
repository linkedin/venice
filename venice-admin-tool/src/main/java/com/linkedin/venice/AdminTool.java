package com.linkedin.venice;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.venice.client.store.QueryTool;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.MigrationPushStrategyResponse;
import com.linkedin.venice.controllerapi.MultiNodeResponse;
import com.linkedin.venice.controllerapi.MultiNodesStatusResponse;
import com.linkedin.venice.controllerapi.MultiReplicaResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.controllerapi.MultiStoreStatusResponse;
import com.linkedin.venice.controllerapi.MultiVersionResponse;
import com.linkedin.venice.controllerapi.MultiVersionStatusResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.NodeStatusResponse;
import com.linkedin.venice.controllerapi.OwnerResponse;
import com.linkedin.venice.controllerapi.PartitionResponse;
import com.linkedin.venice.controllerapi.RoutersClusterConfigResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreMigrationResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.TrackableControllerResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.controllerapi.routes.AdminCommandExecutionResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.MirrorMakerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.VeniceOperationAgainstKafkaTimedOut;
import com.linkedin.venice.kafka.consumer.VeniceAdminToolConsumerFactory;
import com.linkedin.venice.kafka.consumer.VeniceConsumerFactory;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.Console;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.type.TypeReference;
import org.jetbrains.annotations.NotNull;

import static com.linkedin.venice.CommonConfigKeys.*;
import static com.linkedin.venice.VeniceConstants.*;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;


public class AdminTool {

  // TODO: static state means this can only be used by command line,
  // if we want to use this class programmatically it should get refactored.
  private static ObjectWriter jsonWriter = new ObjectMapper().writerWithDefaultPrettyPrinter();
  private static List<String> fieldsToDisplay = new ArrayList<>();
  private static final String STATUS = "status";
  private static final String ERROR = "error";
  private static final String SUCCESS = "success";

  private static ControllerClient controllerClient;
  private static Optional<SSLFactory> sslFactory = Optional.empty();

  public static void main(String args[])
      throws Exception {
    CommandLine cmd = getCommandLine(args);

    try {
      Command foundCommand = ensureOnlyOneCommand(cmd);

      // Variables used within the switch case need to be defined in advance
      String veniceUrl = null;
      String clusterName;
      String storeName;
      String versionString;
      String topicName;
      String sslConfigPath;

      int version;
      MultiStoreResponse storeResponse;
      ControllerResponse response;

      if (Arrays.stream(foundCommand.getRequiredArgs()).anyMatch(arg -> arg.equals(Arg.URL)) &&
          Arrays.stream(foundCommand.getRequiredArgs()).anyMatch(arg -> arg.equals(Arg.CLUSTER))) {
        veniceUrl = getRequiredArgument(cmd, Arg.URL);
        clusterName = getRequiredArgument(cmd, Arg.CLUSTER);

        /**
         * SSL config file is not mandatory now; build the controller with SSL config if provided.
         */
        if (Arrays.stream(foundCommand.getRequiredArgs()).anyMatch(arg -> arg.equals(Arg.SSL_CONFIG_PATH))) {
          /**
           * Build SSL factory
           */
          sslConfigPath = getRequiredArgument(cmd, Arg.SSL_CONFIG_PATH);
          Properties sslProperties = SslUtils.loadSSLConfig(sslConfigPath);
          String sslFactoryClassName = sslProperties.getProperty(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME);
          sslFactory = Optional.of(SslUtils.getSSLFactory(sslProperties, sslFactoryClassName));
        }
        controllerClient = new ControllerClient(clusterName, veniceUrl, sslFactory);
      }

      if (cmd.hasOption(Arg.FLAT_JSON.toString())){
        jsonWriter = new ObjectMapper().writer();
      }
      if (cmd.hasOption(Arg.FILTER_JSON.toString())) {
        fieldsToDisplay = Arrays.asList(cmd.getOptionValue(Arg.FILTER_JSON.first()).split(","));
      }

      switch (foundCommand) {
        case LIST_STORES:
          storeResponse = queryStoreList(cmd);
          printObject(storeResponse);
          break;
        case DESCRIBE_STORE:
          storeName = getRequiredArgument(cmd, Arg.STORE, Command.DESCRIBE_STORE);
          for (String store : storeName.split(",")) {
            printStoreDescription(store);
          }
          break;
        case DESCRIBE_STORES:
          storeResponse = queryStoreList(cmd);
          for (String store : storeResponse.getStores()) {
            printStoreDescription(store);
          }
          break;
        case JOB_STATUS:
          storeName = getRequiredArgument(cmd, Arg.STORE, Command.JOB_STATUS);
          versionString = getRequiredArgument(cmd, Arg.VERSION, Command.JOB_STATUS);
          version = Integer.parseInt(versionString);
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
        case SET_PARTITION_COUNT:
          setStorePartition(cmd);
          break;
        case UPDATE_STORE:
          updateStore(cmd);
          break;
        case ADD_SCHEMA:
          applyValueSchemaToStore(cmd);
          break;
        case ADD_DERIVED_SCHEMA:
          applyDerivedSchemaToStore(cmd);
        case LIST_STORAGE_NODES:
          printStorageNodeList();
          break;
        case CLUSTER_HEALTH_INSTANCES:
          printInstancesStatuses();
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
        case WHITE_LIST_ADD_NODE:
          addNodeIntoWhiteList(cmd);
          break;
        case WHITE_LIST_REMOVE_NODE:
          removeNodeFromWhiteList(cmd);
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
          deleteKafkaTopic(cmd);
          break;
        case START_MIRROR_MAKER:
          startMirrorMaker(cmd);
          break;
        case DUMP_ADMIN_MESSAGES:
          dumpAdminMessages(cmd);
          break;
        case DUMP_CONTROL_MESSAGES:
          dumpControlMessages(cmd);
          break;
        case MIGRATE_STORE:
          migrateStore(cmd);
          break;
        case MIGRATION_STATUS:
          checkMigrationStatus(cmd);
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
        default:
          StringJoiner availableCommands = new StringJoiner(", ");
          for (Command c : Command.values()){
            availableCommands.add("--" + c.toString());
          }
          throw new VeniceException("Must supply one of the following commands: " + availableCommands.toString());
      }
    } catch (Exception e){
      printErrAndThrow(e, e.getMessage(), null);
    }
  }

  @VisibleForTesting
  static CommandLine getCommandLine(String[] args) throws ParseException, IOException {
    /**
     * Command Options are split up for help text formatting, see printUsageAndExit()
     *
     * Gather all the commands we have in "commandGroup"
     **/
    OptionGroup commandGroup = new OptionGroup();
    for (Command c : Command.values()){
      createCommandOpt(c, commandGroup);
    }

    /**
     * Gather all the options we have in "options"
     */
    Options options = new Options();
    for (Arg arg : Arg.values()){
      createOpt(arg, arg.isParameterized(), arg.getHelpText(), options);
    }

    Options parameterOptionsForHelp = new Options();
    for (Object obj : options.getOptions()){
      Option o = (Option) obj;
      parameterOptionsForHelp.addOption(o);
    }

    options.addOptionGroup(commandGroup);

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption(Arg.HELP.first())) {
      printUsageAndExit(commandGroup, parameterOptionsForHelp);
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

  private static Command ensureOnlyOneCommand(CommandLine cmd){
    String foundCommand = null;
    for (Command c : Command.values()){
      if (cmd.hasOption(c.toString())){
        if (null == foundCommand) {
          foundCommand = c.toString();
        } else {
          throw new VeniceException("Can only specify one of --" + foundCommand + " and --" + c.toString());
        }
      }
    }
    return Command.getCommand(foundCommand, cmd);
  }

  private static MultiStoreResponse queryStoreList(CommandLine cmd) {
    boolean includeSystemStores = Boolean.parseBoolean(getOptionalArgument(cmd, Arg.INCLUDE_SYSTEM_STORES));
    return controllerClient.queryStoreList(includeSystemStores);
  }

  private static void queryStoreForKey(CommandLine cmd, String veniceUrl)
      throws Exception {
    String store = getRequiredArgument(cmd, Arg.STORE);
    String keyString = getRequiredArgument(cmd, Arg.KEY);
    String sslConfigFileStr = getOptionalArgument(cmd, Arg.VENICE_CLIENT_SSL_CONFIG_FILE);
    boolean isVsonStore = Boolean.parseBoolean(getOptionalArgument(cmd, Arg.VSON_STORE, "false"));
    Optional<String> sslConfigFile =
        Utils.isNullOrEmpty(sslConfigFileStr) ? Optional.empty() : Optional.of(sslConfigFileStr);
    printObject(QueryTool.queryStoreForKey(store, keyString, veniceUrl, isVsonStore, sslConfigFile));
  }

  private static void showSchemas(CommandLine cmd){
    String store = getRequiredArgument(cmd, Arg.STORE);
    SchemaResponse keySchema = controllerClient.getKeySchema(store);
    printObject(keySchema);
    MultiSchemaResponse valueSchemas = controllerClient.getAllValueSchema(store);
    printObject(valueSchemas);
  }

  private static void createNewStore(CommandLine cmd)
      throws Exception {
    String store = getRequiredArgument(cmd, Arg.STORE, Command.NEW_STORE);
    String keySchemaFile = getRequiredArgument(cmd, Arg.KEY_SCHEMA, Command.NEW_STORE);
    String keySchema = readFile(keySchemaFile);
    String valueSchemaFile = getRequiredArgument(cmd, Arg.VALUE_SCHEMA, Command.NEW_STORE);
    String valueSchema = readFile(valueSchemaFile);
    String owner = getOptionalArgument(cmd, Arg.OWNER, "");
    boolean isVsonStore = Utils.parseBooleanFromString(getOptionalArgument(cmd, Arg.VSON_STORE, "false"), "isVsonStore");
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

  private static void deleteStore(CommandLine cmd)
      throws IOException {
    String store = getRequiredArgument(cmd, Arg.STORE, Command.DELETE_STORE);
    verifyStoreExistence(store, true);
    TrackableControllerResponse response = controllerClient.deleteStore(store);
    printObject(response);
  }

  private static void emptyPush(CommandLine cmd)
      throws IOException {
    String store = getRequiredArgument(cmd, Arg.STORE, Command.EMPTY_PUSH);
    String pushId = getRequiredArgument(cmd, Arg.PUSH_ID, Command.EMPTY_PUSH);
    String storeSizeString = getRequiredArgument(cmd, Arg.STORE_SIZE, Command.EMPTY_PUSH);
    long storeSize = Utils.parseLongFromString(storeSizeString, Arg.STORE_SIZE.name());

    verifyStoreExistence(store, true);
    VersionCreationResponse response = controllerClient.emptyPush(store, pushId, storeSize);
    printObject(response);
  }

  private static void setEnableStoreWrites(CommandLine cmd, boolean enableWrites){
    String store = getRequiredArgument(cmd, Arg.STORE, Command.SET_VERSION);
    ControllerResponse response = controllerClient.enableStoreWrites(store, enableWrites);
    printSuccess(response);
  }

  private static void setEnableStoreReads(CommandLine cmd, boolean enableReads) {
    String store = getRequiredArgument(cmd, Arg.STORE, Command.SET_VERSION);
    ControllerResponse response = controllerClient.enableStoreReads(store, enableReads);
    printSuccess(response);
  }

  private static void setEnableStoreReadWrites(CommandLine cmd, boolean enableReadWrites) {
    String store = getRequiredArgument(cmd, Arg.STORE, Command.SET_VERSION);
    ControllerResponse response = controllerClient.enableStoreReadWrites(store, enableReadWrites);
    printSuccess(response);
  }

  private static void applyVersionToStore(CommandLine cmd){
    String storeName = getRequiredArgument(cmd, Arg.STORE, Command.SET_VERSION);
    String version = getRequiredArgument(cmd, Arg.VERSION, Command.SET_VERSION);
    int intVersion = Utils.parseIntFromString(version, Arg.VERSION.name());
    boolean versionExists = false;
    StoreResponse storeResponse = controllerClient.getStore(storeName);
    if (storeResponse.isError()){
      throw new VeniceException("Error querying versions for store: " + storeName + " -- " + storeResponse.getError());
    }
    int[] versionNumbers = storeResponse.getStore().getVersions().stream().mapToInt(v -> v.getNumber()).toArray();
    for(int v: versionNumbers) {
      if (v == intVersion){
        versionExists = true;
        break;
      }
    }
    if (!versionExists){
     throw new VeniceException("Version " + version + " does not exist for store " + storeName + ".  Store only has versions: " + Arrays.toString(versionNumbers));
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

  private static void integerParam(CommandLine cmd, Arg param, Consumer<Integer> setter, Set<Arg> argSet) {
    genericParam(cmd, param, s -> Utils.parseIntFromString(s, param.toString()), setter, argSet);
  }


  private static void longParam(CommandLine cmd, Arg param, Consumer<Long> setter, Set<Arg> argSet) {
    genericParam(cmd, param, s -> Utils.parseLongFromString(s, param.toString()), setter, argSet);
  }

  private static void booleanParam(CommandLine cmd, Arg param, Consumer<Boolean> setter, Set<Arg> argSet) {
    genericParam(cmd, param, s -> Utils.parseBooleanFromString(s, param.toString()), setter, argSet);
  }

  private static <TYPE> void genericParam(CommandLine cmd, Arg param, Function<String, TYPE> parser, Consumer<TYPE> setter,
      Set<Arg> argSet) {
    if (!argSet.contains(param)) {
      throw new VeniceException(" Argument does not exist in command doc: " + param);
    }
    String paramStr = getOptionalArgument(cmd, param);
    if (null != paramStr) {
      setter.accept(parser.apply(paramStr));
    }
  }

  private static void updateStore(CommandLine cmd) {
    UpdateStoreQueryParams params = getUpdateStoreQueryParams(cmd);

    String storeName = getRequiredArgument(cmd, Arg.STORE, Command.UPDATE_STORE);
    ControllerResponse response = controllerClient.updateStore(storeName, params);
    printSuccess(response);
  }

  @VisibleForTesting
  static UpdateStoreQueryParams getUpdateStoreQueryParams(CommandLine cmd) {
    Set<Arg> argSet = new HashSet<>(Arrays.asList(Command.UPDATE_STORE.getOptionalArgs()));
    argSet.addAll(new HashSet<>(Arrays.asList(Command.UPDATE_STORE.getRequiredArgs())));

    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    genericParam(cmd, Arg.OWNER, s -> s, p -> params.setOwner(p), argSet);
    integerParam(cmd, Arg.PARTITION_COUNT, p -> params.setPartitionCount(p), argSet);
    integerParam(cmd, Arg.VERSION, p -> params.setCurrentVersion(p), argSet);
    integerParam(cmd, Arg.LARGEST_USED_VERSION_NUMBER, p -> params.setLargestUsedVersionNumber(p), argSet);
    booleanParam(cmd, Arg.READABILITY, p -> params.setEnableReads(p), argSet);
    booleanParam(cmd, Arg.WRITEABILITY, p -> params.setEnableWrites(p), argSet);
    longParam(cmd, Arg.STORAGE_QUOTA, p -> params.setStorageQuotaInByte(p), argSet);
    booleanParam(cmd, Arg.HYBRID_STORE_OVERHEAD_BYPASS, p -> params.setHybridStoreOverheadBypass(p), argSet);
    longParam(cmd, Arg.READ_QUOTA, p -> params.setReadQuotaInCU(p), argSet);
    longParam(cmd, Arg.HYBRID_REWIND_SECONDS, p -> params.setHybridRewindSeconds(p), argSet);
    longParam(cmd, Arg.HYBRID_OFFSET_LAG, p -> params.setHybridOffsetLagThreshold(p), argSet);
    booleanParam(cmd, Arg.ACCESS_CONTROL, p -> params.setAccessControlled(p), argSet);
    genericParam(cmd, Arg.COMPRESSION_STRATEGY, s -> CompressionStrategy.valueOf(s), p -> params.setCompressionStrategy(p), argSet);
    booleanParam(cmd, Arg.CLIENT_DECOMPRESSION_ENABLED, p -> params.setClientDecompressionEnabled(p), argSet);
    booleanParam(cmd, Arg.CHUNKING_ENABLED, p -> params.setChunkingEnabled(p), argSet);
    booleanParam(cmd, Arg.SINGLE_GET_ROUTER_CACHE_ENABLED, p -> params.setSingleGetRouterCacheEnabled(p), argSet);
    booleanParam(cmd, Arg.BATCH_GET_ROUTER_CACHE_ENABLED, p -> params.setBatchGetRouterCacheEnabled(p), argSet);
    integerParam(cmd, Arg.BATCH_GET_LIMIT, p -> params.setBatchGetLimit(p), argSet);
    integerParam(cmd, Arg.NUM_VERSIONS_TO_PRESERVE, p -> params.setNumVersionsToPreserve(p), argSet);
    booleanParam(cmd, Arg.INCREMENTAL_PUSH_ENABLED, p -> params.setIncrementalPushEnabled(p), argSet);
    booleanParam(cmd, Arg.WRITE_COMPUTATION_ENABLED, p -> params.setWriteComputationEnabled(p), argSet);
    booleanParam(cmd, Arg.READ_COMPUTATION_ENABLED, p -> params.setReadComputationEnabled(p), argSet);
    integerParam(cmd, Arg.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOUR, p -> params.setBootstrapToOnlineTimeoutInHours(p), argSet);
    booleanParam(cmd, Arg.LEADER_FOLLOWER_MODEL_ENABLED, p -> params.setLeaderFollowerModel(p), argSet);
    genericParam(cmd, Arg.BACKUP_STRATEGY, s -> BackupStrategy.valueOf(s), p -> params.setBackupStrategy(p), argSet);
    booleanParam(cmd, Arg.AUTO_SCHEMA_REGISTER_FOR_PUSHJOB_ENABLED, p -> params.setAutoSchemaPushJobEnabled(p), argSet);
    booleanParam(cmd, Arg.AUTO_SUPERSET_SCHEMA_FOR_READ_COMPUTE_STORE_ENABLED,
        p -> params.setAutoSupersetSchemaEnabledFromReadComputeStore(p), argSet);

    /**
     * By default when SRE updates storage quota using AdminTool, we will set the bypass as true,
     * i.e. hybrid store storage quota will not be added overhead ratio automatically.
     */
    if (params.getStorageQuotaInByte().isPresent() && !params.getHybridStoreOverheadBypass().isPresent()) {
      params.setHybridStoreOverheadBypass(true);
    }
    return params;
  }

  private static void applyValueSchemaToStore(CommandLine cmd)
      throws Exception {
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

  private static void applyDerivedSchemaToStore(CommandLine cmd) throws Exception {
    String store = getRequiredArgument(cmd, Arg.STORE, Command.ADD_DERIVED_SCHEMA);
    String derivedSchemaFile = getRequiredArgument(cmd, Arg.DERIVED_SCHEMA, Command.ADD_DERIVED_SCHEMA);
    int valueSchemaId = Utils.parseIntFromString(getRequiredArgument(cmd, Arg.VALUE_SCHEMA_ID, Command.ADD_DERIVED_SCHEMA),
        "value schema id");

    String derivedSchemaStr = readFile(derivedSchemaFile);
    verifyValidSchema(derivedSchemaStr);
    SchemaResponse valueResponse = controllerClient.addDerivedSchema(store, valueSchemaId, derivedSchemaStr);
    if (valueResponse.isError()) {
      throw new VeniceException("Error updating store with schema: " + valueResponse.getError());
    }
    printObject(valueResponse);
  }

  private static void printStoreDescription(String storeName) {
    StoreResponse response = controllerClient.getStore(storeName);
    printObject(response);
  }

  private static void printStorageNodeList(){
    MultiNodeResponse nodeResponse = controllerClient.listStorageNodes();
    printObject(nodeResponse);
  }

  private static void printInstancesStatuses(){
    MultiNodesStatusResponse nodeResponse = controllerClient.listInstancesStatuses();
    printObject(nodeResponse);
  }

  private static void printStoresStatuses() {
    MultiStoreStatusResponse storeResponse = controllerClient.listStoresStatuses();
    printObject(storeResponse);
  }


  private static void printReplicaListForStoreVersion(CommandLine cmd){
    String store = getRequiredArgument(cmd, Arg.STORE, Command.REPLICAS_OF_STORE);
    int version = Utils.parseIntFromString(getRequiredArgument(cmd, Arg.VERSION, Command.REPLICAS_OF_STORE),
        Arg.VERSION.toString());
    MultiReplicaResponse response = controllerClient.listReplicas(store, version);
    printObject(response);
  }

  private static void printReplicaListForStorageNode(CommandLine cmd){
    String storageNodeId = getRequiredArgument(cmd, Arg.STORAGE_NODE);
    MultiReplicaResponse response = controllerClient.listStorageNodeReplicas(storageNodeId);
    printObject(response);
  }

  private static void isNodeRemovable(CommandLine cmd){
    String storageNodeId = getRequiredArgument(cmd, Arg.STORAGE_NODE);
    NodeStatusResponse response = controllerClient.isNodeRemovable(storageNodeId);
    printObject(response);
  }

  private static void addNodeIntoWhiteList(CommandLine cmd){
    String storageNodeId = getRequiredArgument(cmd, Arg.STORAGE_NODE);
    ControllerResponse response = controllerClient.addNodeIntoWhiteList(storageNodeId);
    printSuccess(response);
  }

  private static void removeNodeFromWhiteList(CommandLine cmd){
    String storageNodeId = getRequiredArgument(cmd, Arg.STORAGE_NODE);
    ControllerResponse response = controllerClient.removeNodeFromWhiteList(storageNodeId);
    printSuccess(response);
  }

  private static void removeNodeFromCluster(CommandLine cmd){
    String storageNodeId = getRequiredArgument(cmd, Arg.STORAGE_NODE);
    ControllerResponse response = controllerClient.removeNodeFromCluster(storageNodeId);
    printSuccess(response);
  }

  private static void enableThrottling(boolean enable) {
    ControllerResponse response = controllerClient.enableThrotting(enable);
    printSuccess(response);
  }

  private static void enableMaxCapacityProtection(boolean enable) {
    ControllerResponse response = controllerClient.enableMaxCapacityProtection(enable);
    printSuccess(response);
  }

  private static void enableQuotaRebalance(CommandLine cmd, boolean enable) {
    int expectedRouterCount = 0;
    if (!enable) {
      expectedRouterCount = Integer.valueOf(getRequiredArgument(cmd, Arg.EXPECTED_ROUTER_COUNT));
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

  private static void convertVsonSchemaAndExit(CommandLine cmd) throws IOException{
    String keySchemaStr = readFile(getRequiredArgument(cmd, Arg.KEY_SCHEMA));
    String valueSchemaStr = readFile(getRequiredArgument(cmd, Arg.VALUE_SCHEMA));

    System.out.println(
        String.format("{\n  \"Avro key schema\": \"%s\",\n  \"Avro value schema\": \"%s\"\n}",
            VsonAvroSchemaAdapter.parse(keySchemaStr).toString(),
            VsonAvroSchemaAdapter.parse(valueSchemaStr).toString()));

    System.exit(1);
  }

  private static void listBootstrappingVersions(CommandLine cmd) {
    MultiVersionStatusResponse response = controllerClient.listBootstrappingVersions();
    printObject(response);
  }

  private static void deleteKafkaTopic(CommandLine cmd) throws Exception {
    long startTime = System.currentTimeMillis();
    String kafkaBootstrapServer = getRequiredArgument(cmd, Arg.KAFKA_BOOTSTRAP_SERVERS);
    Properties properties = loadProperties(cmd, Arg.KAFKA_CONSUMER_CONFIG_FILE);
    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer);
    VeniceProperties veniceProperties = new VeniceProperties(properties);
    VeniceConsumerFactory veniceConsumerFactory = new VeniceAdminToolConsumerFactory(veniceProperties);
    String zkConnectionString = getRequiredArgument(cmd, Arg.KAFKA_ZOOKEEPER_CONNECTION_URL);
    int zkSessionTimeoutMs = 30 * Time.MS_PER_SECOND;
    int zkConnectionTimeoutMs = 60 * Time.MS_PER_SECOND;
    int kafkaTimeOut = 30 * Time.MS_PER_SECOND;
    int topicDeletionStatusPollingInterval = 2 * Time.MS_PER_SECOND;
    if (cmd.hasOption(Arg.KAFKA_OPERATION_TIMEOUT.toString())) {
      kafkaTimeOut = Integer.parseInt(getRequiredArgument(cmd, Arg.KAFKA_OPERATION_TIMEOUT)) * Time.MS_PER_SECOND;
    }
    TopicManager topicManager = new TopicManager(zkConnectionString, zkSessionTimeoutMs, zkConnectionTimeoutMs,
        kafkaTimeOut, topicDeletionStatusPollingInterval, 0l, veniceConsumerFactory);
    String topicName = getRequiredArgument(cmd, Arg.KAFKA_TOPIC_NAME);
    try {
      topicManager.ensureTopicIsDeletedAndBlock(topicName);
      long runTime = System.currentTimeMillis() - startTime;
      printObject("Topic '" + topicName + "' is deleted. Run time: " + runTime + " ms.");
    } catch (VeniceOperationAgainstKafkaTimedOut e) {
      printErrAndThrow(e, "Topic deletion timed out for: '" + topicName + "' after " + kafkaTimeOut + " ms.", null);
    }
  }

  private static void startMirrorMaker(CommandLine cmd) {
    MirrorMakerWrapper mirrorMaker = ServiceFactory.getKafkaMirrorMaker(
        getRequiredArgument(cmd, Arg.KAFKA_BOOTSTRAP_SERVERS),
        getRequiredArgument(cmd, Arg.KAFKA_BOOTSTRAP_SERVERS_DESTINATION),
        getRequiredArgument(cmd, Arg.KAFKA_ZOOKEEPER_CONNECTION_URL_DESTINATION),
        getRequiredArgument(cmd, Arg.KAFKA_TOPIC_WHITELIST),
        loadProperties(cmd, Arg.KAFKA_CONSUMER_CONFIG_FILE),
        loadProperties(cmd, Arg.KAFKA_PRODUCER_CONFIG_FILE));

    while (mirrorMaker.isRunning()) {
      Utils.sleep(1000);
    }

    mirrorMaker.close();
    System.out.println("Closed MM.");
  }

  private static void dumpAdminMessages(CommandLine cmd) {
    Properties consumerProperties = loadProperties(cmd, Arg.KAFKA_CONSUMER_CONFIG_FILE);
    List<DumpAdminMessages.AdminOperationInfo> adminMessages = DumpAdminMessages.dumpAdminMessages(
        getRequiredArgument(cmd, Arg.KAFKA_BOOTSTRAP_SERVERS),
        getRequiredArgument(cmd, Arg.CLUSTER),
        consumerProperties,
        Long.parseLong(getRequiredArgument(cmd, Arg.STARTING_OFFSET)),
        Integer.parseInt(getRequiredArgument(cmd, Arg.MESSAGE_COUNT))
    );
    printObject(adminMessages);
  }


  private static void dumpControlMessages(CommandLine cmd) {
    Properties consumerProps = loadProperties(cmd, Arg.KAFKA_CONSUMER_CONFIG_FILE);
    String kafkaUrl = getRequiredArgument(cmd, Arg.KAFKA_BOOTSTRAP_SERVERS);

    consumerProps.setProperty(BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
    consumerProps.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class.getName());
    consumerProps.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, KafkaValueSerializer.class.getName());

    String kafkaTopic = getRequiredArgument(cmd, Arg.KAFKA_TOPIC_NAME);
    int partitionNumber = Integer.parseInt(getRequiredArgument(cmd, Arg.KAFKA_TOPIC_PARTITION));
    int startingOffset = Integer.parseInt(getRequiredArgument(cmd, Arg.STARTING_OFFSET));
    int messageCount = Integer.parseInt(getRequiredArgument(cmd, Arg.MESSAGE_COUNT));

    new ControlMessageDumper(consumerProps, kafkaTopic, partitionNumber, startingOffset, messageCount).fetch().display();
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

    StoreResponse storeResponse = srcControllerClient.getStore(storeName);
    if (storeResponse.isError()) {
      printObject(storeResponse);
      return;
    } else {
      // Store migration should not be started already.
      if (storeResponse.getStore().isMigrating()) {
        System.err.println("ERROR: store " + storeName + " is migrating. Finish the current migration before starting a new one.");
        return;
      }
    }

    StoreMigrationResponse storeMigrationResponse = destControllerClient.migrateStore(storeName, srcClusterName);
    printObject(storeMigrationResponse);

    if (storeMigrationResponse.isError()) {
      System.err.println("ERROR: Store migration failed!");
      return;
    }

    // Logging to stderr means users can see it but it gets ignored
    // if you (for example) pipe the output to jq.
    // This maintains the ability to easily write scripts around the admin tool that do parsing of the output.
    System.err.println("\nThe migration request has been submitted successfully.\n"
        + "Make sure at least one version is online before deleting the original store.\n"
        + "If you terminate this process, the migration will continue and "
        + "you can verify the process by describing the store in the destination cluster.");

    // Progress monitor
    if (null != storeMigrationResponse.getChildControllerUrls()) {
      // This must be parent controller
      System.err.println("When complete, the \"cluster\" property of this store should become " + destClusterName);
      monitorMultiClusterAfterMigration(destControllerClient, storeMigrationResponse);
    } else {
      // Child controller
      System.err.println("When complete, this store should have at least one online version.");
      monitorSingleClusterAfterMigration(srcControllerClient, destControllerClient, storeMigrationResponse);
    }
  }

  private static void printMigrationStatus(ControllerClient controller, String storeName) {
    StoreInfo store = controller.getStore(storeName).getStore();

    System.err.println("\n" + controller.getClusterName() + "\t" + controller.getMasterControllerUrl());

    if (null == store) {
      System.err.println(storeName + " DOES NOT EXIST in this cluster " + controller.getClusterName());
    } else {
      System.err.println(storeName + " exists in this cluster " + controller.getClusterName());
      System.err.println("\t" + storeName + ".isMigrating = " + store.isMigrating());
      System.err.println("\t" + storeName + ".largestUsedVersion = " + store.getLargestUsedVersionNumber());
      System.err.println("\t" + storeName + ".currentVersion = " + store.getCurrentVersion());
      System.err.println("\t" + storeName + ".versions = ");
      store.getVersions().stream().forEach(version -> System.err.println("\t\t" + version.toString()));
    }

    System.err.println("\t" + storeName + " belongs to cluster " + controller.discoverCluster(storeName).getCluster() + " according to cluster discovery");
  }

  private static void checkMigrationStatus(CommandLine cmd) {
    String veniceUrl = getRequiredArgument(cmd, Arg.URL);
    String storeName = getRequiredArgument(cmd, Arg.STORE);
    String srcClusterName = getRequiredArgument(cmd, Arg.CLUSTER_SRC);
    String destClusterName = getRequiredArgument(cmd, Arg.CLUSTER_DEST);
    if (srcClusterName.equals(destClusterName)) {
      throw new VeniceException("Source cluster and destination cluster cannot be the same!");
    }

    ControllerClient srcControllerClient = new ControllerClient(srcClusterName, veniceUrl, sslFactory);
    ControllerClient destControllerClient = new ControllerClient(destClusterName, veniceUrl, sslFactory);

    List<String> childControllerUrls = srcControllerClient.listChildControllers(srcClusterName).getChildControllerUrls();
    if (childControllerUrls == null) {
      // This is a controller in single datacenter setup
      printMigrationStatus(srcControllerClient, storeName);
      printMigrationStatus(destControllerClient, storeName);
    } else {
      // This is a parent controller
      System.err.println("\n=================== Parent Controllers ====================");
      printMigrationStatus(srcControllerClient, storeName);
      printMigrationStatus(destControllerClient, storeName);

      ControllerClient[] srcChildControllerClients = createControllerClients(srcClusterName, childControllerUrls);
      ControllerClient[] destChildControllerClients = createControllerClients(destClusterName, childControllerUrls);

      for (int i = 0; i < childControllerUrls.size(); i++) {
        System.err.println("\n\n=================== Child Datacenter " + i + " ====================");
        printMigrationStatus(srcChildControllerClients[i], storeName);
        printMigrationStatus(destChildControllerClients[i], storeName);
      }
    }
  }

  private static boolean isClonedStoreOnline(ControllerClient srcControllerClient, ControllerClient destControllerClient,
      String storeName) {
    StoreInfo srcStore = srcControllerClient.getStore(storeName).getStore();
    if (null == srcStore) {
      throw new VeniceException("Store " + storeName + " does not exist in the original cluster!");
    }

    StoreInfo destStore = destControllerClient.getStore(storeName).getStore();
    if (null == destStore) {
      System.err.println("WARN: Cloned store has not been created in the destination cluster!");
      return false;
    }

    List<Version> srcVersions = srcStore.getVersions();
    List<Version> destVersions = destStore.getVersions();

    int srcLatestOnlineVersion = getLatestOnlineVersionNum(srcVersions);
    int destLatestOnlineVersion = getLatestOnlineVersionNum(destVersions);

    System.err.println(destControllerClient.getMasterControllerUrl());
    if (srcLatestOnlineVersion == -1) {
      System.err.println("Original store doesn't have online version");
    } else {
      destVersions.stream().forEach(System.err::println);
    }

    return destLatestOnlineVersion >= srcLatestOnlineVersion;
  }

  private static void monitorSingleClusterAfterMigration(ControllerClient srcControllerClient,
      ControllerClient destControllerClient, StoreMigrationResponse storeMigrationResponse) {

    String srcClusterName = storeMigrationResponse.getSrcClusterName();
    String destClusterName = storeMigrationResponse.getCluster();
    String storeName = storeMigrationResponse.getName();

    boolean isOnline = false;
    while (!isOnline) {
      Utils.sleep(5000);
      System.err.println();
      System.err.println(new java.util.Date());

      isOnline = isClonedStoreOnline(srcControllerClient, destControllerClient, storeName);
    }

    // Cluster discovery info should be updated accordingly
    System.err.println("\nCloned store is ready in dest cluster " + destClusterName + ". Checking cluster discovery info ...");
    Utils.sleep(15000);
    String clusterDiscovered = destControllerClient.discoverCluster(storeName).getCluster();
    if (destClusterName.equals(clusterDiscovered)) {
      System.err.println("\nThe cloned store in " + destClusterName + " has bootstrapped successfully.");
      System.err.println("Please verify that the original store in " + srcClusterName
          + " has zero read activity, and then delete it.");
    } else {
      System.err.println("\nWARN: The cloned store in " + destClusterName
          + " has bootstrapped successfully. BUT CLUSTER DISCOVERY INFORMATION IS NOT YET UPDATED!");
      System.err.println("Expected cluster discovery result: " + destClusterName);
      System.err.println("Actual cluster discovery result: " + clusterDiscovered);
    }
  }

  private static ControllerClient[] createControllerClients(String clusterName, List<String> controllerUrls) {
    int numChildDatacenters = controllerUrls.size();
    ControllerClient[] controllerClients = new ControllerClient[numChildDatacenters];
    for (int i = 0; i < numChildDatacenters; i++) {
      String controllerUrl = controllerUrls.get(i);
      controllerClients[i] = new ControllerClient(clusterName, controllerUrl, sslFactory);
    }
    return controllerClients;
  }

  private static void monitorMultiClusterAfterMigration(ControllerClient destControllerClient,
      StoreMigrationResponse storeMigrationResponse) {

    String srcClusterName = storeMigrationResponse.getSrcClusterName();
    String destClusterName = storeMigrationResponse.getCluster();
    String storeName = storeMigrationResponse.getName();
    List<String> childControllerUrls = storeMigrationResponse.getChildControllerUrls();
    int numChildDatacenters = childControllerUrls.size();

    ControllerClient[] srcChildControllerClients = createControllerClients(srcClusterName, childControllerUrls);
    ControllerClient[] destChildControllerClients = createControllerClients(destClusterName, childControllerUrls);

    boolean isOnline = false;
    while (!isOnline) {
      Utils.sleep(5000);
      System.err.println();
      System.err.println(new java.util.Date());

      int onlineChildCount = 0;

      for (int i = 0; i < numChildDatacenters; i++) {
        ControllerClient srcChildController = srcChildControllerClients[i];
        ControllerClient destChildController = destChildControllerClients[i];

        if (isClonedStoreOnline(srcChildController, destChildController, storeName)) {
          onlineChildCount++;
        }
      }

      if (onlineChildCount == numChildDatacenters) {
        isOnline = true;
      }
    }

    // All child clusters are online, the parent cluster should also have update cluster discovery info
    System.err.println("\nCloned store is ready in dest cluster " + destClusterName + ". Checking cluster discovery info ...");
    String clusterDiscovered = null;
    for (int i = 0; i < 60; i++) {
      Utils.sleep(3000);  // Polls every 3 seconds for the next 3 minutes
      clusterDiscovered = destControllerClient.discoverCluster(storeName).getCluster();

      if (clusterDiscovered.equals(destClusterName)) {
        System.err.println(
            "\nStore migration complete.\n" + "Please make sure read activity to the original cluster is zero, "
                + "and then delete the original store using admin-tool command --end-migration");
        return;
      }
    }

    System.err.println("\nWARN: Store migration complete in child clusters but not in parent yet. "
        + "Make sure cluster discovery in parent controller gets updated, "
        + "then confirm the original store has zero read activity and delete it using --end-migration command");

    System.err.println("\nExpected cluster discovery result: " + destClusterName);
    System.err.println("Actual cluster discovery result in parent: " + clusterDiscovered);
    for (int i = 0; i < numChildDatacenters; i++) {
      ControllerClient destChildController = destChildControllerClients[i];
      String childControllerUrl = destChildController.getMasterControllerUrl();
      String clusterDiscoveredInChild = destChildController.discoverCluster(storeName).getCluster();
      System.err.println("Actual cluster discovery result in " + childControllerUrl + " : " + clusterDiscoveredInChild);
    }
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
  public static void abortMigration(String veniceUrl, String storeName, String srcClusterName, String destClusterName,
      boolean force, boolean[] promptsOverride) {
    if (srcClusterName.equals(destClusterName)) {
      throw new VeniceException("Source cluster and destination cluster cannot be the same!");
    }
    boolean terminate = false;

    ControllerClient srcControllerClient = new ControllerClient(srcClusterName, veniceUrl, sslFactory);
    ControllerClient destControllerClient = new ControllerClient(destClusterName, veniceUrl, sslFactory);

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
        System.err.println("WARNING: Either store migration has completed, or the internal states are messed up.\n"
            + "You can force execute this command with --" + Arg.FORCE.toString() + " / -" + Arg.FORCE.first()
            + ", but make sure your src and dest cluster names are correct.");
        return;
      }
    }

    // Reset original store, storeConfig, and cluster discovery
    if (promptsOverride.length > 1) {
      terminate = !promptsOverride[1];
    } else {
      terminate = !userGivesPermission("Next step is to reset store migration flag, storeConfig and cluster"
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
      terminate = !userGivesPermission("Next step is to delete the cloned store in dest cluster "
          + destClusterName + ". " + storeName + " in " + destClusterName + " will be deleted irreversibly. Do you want to proceed?");
    }
    if (terminate) {
      return;
    }
    if (destControllerClient.getStore(storeName).getStore() == null) {
      // Cloned store has not been created
      // Directly delete cloned stores in children datacenters if two layer setup, otherwise skip
      List<String> childControllerUrls = abortMigrationResponse.getChildControllerUrls();
      if (childControllerUrls != null) {
        int numChildDatacenters = childControllerUrls.size();
        int deletedStoreCount = 0;
        ControllerClient[] destChildControllerClients = createControllerClients(destClusterName, childControllerUrls);

        // The abort migration logic could not handle abort migration test 1 (aka abort immediately)
        // in TestStoreMigration#testAbortMigrationMultiDatacenter very well due to race conditon
        // The following wait logic is introduced as a workaround for this edge case.
        // Wait until cloned store being created in dest cluster, then delete.
        while (deletedStoreCount < numChildDatacenters) {
          for (int i = 0; i < numChildDatacenters; i++) {
            ControllerClient destChildController = destChildControllerClients[i];
            if (destChildController.getStore(storeName).getStore() != null) {
              System.err.println("Deleting cloned store " + storeName + " in " + destChildController.getMasterControllerUrl() + " ...");
              destChildController.updateStore(storeName, new UpdateStoreQueryParams().setEnableReads(false).setEnableWrites(false));
              TrackableControllerResponse deleteResponse = destChildController.deleteStore(storeName);
              printObject(deleteResponse);
              deletedStoreCount++;
            }
          }
          System.err.println("Deleted cloned store in " + deletedStoreCount + "/" + numChildDatacenters + " child datacenters.");
          Utils.sleep(3000);
        }
      }
    } else {
      // Delete cloned store in (parent) dest controller
      System.err.println("Deleting cloned store " + storeName + " in " + destControllerClient.getMasterControllerUrl() + " ...");
      destControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setEnableReads(false).setEnableWrites(false));
      TrackableControllerResponse deleteResponse = destControllerClient.deleteStore(storeName);
      printObject(deleteResponse);
    }


    // Cluster discovery should point to original cluster
    discoveryResponse = srcControllerClient.discoverCluster(storeName);
    if (!discoveryResponse.getCluster().equals(srcClusterName)) {
      System.err.println("ERROR: Incorrect cluster discovery result");
    }

    // Dest cluster(s) should not contain the cloned store
    // This should not happen but in case it does, migration monitor probably has just created the cloned store in parent
    // while the admin-tool trying to delete the cloned store in child datacenters.
    if (destControllerClient.getStore(storeName).getStore() != null) {
      System.err.println("ERROR: Dest cluster " + destClusterName + " " + destControllerClient.getMasterControllerUrl()
          + " still contains store " + storeName + ". Possibly caused by some race condition.\n"
          + " Use --migration-status to check the current status and delete the cloned store in dest cluster when safe.\n"
          + " Make sure admin channels don't get stuck.");
    }
  }

  private static boolean userGivesPermission(String prompt) {
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

    // Make sure destClusternName does agree with the cluster discovery result
    String clusterDiscovered = destControllerClient.discoverCluster(storeName).getCluster();
    if (!clusterDiscovered.equals(destClusterName)) {
      System.err.println("ERROR: store " + storeName + " belongs to cluster " + clusterDiscovered
          + ", which is different from the src cluster name " + destClusterName + " in your command!");
      return;
    }

    // Skip original store deletion if it has already been deleted
    if (null != srcControllerClient.getStore(storeName).getStore()) {
      // Delete original store
      srcControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setEnableReads(false).setEnableWrites(false));
      TrackableControllerResponse deleteResponse = srcControllerClient.deleteStore(storeName);
      printObject(deleteResponse);
      if (deleteResponse.isError()) {
        System.err.println("ERROR: failed to delete store " + storeName + " in the original cluster " + srcClusterName);
        return;
      }
    }

    // Reset migration flag
    ControllerResponse controllerResponse =
        destControllerClient.updateStore(storeName, new UpdateStoreQueryParams().setStoreMigration(false));
    printObject(controllerResponse);
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

  private static void printUsageAndExit(OptionGroup commandGroup, Options options){

    /* Commands */
    String command = "java -jar " + new java.io.File(AdminTool.class.getProtectionDomain()
        .getCodeSource()
        .getLocation()
        .getPath())
        .getName();
    new HelpFormatter().printHelp(command + " --<command> [parameters]\n\nCommands:",
        new Options().addOptionGroup(commandGroup));

    /* Parameters */
    new HelpFormatter().printHelp("Parameters: ", options);

    /* Examples */
    System.out.println("\nExamples:");
    Command[] commands = Command.values();
    Arrays.sort(commands, Command.commandComparator);
    for (Command c : commands){
      StringJoiner exampleArgs = new StringJoiner(" ");
      for (Arg a : c.getRequiredArgs()){
        exampleArgs.add("--" + a.toString());
        exampleArgs.add("<" + a.toString() + ">");
      }
      for (Arg a : c.getOptionalArgs()){
        exampleArgs.add("[--" + a.toString());
        exampleArgs.add("<" + a.toString() + ">]");
      }

      System.out.println(command + " --" + c.toString() + " " + exampleArgs.toString());
    }
    System.exit(1);
  }

  private static String getRequiredArgument(CommandLine cmd, Arg arg){
    return getRequiredArgument(cmd, arg, "");
  }
  private static String getRequiredArgument(CommandLine cmd, Arg arg, Command command){
    return getRequiredArgument(cmd, arg, "when using --" + command.toString());
  }

  private static String getRequiredArgument(CommandLine cmd, Arg arg, String errorClause){
    if (!cmd.hasOption(arg.first())){
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
      try {
        properties.load(new FileInputStream(configFilePath));
      } catch (IOException e) {
        throw new VeniceException("Cannot read file: " + configFilePath + " specified by: " + arg.toString());
      }
    }
    return properties;
  }

  private static void verifyStoreExistence(String storename, boolean desiredExistence){
    MultiStoreResponse storeResponse = controllerClient.queryStoreList(true);
    if (storeResponse.isError()){
      throw new VeniceException("Error verifying store exists: " + storeResponse.getError());
    }
    boolean storeExists = false;
    for (String s : storeResponse.getStores()) {
      if (s.equals(storename)){
        storeExists = true;
        break;
      }
    }
    if (storeExists != desiredExistence) {
      throw new VeniceException("Store " + storename +
          (storeExists ? " already exists" : " does not exist"));
    }
  }

  private static void verifyValidSchema(String schema) throws Exception {
    try {
      Schema.parse(schema);
    } catch (Exception e){
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
    int version = Integer.valueOf(getRequiredArgument(cmd, Arg.VERSION, Command.DELETE_OLD_VERSION));
    VersionResponse response = controllerClient.deleteOldVersion(store, version);
    printObject(response);
  }

  private static void getExecution(CommandLine cmd) {
    long executionId = Long.valueOf(getRequiredArgument(cmd, Arg.EXECUTION, Command.GET_EXECUTION));
    AdminCommandExecutionResponse response = controllerClient.getAdminCommandExecution(executionId);
    printObject(response);
  }


  private static void printErrAndExit(String err) {
    Map<String, String> errMap = new HashMap<>();
    printErrAndExit(err, errMap);
  }

  private static void createOpt(Arg name, boolean hasArg, String help, Options options){
    options.addOption(new Option(name.first(), name.toString(), hasArg, help));
  }

  private static void createCommandOpt(Command command, OptionGroup group){
    group.addOption(
        OptionBuilder
            .withLongOpt(command.toString())
            .withDescription(command.getDesc())
            .create()
    );
  }

  static String readFile(String path) throws IOException {
    String fullPath = path.replace("~", System.getProperty("user.home"));
    byte[] encoded = Files.readAllBytes(Paths.get(fullPath));
    return new String(encoded, StandardCharsets.UTF_8).trim();
  }

  ///// Print Output ////
  private static void printObject(Object response) {
    printObject(response, System.out::print);
  }
  protected static void printObject(Object response, Consumer<String> printFunction){
    try {
      if (fieldsToDisplay.size() == 0){
        printFunction.accept(jsonWriter.writeValueAsString(response));
      } else { // Only display specified keys
        ObjectMapper mapper = new ObjectMapper();
        ObjectWriter plainJsonWriter = mapper.writer();
        String jsonString = plainJsonWriter.writeValueAsString(response);
        Map<String, Object> printMap = mapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {});
        Map<String, Object> filteredPrintMap = new HashMap<>();
        printMap.entrySet().stream().filter(entry -> fieldsToDisplay.contains(entry.getKey())).forEach(entry -> {
          filteredPrintMap.put(entry.getKey(), entry.getValue());
            });
        printFunction.accept(jsonWriter.writeValueAsString(filteredPrintMap));
      }
      printFunction.accept("\n");
    } catch (IOException e) {
      printFunction.accept("{\"" + ERROR + "\":\"" + e.getMessage() + "\"}");
      System.exit(1);
    }
  }

  static void printSuccess(ControllerResponse response){
    if (response.isError()){
      printErrAndExit(response.getError());
    } else {
      System.out.println("{\"" + STATUS + "\":\"" + SUCCESS + "\"}");
    }
  }

  private static void printErrAndExit(String errorMessage, Map<String, String> customMessages) {
    printErr(errorMessage, customMessages);
    System.exit(1);
  }

  private static void printErrAndThrow(Exception e, String errorMessage, Map<String, String> customMessages) throws Exception {
    printErr(errorMessage, customMessages);
    throw e;
  }

  private static void printErr(String errorMessage, Map<String, String> customMessages) {
    Map<String, String> errMap = new HashMap<>();
    if (customMessages != null) {
      for (Map.Entry<String, String> messagePair : customMessages.entrySet()) {
        errMap.put(messagePair.getKey(), messagePair.getValue());
      }
    }
    if (errMap.keySet().contains(ERROR)){
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
}
