package com.linkedin.venice;

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
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.kafka.clients.CommonClientConfigs;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.type.TypeReference;

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

  public static void main(String args[])
      throws Exception {

    /* Command Options are split up for help text formatting, see printUsageAndExit() */
    Options options = new Options();
    OptionGroup commandGroup = new OptionGroup();
    for (Command c : Command.values()){
      createCommandOpt(c, commandGroup);
    }

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

    try {
      if (cmd.hasOption(Arg.HELP.first())) {
        printUsageAndExit(commandGroup, parameterOptionsForHelp);
      } else if (cmd.hasOption(Command.CONVERT_VSON_SCHEMA.toString())) {
        convertVsonSchemaAndExit(cmd);
      }

      Command foundCommand = ensureOnlyOneCommand(cmd);

      // Variables used within the switch case need to be defined in advance
      String routerHosts = null, clusterName, storeName, versionString, topicName;
      int version;
      MultiStoreResponse storeResponse;
      ControllerResponse response;

      if (Arrays.stream(foundCommand.getRequiredArgs()).anyMatch(arg -> arg.equals(Arg.URL)) &&
          Arrays.stream(foundCommand.getRequiredArgs()).anyMatch(arg -> arg.equals(Arg.CLUSTER))) {
        routerHosts = getRequiredArgument(cmd, Arg.URL);
        clusterName = getRequiredArgument(cmd, Arg.CLUSTER);
        controllerClient = new ControllerClient(clusterName, routerHosts);
      }

      switch (foundCommand) {
        case LIST_STORES:
          storeResponse = controllerClient.queryStoreList();
          printObject(storeResponse);
          break;
        case DESCRIBE_STORE:
          storeName = getRequiredArgument(cmd, Arg.STORE, Command.DESCRIBE_STORE);
          for (String store : storeName.split(",")) {
            printStoreDescription(store);
          }
          break;
        case DESCRIBE_STORES:
          storeResponse = controllerClient.queryStoreList();
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
          response = controllerClient.skipAdminMessage(offset);
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
          queryStoreForKey(cmd, routerHosts);
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
        default:
          StringJoiner availableCommands = new StringJoiner(", ");
          for (Command c : Command.values()){
            availableCommands.add("--" + c.toString());
          }
          throw new VeniceException("Must supply one of the following commands: " + availableCommands.toString());
      }
    } catch (VeniceException e){
      printErrAndExit(e.getMessage());
    }
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
    return Command.getCommand(foundCommand);
  }

  private static void queryStoreForKey(CommandLine cmd, String routerHosts)
      throws Exception {
    String store = getRequiredArgument(cmd, Arg.STORE);
    String keyString = getRequiredArgument(cmd, Arg.KEY);
    String sslConfigFileStr = getOptionalArgument(cmd, Arg.VENICE_CLIENT_SSL_CONFIG_FILE);
    boolean isVsonStore = Boolean.parseBoolean(getOptionalArgument(cmd, Arg.VSON_STORE, "false"));
    Optional<String> sslConfigFile =
        Utils.isNullOrEmpty(sslConfigFileStr) ? Optional.empty() : Optional.of(sslConfigFileStr);
    printObject(QueryTool.queryStoreForKey(store, keyString, routerHosts, isVsonStore, sslConfigFile));
  }

  private static void showSchemas(CommandLine cmd){
    String store = getRequiredArgument(cmd, Arg.STORE);
    SchemaResponse keySchema = controllerClient.getKeySchema(store);
    printObject(keySchema);
    MultiSchemaResponse valueSchemas = controllerClient.getAllValueSchema(store);
    printObject(valueSchemas);
  }

  private static void createNewStore(CommandLine cmd)
      throws IOException {
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

  private static void integerParam(CommandLine cmd, Arg param, Consumer<Integer> setter) {
    genericParam(cmd, param, s -> Utils.parseIntFromString(s, param.toString()), setter);
  }


  private static void longParam(CommandLine cmd, Arg param, Consumer<Long> setter) {
    genericParam(cmd, param, s -> Utils.parseLongFromString(s, param.toString()), setter);
  }

  private static void booleanParam(CommandLine cmd, Arg param, Consumer<Boolean> setter) {
    genericParam(cmd, param, s -> Utils.parseBooleanFromString(s, param.toString()), setter);
  }

  private static <TYPE> void genericParam(CommandLine cmd, Arg param, Function<String, TYPE> parser, Consumer<TYPE> setter) {
    String paramStr = getOptionalArgument(cmd, param);
    if (null != paramStr) {
      setter.accept(parser.apply(paramStr));
    }
  }

  private static void updateStore(CommandLine cmd) {
    String storeName = getRequiredArgument(cmd, Arg.STORE, Command.UPDATE_STORE);
    UpdateStoreQueryParams params = new UpdateStoreQueryParams();
    genericParam(cmd, Arg.OWNER, s -> s, p -> params.setOwner(p));
    integerParam(cmd, Arg.PARTITION_COUNT, p -> params.setPartitionCount(p));
    integerParam(cmd, Arg.VERSION, p -> params.setCurrentVersion(p));
    integerParam(cmd, Arg.LARGEST_USED_VERSION_NUMBER, p -> params.setLargestUsedVersionNumber(p));
    booleanParam(cmd, Arg.READABILITY, p -> params.setEnableReads(p));
    booleanParam(cmd, Arg.WRITEABILITY, p -> params.setEnableWrites(p));
    longParam(cmd, Arg.STORAGE_QUOTA, p -> params.setStorageQuotaInByte(p));
    longParam(cmd, Arg.READ_QUOTA, p -> params.setReadQuotaInCU(p));
    longParam(cmd, Arg.HYBRID_REWIND_SECONDS, p -> params.setHybridRewindSeconds(p));
    longParam(cmd, Arg.HYBRID_OFFSET_LAG, p -> params.setHybridOffsetLagThreshold(p));
    booleanParam(cmd, Arg.ACCESS_CONTROL, p -> params.setAccessControlled(p));
    genericParam(cmd, Arg.COMPRESSION_STRATEGY, s -> CompressionStrategy.valueOf(s), p -> params.setCompressionStrategy(p));
    booleanParam(cmd, Arg.CHUNKING_ENABLED, p -> params.setChunkingEnabled(p));
    booleanParam(cmd, Arg.SINGLE_GET_ROUTER_CACHE_ENABLED, p -> params.setSingleGetRouterCacheEnabled(p));
    booleanParam(cmd, Arg.BATCH_GET_ROUTER_CACHE_ENABLED, p -> params.setBatchGetRouterCacheEnabled(p));
    integerParam(cmd, Arg.BATCH_GET_LIMIT, p -> params.setBatchGetLimit(p));
    integerParam(cmd, Arg.NUM_VERSIONS_TO_PRESERVE, p -> params.setNumVersionsToPreserve(p));
    booleanParam(cmd, Arg.INCREMENTAL_PUSH_ENABLED, p -> params.setIncrementalPushEnabled(p));

    ControllerResponse response = controllerClient.updateStore(storeName, params);

    printSuccess(response);
  }

  private static void applyValueSchemaToStore(CommandLine cmd)
      throws IOException {
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

  private static void deleteKafkaTopic(CommandLine cmd) {
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
    if (cmd.hasOption(Arg.KAFKA_OPERATION_TIMEOUT.toString())) {
      kafkaTimeOut = Integer.parseInt(getRequiredArgument(cmd, Arg.KAFKA_OPERATION_TIMEOUT)) * Time.MS_PER_SECOND;
    }
    TopicManager topicManager = new TopicManager(zkConnectionString, zkSessionTimeoutMs, zkConnectionTimeoutMs, kafkaTimeOut, veniceConsumerFactory);
    String topicName = getRequiredArgument(cmd, Arg.KAFKA_TOPIC_NAME);
    try {
      topicManager.ensureTopicIsDeletedAndBlock(topicName);
      long runTime = System.currentTimeMillis() - startTime;
      printObject("Topic '" + topicName + "' is deleted. Run time: " + runTime + " ms.");
    } catch (VeniceOperationAgainstKafkaTimedOut e) {
      printErrAndExit("Topic deletion timed out for: '" + topicName + "' after " + kafkaTimeOut + " ms.");
    }
  }

  private static void startMirrorMaker(CommandLine cmd) {
    Properties consumerProps = loadProperties(cmd, Arg.KAFKA_CONSUMER_CONFIG_FILE);
    Properties producerProps = loadProperties(cmd, Arg.KAFKA_PRODUCER_CONFIG_FILE);
    MirrorMakerWrapper mirrorMakerWrapper = ServiceFactory.getKafkaMirrorMaker(
        getRequiredArgument(cmd, Arg.KAFKA_ZOOKEEPER_CONNECTION_URL_SOURCE),
        getRequiredArgument(cmd, Arg.KAFKA_ZOOKEEPER_CONNECTION_URL_DESTINATION),
        getRequiredArgument(cmd, Arg.KAFKA_BOOTSTRAP_SERVERS_DESTINATION),
        getRequiredArgument(cmd, Arg.KAFKA_TOPIC_WHITELIST),
        consumerProps,
        producerProps);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        mirrorMakerWrapper.close();
        System.out.println("Closed MM.");
      }
    });
    while (mirrorMakerWrapper.isRunning()) {
      Utils.sleep(1000);
    }
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
    MultiStoreResponse storeResponse = controllerClient.queryStoreList();
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

  private static void verifyValidSchema(String schema) {
    try {
      Schema.parse(schema);
    } catch (Exception e){
      Map<String, String> errMap = new HashMap<>();
      errMap.put("schema", schema);
      printErrAndExit("Invalid Schema: " + e.getMessage(), errMap);
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
    Map<String, String> errMap = new HashMap<>();
    for (Map.Entry<String, String> messagePair : customMessages.entrySet()){
      errMap.put(messagePair.getKey(), messagePair.getValue());
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
    System.exit(1);
  }

}
