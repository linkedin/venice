package com.linkedin.venice;

import com.linkedin.venice.client.store.QueryTool;
import com.linkedin.venice.client.exceptions.VeniceClientException;
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
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.controllerapi.routes.AdminCommandExecutionResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import com.linkedin.venice.utils.Utils;
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
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
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
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.type.TypeReference;


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
      throws ParseException, IOException, InterruptedException, ExecutionException, VeniceClientException {

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


      //cmd = parser.parse(options, args);

      ensureOnlyOneCommand(cmd);

      String routerHosts = getRequiredArgument(cmd, Arg.URL);
      String clusterName = getRequiredArgument(cmd, Arg.CLUSTER);

      controllerClient = new ControllerClient(clusterName, routerHosts);

      if (cmd.hasOption(Arg.FLAT_JSON.toString())){
        jsonWriter = new ObjectMapper().writer();
      }
      if (cmd.hasOption(Arg.FILTER_JSON.toString())){
        fieldsToDisplay = Arrays.asList(
            cmd.getOptionValue(Arg.FILTER_JSON.first()).split(","));
      }

      if (cmd.hasOption(Command.LIST_STORES.toString())){
        MultiStoreResponse storeResponse = controllerClient.queryStoreList();
        printObject(storeResponse);
      } else if (cmd.hasOption(Command.DESCRIBE_STORE.toString())){
        String storeName = getRequiredArgument(cmd, Arg.STORE, Command.DESCRIBE_STORE);
        for (String store : storeName.split(",")) {
          printStoreDescription(store);
        }
      } else if (cmd.hasOption(Command.DESCRIBE_STORES.toString())){
        MultiStoreResponse storeResponse = controllerClient.queryStoreList();
        for (String store : storeResponse.getStores()) {
          printStoreDescription(store);
        }
      } else if (cmd.hasOption(Command.JOB_STATUS.toString())) {
        String storeName = getRequiredArgument(cmd, Arg.STORE, Command.JOB_STATUS);
        String versionString = getRequiredArgument(cmd, Arg.VERSION, Command.JOB_STATUS);
        int version = Integer.parseInt(versionString);
        String topicName = Version.composeKafkaTopic(storeName, version);
        JobStatusQueryResponse jobStatus = controllerClient.queryJobStatus(topicName);
        printObject(jobStatus);
      } else if (cmd.hasOption(Command.KILL_JOB.toString())){
        String storeName = getRequiredArgument(cmd, Arg.STORE, Command.KILL_JOB);
        String versionString = getRequiredArgument(cmd, Arg.VERSION, Command.KILL_JOB);
        int version = Integer.parseInt(versionString);
        String topicName = Version.composeKafkaTopic(storeName, version);
        ControllerResponse response = controllerClient.killOfflinePushJob(topicName);
        printObject(response);
      } else if (cmd.hasOption(Command.SKIP_ADMIN.toString())){
        String offset = getRequiredArgument(cmd, Arg.OFFSET, Command.SKIP_ADMIN);
        ControllerResponse response = controllerClient.skipAdminMessage(offset);
        printObject(response);
      } else if (cmd.hasOption(Command.NEW_STORE.toString())) {
        createNewStore(cmd);
      }else if (cmd.hasOption(Command.DELETE_STORE.toString())) {
        deleteStore(cmd);
      } else if (cmd.hasOption(Command.EMPTY_PUSH.toString())) {
        emptyPush(cmd);
      } else if (cmd.hasOption(Command.DISABLE_STORE_WRITE.toString())) {
        setEnableStoreWrites(cmd, false);
      } else if (cmd.hasOption(Command.ENABLE_STORE_WRITE.toString())){
        setEnableStoreWrites(cmd, true);
      } else if (cmd.hasOption(Command.DISABLE_STORE_READ.toString())) {
        setEnableStoreReads(cmd, false);
      } else if (cmd.hasOption(Command.ENABLE_STORE_READ.toString())) {
        setEnableStoreReads(cmd, true);
      } else if (cmd.hasOption(Command.DISABLE_STORE.toString())) {
        setEnableStoreReadWrites(cmd, false);
      } else if (cmd.hasOption(Command.ENABLE_STORE.toString())) {
        setEnableStoreReadWrites(cmd, true);
      } else if (cmd.hasOption(Command.DELETE_ALL_VERSIONS.toString())) {
        deleteAllVersions(cmd);
      } else if (cmd.hasOption(Command.DELETE_OLD_VERSION.toString())) {
        deleteOldVersion(cmd);
      } else if (cmd.hasOption(Command.SET_VERSION.toString())) {
        applyVersionToStore(cmd);
      } else if (cmd.hasOption(Command.SET_OWNER.toString())) {
        setStoreOwner(cmd);
      } else if (cmd.hasOption(Command.SET_PARTITION_COUNT.toString())) {
        setStorePartition(cmd);
      } else if (cmd.hasOption(Command.UPDATE_STORE.toString())) {
        updateStore(cmd);
      } else if (cmd.hasOption(Command.ADD_SCHEMA.toString())){
        applyValueSchemaToStore(cmd);
      } else if (cmd.hasOption(Command.LIST_STORAGE_NODES.toString())) {
        printStorageNodeList();
      } else if (cmd.hasOption(Command.CLUSTER_HEALTH_INSTANCES.toString())) {
        printInstancesStatuses();
      }  else if (cmd.hasOption(Command.CLUSTER_HEALTH_STORES.toString())) {
        printStoresStatuses();
      } else if (cmd.hasOption(Command.NODE_REMOVABLE.toString())){
        isNodeRemovable(cmd);
      } else if (cmd.hasOption(Command.REMOVE_NODE.toString())) {
        removeNodeFromCluster(cmd);
      } else if (cmd.hasOption(Command.WHITE_LIST_ADD_NODE.toString())) {
        addNodeIntoWhiteList(cmd);
      } else if (cmd.hasOption(Command.WHITE_LIST_REMOVE_NODE.toString())) {
        removeNodeFromWhiteList(cmd);
      } else if (cmd.hasOption(Command.REPLICAS_OF_STORE.toString())) {
        printReplicaListForStoreVersion(cmd);
      } else if (cmd.hasOption(Command.REPLICAS_ON_STORAGE_NODE.toString())) {
        printReplicaListForStorageNode(cmd);
      } else if (cmd.hasOption(Command.QUERY.toString())) {
        queryStoreForKey(cmd, routerHosts);
      } else if (cmd.hasOption(Command.SHOW_SCHEMAS.toString())){
        showSchemas(cmd);
      } else if(cmd.hasOption(Command.GET_EXECUTION.toString())){
        getExecution(cmd);
      } else if (cmd.hasOption(Command.ENABLE_THROTTLING.toString())) {
        enableThrottling(true);
      } else if (cmd.hasOption(Command.DISABLE_THROTTLING.toString())) {
        enableThrottling(false);
      } else if (cmd.hasOption(Command.ENABLE_MAX_CAPACITY_PROTECTION.toString())) {
        enableMaxCapacityProtection(true);
      } else if (cmd.hasOption(Command.DISABLE_MAX_CAPACITY_PROTECTION.toString())) {
        enableMaxCapacityProtection(false);
      } else if (cmd.hasOption(Command.ENABLE_QUTOA_REBALANCE.toString())) {
        enableQuotaRebalance(cmd, true);
      } else if (cmd.hasOption(Command.DISABLE_QUTOA_REBALANCE.toString())) {
        enableQuotaRebalance(cmd, false);
      } else if (cmd.hasOption(Command.GET_ROUTERS_CLUSTER_CONFIG.toString())) {
        getRoutersClusterConfig();
      } else if (cmd.hasOption(Command.GET_ALL_MIGRATION_PUSH_STRATEGIES.toString())) {
        getAllMigrationPushStrategies();
      } else if (cmd.hasOption(Command.GET_MIGRATION_PUSH_STRATEGY.toString())) {
        getMigrationPushStrategy(cmd);
      } else if (cmd.hasOption(Command.SET_MIGRATION_PUSH_STRATEGY.toString())) {
        setMigrationPushStrategy(cmd);
      } else if (cmd.hasOption(Command.LIST_BOOTSTRAPPING_VERSIONS.toString())) {
        listBootstrappingVersions(cmd);
      } else {
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

  private static void ensureOnlyOneCommand(CommandLine cmd){
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
  }

  private static void queryStoreForKey(CommandLine cmd, String routerHosts)
      throws VeniceClientException, ExecutionException, InterruptedException {
    String store = getRequiredArgument(cmd, Arg.STORE);
    String keyString = getRequiredArgument(cmd, Arg.KEY);
    boolean isVsonStore = Boolean.parseBoolean(getOptionalArgument(cmd, Arg.VSON_STORE, "false"));
    printObject(QueryTool.queryStoreForKey(store, keyString, routerHosts, isVsonStore));
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

  private static void updateStore(CommandLine cmd) {
    String storeName = getRequiredArgument(cmd, Arg.STORE, Command.UPDATE_STORE);
    Optional<String> owner = Optional.ofNullable(getOptionalArgument(cmd, Arg.OWNER));

    String currentVersionStr = getOptionalArgument(cmd, Arg.VERSION);
    Optional<Integer> currentVersion = currentVersionStr == null ? Optional.empty() :
        Optional.of(Utils.parseIntFromString(currentVersionStr, "currentVersion"));

    String partitionCountStr = getOptionalArgument(cmd, Arg.PARTITION_COUNT);
    Optional<Integer> partitionCount = partitionCountStr == null ? Optional.empty() :
        Optional.of(Utils.parseIntFromString(partitionCountStr, "partitionCount"));

    String readabilityStr = getOptionalArgument(cmd, Arg.READABILITY);
    Optional<Boolean> readability = readabilityStr == null ? Optional.empty() :
        Optional.of(Utils.parseBooleanFromString(readabilityStr, "enableReads"));

    String writeabilityStr = getOptionalArgument(cmd, Arg.WRITEABILITY);
    Optional<Boolean> writeability = writeabilityStr == null ? Optional.empty() :
        Optional.of(Utils.parseBooleanFromString(writeabilityStr, "enableWrites"));

    String storageQuotaStr = getOptionalArgument(cmd, Arg.STORAGE_QUOTA);
    Optional<Long> storageQuotaInByte = storageQuotaStr == null ? Optional.empty() :
        Optional.of(Utils.parseLongFromString(storageQuotaStr, "storageQuotaInByte"));

    String readQuotaStr = getOptionalArgument(cmd, Arg.READ_QUOTA);
    Optional<Long> readQuotaInCU = readQuotaStr == null ? Optional.empty() :
        Optional.of(Utils.parseLongFromString(readQuotaStr, "readQuotaInCU"));

    String hybridRewindSecondsStr = getOptionalArgument(cmd, Arg.HYBRID_REWIND_SECONDS);
    Optional<Long> hybridRewindSeconds = (null == hybridRewindSecondsStr)
        ? Optional.empty()
        : Optional.of(Utils.parseLongFromString(hybridRewindSecondsStr, Arg.HYBRID_REWIND_SECONDS.name()));

    String hybridOffsetLagStr = getOptionalArgument(cmd, Arg.HYBRID_OFFSET_LAG);
    Optional<Long> hybridOffsetLag = (null == hybridOffsetLagStr)
        ? Optional.empty()
        : Optional.of(Utils.parseLongFromString(hybridOffsetLagStr, Arg.HYBRID_OFFSET_LAG.name()));

    String accessControlStr = getOptionalArgument(cmd, Arg.ACCESS_CONTROL);
    Optional<Boolean> accessControlled = accessControlStr == null ? Optional.empty() :
        Optional.of(Utils.parseBooleanFromString(accessControlStr, Arg.ACCESS_CONTROL.name()));

    String compressionStrategyStr = getOptionalArgument(cmd, Arg.COMPRESSION_STRATEGY);
    Optional<CompressionStrategy> compressionStrategy = compressionStrategyStr == null
        ? Optional.empty()
        : Optional.of(CompressionStrategy.valueOf(compressionStrategyStr));

    String chunkingEnabledStr = getOptionalArgument(cmd, Arg.CHUNKING_ENABLED);
    Optional<Boolean> chunkingEnabled = Utils.isNullOrEmpty(chunkingEnabledStr) ? Optional.empty() :
        Optional.of(Utils.parseBooleanFromString(chunkingEnabledStr, Arg.CHUNKING_ENABLED.name()));

    String routerCacheEnabledStr = getOptionalArgument(cmd, Arg.ROUTER_CACHE_ENABLED);
    Optional<Boolean> routerCacheEnabled = Utils.isNullOrEmpty(routerCacheEnabledStr) ? Optional.empty() :
        Optional.of(Utils.parseBooleanFromString(routerCacheEnabledStr, Arg.ROUTER_CACHE_ENABLED.name()));

    ControllerResponse response = controllerClient.updateStore(
        storeName,
        owner,
        partitionCount,
        currentVersion,
        readability,
        writeability,
        storageQuotaInByte,
        readQuotaInCU,
        hybridRewindSeconds,
        hybridOffsetLag,
        accessControlled,
        compressionStrategy,
        chunkingEnabled,
        routerCacheEnabled);

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
    if (enable) {
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
    System.err.println("\nExamples:");
    Command[] commands = Command.values();
    Arrays.sort(commands, Command.commandComparator);
    for (Command c : commands){
      StringJoiner exampleArgs = new StringJoiner(" ");
      for (Arg a : c.getAllArgs()){
        exampleArgs.add("--" + a.toString());
        exampleArgs.add("<" + a.toString() + ">");
      }

      System.err.println(command + " --" + c.toString() + " " + exampleArgs.toString());
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
