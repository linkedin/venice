package com.linkedin.venice.router;

import static com.linkedin.venice.VeniceConstants.TYPE_PUSH_STATUS;
import static com.linkedin.venice.VeniceConstants.TYPE_STORE_STATE;
import static com.linkedin.venice.VeniceConstants.TYPE_STREAM_HYBRID_STORE_QUOTA;
import static com.linkedin.venice.VeniceConstants.TYPE_STREAM_REPROCESSING_HYBRID_STORE_QUOTA;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PARTITIONERS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_PARTITION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_VERSION;
import static com.linkedin.venice.meta.DataReplicationPolicy.NON_AGGREGATE;
import static com.linkedin.venice.router.api.RouterResourceType.TYPE_ALL_VALUE_SCHEMA_IDS;
import static com.linkedin.venice.router.api.RouterResourceType.TYPE_CURRENT_VERSION;
import static com.linkedin.venice.router.api.RouterResourceType.TYPE_GET_UPDATE_SCHEMA;
import static com.linkedin.venice.router.api.RouterResourceType.TYPE_LATEST_VALUE_SCHEMA;
import static com.linkedin.venice.router.api.VenicePathParser.TYPE_CLUSTER_DISCOVERY;
import static com.linkedin.venice.router.api.VenicePathParser.TYPE_KEY_SCHEMA;
import static com.linkedin.venice.router.api.VenicePathParser.TYPE_REQUEST_TOPIC;
import static com.linkedin.venice.router.api.VenicePathParser.TYPE_RESOURCE_STATE;
import static com.linkedin.venice.router.api.VenicePathParser.TYPE_VALUE_SCHEMA;
import static com.linkedin.venice.router.api.VenicePathParserHelper.parseRequest;
import static com.linkedin.venice.utils.NettyUtils.setupResponseAndFlush;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.blobtransfer.BlobPeersDiscoveryResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.CurrentVersionResponse;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.LeaderControllerResponse;
import com.linkedin.venice.controllerapi.MultiSchemaIdResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.exceptions.VeniceNoHelixResourceException;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.helix.HelixHybridStoreQuotaRepository;
import com.linkedin.venice.helix.StoreJSONSerializer;
import com.linkedin.venice.helix.SystemStoreJSONSerializer;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreConfigRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.SystemStore;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.HybridStoreQuotaStatus;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.router.api.RouterResourceType;
import com.linkedin.venice.router.api.VenicePathParserHelper;
import com.linkedin.venice.router.api.VeniceVersionFinder;
import com.linkedin.venice.routerapi.HybridStoreQuotaStatusResponse;
import com.linkedin.venice.routerapi.PushStatusResponse;
import com.linkedin.venice.routerapi.ReplicaState;
import com.linkedin.venice.routerapi.ResourceStateResponse;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.CertificateExpiredException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This MetaDataHandle is used to handle the following meta data requests:
 * 1. Controller lookup: /controller, and it will return leader controller url as the content.
 * 2. Key schema lookup: /key_schema/${storeName}, and it will return key schema in json format.
 *    The client can use {@link com.linkedin.venice.controllerapi.SchemaResponse} to parse it.
 * 3. Single value schema lookup: /value_schema/${storeName}/${valueSchemaId}, and it will return
 *    the corresponding value schema in json format.
 *    The client can use {@link com.linkedin.venice.controllerapi.SchemaResponse} to parse it.
 * 4. All value schema lookup: /value_schema/{$storeName}, and it will return all the value schema
 *    of the specified store in json format. The client can use
 *    {@link com.linkedin.venice.controllerapi.MultiSchemaResponse} to parse it.
 */
@ChannelHandler.Sharable
public class MetaDataHandler extends SimpleChannelInboundHandler<HttpRequest> {
  private static final Logger LOGGER = LogManager.getLogger(MetaDataHandler.class);
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
  private static final StoreJSONSerializer STORE_SERIALIZER = new StoreJSONSerializer();
  private static final SystemStoreJSONSerializer SYSTEM_STORE_SERIALIZER = new SystemStoreJSONSerializer();
  private static final RedundantExceptionFilter EXCEPTION_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();

  private final PushStatusStoreReader pushStatusStoreReader;

  private final HelixCustomizedViewOfflinePushRepository routingDataRepository;
  private final ReadOnlySchemaRepository schemaRepo;
  private final ReadOnlyStoreConfigRepository storeConfigRepo;
  private final Map<String, String> clusterToD2Map;
  private final Map<String, String> clusterToServerD2Map;
  private final Optional<HelixHybridStoreQuotaRepository> hybridStoreQuotaRepository;
  private final ReadOnlyStoreRepository storeRepository;
  private final String clusterName;
  private final String zkAddress;
  private final String kafkaBootstrapServers;
  private final boolean isSslToKafka;

  static final String REQUEST_TOPIC_ERROR_WRITES_DISABLED = "Write operations to the store are disabled.";
  static final String REQUEST_TOPIC_ERROR_BATCH_ONLY_STORE = "Online writes are only supported for hybrid stores.";
  static final String REQUEST_TOPIC_ERROR_NO_CURRENT_VERSION =
      "Store doesn't have an active version. Please push data to the store.";
  static final String REQUEST_TOPIC_ERROR_MISSING_CURRENT_VERSION =
      "Store has a current version, but the configs for the current version are not present. This is unexpected.";
  static final String REQUEST_TOPIC_ERROR_CURRENT_VERSION_NOT_HYBRID =
      "Online writes are only supported for stores with a current version capable of receiving hybrid writes.";
  static final String REQUEST_TOPIC_ERROR_UNSUPPORTED_REPLICATION_POLICY =
      "Online writes are only supported for hybrid stores that either have Active-Active replication enabled or "
          + NON_AGGREGATE + " data replication policy when Active-Active replication is disabled.";
  static final String REQUEST_TOPIC_ERROR_FORMAT_UNSUPPORTED_PARTITIONER =
      "Expected partitioner class %s cannot be found.";

  static final String REQUEST_ERROR_STORE_NOT_FOUND_IN_CLUSTER = "Store: %s could not be found in cluster: %s";

  static final String REQUEST_BLOB_DISCOVERY_ERROR_INVALID_SETTINGS =
      "Blob Discovery: blob transfer is not enabled or store: %s is not a batch-only store";

  static final String REQUEST_BLOB_DISCOVERY_MISSING_QUERY_PARAMS =
      "Blob Discovery: missing storeName:%s, storeVersion:%s, or storePartition:%s";

  static final String REQUEST_BLOB_DISCOVERY_ERROR_PUSH_STORE =
      "Blob Discovery: failed to get the live node hostNames for store:%s version:%s partition:%s";
  private final VeniceVersionFinder veniceVersionFinder;

  public MetaDataHandler(
      HelixCustomizedViewOfflinePushRepository routingDataRepository,
      ReadOnlySchemaRepository schemaRepo,
      ReadOnlyStoreConfigRepository storeConfigRepo,
      Map<String, String> clusterToD2Map,
      Map<String, String> clusterToServerD2Map,
      ReadOnlyStoreRepository storeRepository,
      Optional<HelixHybridStoreQuotaRepository> hybridStoreQuotaRepository,
      String clusterName,
      String zkAddress,
      String kafkaBootstrapServers,
      boolean isSslToKafka,
      VeniceVersionFinder versionFinder,
      PushStatusStoreReader pushStatusStoreReader) {
    super();
    this.routingDataRepository = routingDataRepository;
    this.schemaRepo = schemaRepo;
    this.storeConfigRepo = storeConfigRepo;
    this.clusterToD2Map = clusterToD2Map;
    this.clusterToServerD2Map = clusterToServerD2Map;
    this.hybridStoreQuotaRepository = hybridStoreQuotaRepository;
    this.storeRepository = storeRepository;
    this.clusterName = clusterName;
    this.zkAddress = zkAddress;
    this.kafkaBootstrapServers = kafkaBootstrapServers;
    this.isSslToKafka = isSslToKafka;
    this.veniceVersionFinder = versionFinder;
    this.pushStatusStoreReader = pushStatusStoreReader;
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpRequest req) throws IOException {
    VenicePathParserHelper helper = parseRequest(req);
    RouterResourceType resourceType = helper.getResourceType(); // may be null

    try {
      switch (resourceType) {
        case TYPE_LEADER_CONTROLLER:
        case TYPE_LEADER_CONTROLLER_LEGACY:
          // URI: /leader_controller or /master_controller
          handleControllerLookup(ctx);
          break;
        case TYPE_KEY_SCHEMA:
          // URI: /key_schema/${storeName}
          // For key schema lookup, we only consider storeName
          handleKeySchemaLookup(ctx, helper);
          break;
        case TYPE_VALUE_SCHEMA:
          // The request could fetch one value schema by id or all the value schema for the given store
          // URI: /value_schema/{$storeName} - Get all the value schema
          // URI: /value_schema/{$storeName}/{$valueSchemaId} - Get single value schema
          handleValueSchemaLookup(ctx, helper);
          break;
        case TYPE_LATEST_VALUE_SCHEMA:
          // The request could fetch the latest value schema for the given store
          // URI: /latest_value_schema/{$storeName} - Get the latest value schema
          handleLatestValueSchemaLookup(ctx, helper);
          break;
        case TYPE_ALL_VALUE_SCHEMA_IDS:
          // The request could fetch all the value schema IDs for the given store, also superset schema ID.
          // URI: /all_value_schema_ids/{$storeName} - Get all value schema IDs.
          handleValueSchemaIdsLookup(ctx, helper);
          break;
        case TYPE_GET_UPDATE_SCHEMA:
          // URI: /update_schema/{$storeName} - Get all the update schema
          // URI: /update_schema/{$storeName}/{$valueSchemaId} - Get single update schema
          // The request could fetch the latest derived update schema of a specific value schema
          handleUpdateSchemaLookup(ctx, helper);
          break;
        case TYPE_CLUSTER_DISCOVERY:
          // URI: /discover_cluster/${storeName}
          handleD2ServiceLookup(ctx, helper, req);
          break;
        case TYPE_RESOURCE_STATE:
          // URI: /resource_state
          handleResourceStateLookup(ctx, helper);
          break;
        case TYPE_PUSH_STATUS:
          // URI: /push_status
          handlePushStatusLookUp(ctx, helper);
          break;
        case TYPE_STREAM_HYBRID_STORE_QUOTA:
          handleStreamHybridStoreQuotaStatusLookup(ctx, helper);
          break;
        case TYPE_STREAM_REPROCESSING_HYBRID_STORE_QUOTA:
          handleStreamReprocessingHybridStoreQuotaStatusLookup(ctx, helper);
          break;
        case TYPE_STORE_STATE:
          handleStoreStateLookup(ctx, helper);
          break;
        case TYPE_REQUEST_TOPIC:
          handleRequestTopic(ctx, helper, req);
          break;
        case TYPE_CURRENT_VERSION:
          handleCurrentVersionLookup(ctx, helper);
          break;
        case TYPE_BLOB_DISCOVERY:
          handleBlobDiscovery(ctx, helper, req);
          break;
        default:
          // SimpleChannelInboundHandler automatically releases the request after channelRead0 is done.
          // since we're passing it on to the next handler, we need to retain an extra reference.
          ReferenceCountUtil.retain(req);
          ctx.fireChannelRead(req);
      }
    } catch (VeniceHttpException e) {
      setupResponseAndFlush(HttpResponseStatus.valueOf(e.getHttpStatusCode()), e.getMessage().getBytes(), false, ctx);
    }
  }

  private void handleControllerLookup(ChannelHandlerContext ctx) throws IOException {
    LeaderControllerResponse responseObject = new LeaderControllerResponse();
    responseObject.setCluster(clusterName);
    responseObject.setUrl(routingDataRepository.getLeaderController().getUrl());
    LOGGER.info(
        "For cluster: {}, the leader controller url: {}, last refreshed at {}",
        responseObject.getCluster(),
        responseObject.getUrl(),
        routingDataRepository.getLeaderControllerChangeTimeMs());
    setupResponseAndFlush(OK, OBJECT_MAPPER.writeValueAsBytes(responseObject), true, ctx);
  }

  private void handleKeySchemaLookup(ChannelHandlerContext ctx, VenicePathParserHelper helper) throws IOException {
    String storeName = helper.getResourceName();
    checkResourceName(storeName, "/" + TYPE_KEY_SCHEMA + "/${storeName}");
    SchemaEntry keySchema = schemaRepo.getKeySchema(storeName);
    if (keySchema == null) {
      byte[] errBody = ("Key schema for store: " + storeName + " doesn't exist").getBytes();
      setupResponseAndFlush(NOT_FOUND, errBody, false, ctx);
      return;
    }
    SchemaResponse responseObject = new SchemaResponse();
    responseObject.setCluster(clusterName);
    responseObject.setName(storeName);
    responseObject.setId(keySchema.getId());
    responseObject.setSchemaStr(keySchema.getSchema().toString());
    setupResponseAndFlush(OK, OBJECT_MAPPER.writeValueAsBytes(responseObject), true, ctx);
  }

  private void handleValueSchemaLookup(ChannelHandlerContext ctx, VenicePathParserHelper helper) throws IOException {
    String storeName = helper.getResourceName();
    checkResourceName(
        storeName,
        "/" + TYPE_VALUE_SCHEMA + "/${storeName} or /" + TYPE_VALUE_SCHEMA + "/${storeName}/${valueSchemaId}");
    String id = helper.getKey();
    if (id == null || id.isEmpty()) {
      // URI: /value_schema/{$storeName}
      // Get all the value schema
      MultiSchemaResponse responseObject = new MultiSchemaResponse();
      responseObject.setCluster(clusterName);
      responseObject.setName(storeName);
      int superSetSchemaId = storeRepository.getStore(storeName).getLatestSuperSetValueSchemaId();
      if (superSetSchemaId != SchemaData.INVALID_VALUE_SCHEMA_ID) {
        responseObject.setSuperSetSchemaId(superSetSchemaId);
      }
      Collection<SchemaEntry> valueSchemaEntries = schemaRepo.getValueSchemas(storeName);
      int schemaNum = (int) valueSchemaEntries.stream().filter(schemaEntry -> schemaEntry.getId() > 0).count();
      MultiSchemaResponse.Schema[] schemas = new MultiSchemaResponse.Schema[schemaNum];
      int index = 0;
      for (SchemaEntry entry: valueSchemaEntries) {
        int schemaId = entry.getId();
        if (schemaId < 1) {
          LOGGER.warn(
              "Got an invalid schema id ({}) for store {} in handleValueSchemaLookup; will not include this in the {}.",
              entry.getId(),
              storeName,
              responseObject.getClass().getSimpleName());
          continue;
        }
        schemas[index] = new MultiSchemaResponse.Schema();
        schemas[index].setId(schemaId);
        schemas[index].setSchemaStr(entry.getSchema().toString());
        index++;
      }
      responseObject.setSchemas(schemas);
      setupResponseAndFlush(OK, OBJECT_MAPPER.writeValueAsBytes(responseObject), true, ctx);
    } else {
      // URI: /value_schema/{$storeName}/{$valueSchemaId}
      // Get single value schema
      SchemaResponse responseObject = new SchemaResponse();
      responseObject.setCluster(clusterName);
      responseObject.setName(storeName);
      SchemaEntry valueSchema = schemaRepo.getValueSchema(storeName, Integer.parseInt(id));
      if (valueSchema == null) {
        byte[] errBody = ("Value schema doesn't exist for schema id: " + id + " of store: " + storeName).getBytes();
        setupResponseAndFlush(NOT_FOUND, errBody, false, ctx);
        return;
      }
      responseObject.setId(valueSchema.getId());
      responseObject.setSchemaStr(valueSchema.getSchema().toString());
      setupResponseAndFlush(OK, OBJECT_MAPPER.writeValueAsBytes(responseObject), true, ctx);
    }
  }

  /**
   * Returns the latest available value schema of the store. The latest superset schema is:
   * 1. If a superset schema exists for the store, return the superset schema
   * 2. If no superset schema exists for the store, return the value schema with the largest schema id
   */
  private void handleLatestValueSchemaLookup(ChannelHandlerContext ctx, VenicePathParserHelper helper)
      throws IOException {
    String storeName = helper.getResourceName();
    checkResourceName(storeName, "/" + TYPE_LATEST_VALUE_SCHEMA + "/${storeName}");

    // URI: /latest_value_schema/{$storeName}
    // Get the latest value schema
    // If a superset schema exists, return that
    // Otherwise, return the largest value schema
    SchemaResponse responseObject = new SchemaResponse();
    responseObject.setCluster(clusterName);
    responseObject.setName(storeName);
    SchemaEntry latestValueSchemaEntry = schemaRepo.getSupersetOrLatestValueSchema(storeName);
    if (latestValueSchemaEntry == null) {
      byte[] errBody = ("Latest value schema doesn't exist for store: " + storeName).getBytes();
      setupResponseAndFlush(INTERNAL_SERVER_ERROR, errBody, false, ctx);
      return;
    }
    responseObject.setId(latestValueSchemaEntry.getId());
    responseObject.setSchemaStr(latestValueSchemaEntry.getSchemaStr());
    setupResponseAndFlush(OK, OBJECT_MAPPER.writeValueAsBytes(responseObject), true, ctx);
  }

  private void handleValueSchemaIdsLookup(ChannelHandlerContext ctx, VenicePathParserHelper helper) throws IOException {
    String storeName = helper.getResourceName();
    checkResourceName(storeName, "/" + TYPE_ALL_VALUE_SCHEMA_IDS + "/${storeName}");
    // URI: /value_schema_ids/{$storeName}
    // Return all value schema IDs as a set.
    // If superset schema id exists, also return it.
    MultiSchemaIdResponse responseObject = new MultiSchemaIdResponse();
    responseObject.setCluster(clusterName);
    responseObject.setName(storeName);

    int superSetSchemaId = storeRepository.getStore(storeName).getLatestSuperSetValueSchemaId();
    if (superSetSchemaId != SchemaData.INVALID_VALUE_SCHEMA_ID) {
      responseObject.setSuperSetSchemaId(superSetSchemaId);
    }
    Set<Integer> schemaIdSet = new HashSet<>();
    for (SchemaEntry entry: schemaRepo.getValueSchemas(storeName)) {
      if (entry.getId() < 1) {
        LOGGER.warn(
            "Got an invalid schema id ({}) for store {} in handleValueSchemaIdsLookup; will not include this in the {}.",
            entry.getId(),
            storeName,
            responseObject.getClass().getSimpleName());
        continue;
      }
      schemaIdSet.add(entry.getId());
    }
    responseObject.setSchemaIdSet(schemaIdSet);
    setupResponseAndFlush(OK, OBJECT_MAPPER.writeValueAsBytes(responseObject), true, ctx);
  }

  private void handleUpdateSchemaLookup(ChannelHandlerContext ctx, VenicePathParserHelper helper) throws IOException {
    String storeName = helper.getResourceName();
    checkResourceName(
        storeName,
        "/" + TYPE_GET_UPDATE_SCHEMA + "/${storeName} or /" + TYPE_GET_UPDATE_SCHEMA
            + "/${storeName}/${valueSchemaId}");
    String valueSchemaIdStr = helper.getKey();
    if (valueSchemaIdStr == null || valueSchemaIdStr.isEmpty()) {
      // URI: /update_schema/{$storeName}
      // Get all the update schema
      MultiSchemaResponse responseObject = new MultiSchemaResponse();
      responseObject.setCluster(clusterName);
      responseObject.setName(storeName);
      int superSetSchemaId = storeRepository.getStore(storeName).getLatestSuperSetValueSchemaId();
      if (superSetSchemaId != SchemaData.INVALID_VALUE_SCHEMA_ID) {
        responseObject.setSuperSetSchemaId(superSetSchemaId);
      }
      Collection<DerivedSchemaEntry> derivedSchemaEntries = schemaRepo.getDerivedSchemas(storeName);
      int schemaNum = derivedSchemaEntries.size();
      MultiSchemaResponse.Schema[] schemas = new MultiSchemaResponse.Schema[schemaNum];
      for (DerivedSchemaEntry entry: derivedSchemaEntries) {
        int valueSchemaId = entry.getValueSchemaID();
        schemas[valueSchemaId - 1] = new MultiSchemaResponse.Schema();
        schemas[valueSchemaId - 1].setSchemaStr(entry.getSchema().toString());
        schemas[valueSchemaId - 1].setDerivedSchemaId(entry.getId());
        schemas[valueSchemaId - 1].setId(valueSchemaId);
      }
      responseObject.setSchemas(schemas);
      setupResponseAndFlush(OK, OBJECT_MAPPER.writeValueAsBytes(responseObject), true, ctx);
    } else {
      // URI: /update_schema/{$storeName}/{$valueSchemaId}
      // Get latest update schema by value schema id
      int valueSchemaId = Integer.parseInt(valueSchemaIdStr);
      Optional<DerivedSchemaEntry> updateSchemaOptional =
          getLatestUpdateSchemaWithValueSchemaId(storeName, valueSchemaId);
      if (!updateSchemaOptional.isPresent()) {
        byte[] errBody =
            ("Update schema doesn't exist for value schema id: " + valueSchemaIdStr + " of store: " + storeName)
                .getBytes();
        setupResponseAndFlush(NOT_FOUND, errBody, false, ctx);
        return;
      }
      SchemaResponse responseObject = new SchemaResponse();
      responseObject.setCluster(clusterName);
      responseObject.setName(storeName);
      responseObject.setId(valueSchemaId);
      responseObject.setDerivedSchemaId(updateSchemaOptional.get().getId());
      responseObject.setSchemaStr(updateSchemaOptional.get().getSchemaStr());
      setupResponseAndFlush(OK, OBJECT_MAPPER.writeValueAsBytes(responseObject), true, ctx);
    }
  }

  private void handleD2ServiceLookup(ChannelHandlerContext ctx, VenicePathParserHelper helper, HttpRequest request)
      throws IOException {
    String storeName = helper.getResourceName();
    if (StringUtils.isEmpty(storeName)) {
      Map<String, String> queryParams = helper.extractQueryParameters(request);
      storeName = queryParams.get(NAME);
    }
    checkResourceName(
        storeName,
        "/" + TYPE_CLUSTER_DISCOVERY + "/${storeName} or /" + TYPE_CLUSTER_DISCOVERY + "?store_name=${storeName}");
    Optional<StoreConfig> config = storeConfigRepo.getStoreConfig(storeName);
    if (!config.isPresent() || StringUtils.isEmpty(config.get().getCluster())) {
      String errorMsg = "Cluster for store: " + storeName + " doesn't exist";
      setupErrorD2DiscoveryResponseAndFlush(NOT_FOUND, errorMsg, ctx);
      return;
    }
    String clusterName = config.get().getCluster();
    String d2Service = getD2ServiceByClusterName(clusterName);
    if (StringUtils.isEmpty(d2Service)) {
      String errorMsg = "D2 service for store: " + storeName + " doesn't exist";
      setupErrorD2DiscoveryResponseAndFlush(NOT_FOUND, errorMsg, ctx);
      return;
    }

    D2ServiceDiscoveryResponse responseObject = new D2ServiceDiscoveryResponse();
    responseObject.setCluster(config.get().getCluster());
    responseObject.setName(config.get().getStoreName());
    responseObject.setD2Service(d2Service);
    responseObject.setServerD2Service(getServerD2ServiceByClusterName(clusterName));
    responseObject.setZkAddress(zkAddress);
    responseObject.setKafkaBootstrapServers(kafkaBootstrapServers);
    setupResponseAndFlush(OK, OBJECT_MAPPER.writeValueAsBytes(responseObject), true, ctx);
  }

  private void setupErrorD2DiscoveryResponseAndFlush(
      HttpResponseStatus status,
      String errorMsg,
      ChannelHandlerContext ctx) throws IOException {
    D2ServiceDiscoveryResponse responseObject = new D2ServiceDiscoveryResponse();
    responseObject.setError(errorMsg);
    if (status.equals(NOT_FOUND)) {
      responseObject.setErrorType(ErrorType.STORE_NOT_FOUND);
    }
    setupResponseAndFlush(status, OBJECT_MAPPER.writeValueAsBytes(responseObject), true, ctx);
  }

  private void handleCurrentVersionLookup(ChannelHandlerContext ctx, VenicePathParserHelper helper) throws IOException {
    String storeName = helper.getResourceName();
    checkResourceName(storeName, "/" + TYPE_CURRENT_VERSION + "/${storeName}");

    if (storeRepository.getStore(storeName) == null) {
      byte[] errBody =
          ("Cannot find current version for store: " + storeName + " as it cannot be found in cluster: " + clusterName)
              .getBytes();
      setupResponseAndFlush(NOT_FOUND, errBody, false, ctx);
      return;
    }
    int currentVersion = veniceVersionFinder.getVersion(storeName, null);
    CurrentVersionResponse response = new CurrentVersionResponse();
    response.setCurrentVersion(currentVersion);
    setupResponseAndFlush(OK, OBJECT_MAPPER.writeValueAsBytes(response), true, ctx);
  }

  /**
   * Handles the discovery of blob transfer nodes based on store settings.
   * Retrieves host names for live DVC nodes ready to transfer blobs.
   * Only returns host names from nodes that have completed a full push and are active.
   *
   * @return a response with a list of host names for live DVC nodes; returns an empty list if no live nodes are found or if conditions are not met
   */
  private void handleBlobDiscovery(ChannelHandlerContext ctx, VenicePathParserHelper helper, HttpRequest request)
      throws IOException {

    // i.e. /blob_discovery?store_name=storeName&store_version=22&store_partition=2
    Map<String, String> queryParams = helper.extractQueryParameters(request);
    String storeName = queryParams.getOrDefault(NAME, "");
    String storeVersion = queryParams.getOrDefault(STORE_VERSION, "");
    String storePartition = queryParams.getOrDefault(STORE_PARTITION, "");

    if (StringUtils.isEmpty(storeName) || StringUtils.isEmpty(storeVersion) || StringUtils.isEmpty(storePartition)) {
      byte[] errBody =
          (String.format(REQUEST_BLOB_DISCOVERY_MISSING_QUERY_PARAMS, storeName, storeVersion, storePartition))
              .getBytes();
      setupResponseAndFlush(BAD_REQUEST, errBody, false, ctx);
      return;
    }

    Store store = storeRepository.getStore(storeName);
    if (store == null) {
      byte[] errBody = (String.format(REQUEST_ERROR_STORE_NOT_FOUND_IN_CLUSTER, storeName, clusterName)).getBytes();
      setupResponseAndFlush(NOT_FOUND, errBody, false, ctx);
      return;
    }

    if (!store.isBlobTransferEnabled() || store.isHybrid()) {
      byte[] errBody = (String.format(REQUEST_BLOB_DISCOVERY_ERROR_INVALID_SETTINGS, storeName)).getBytes();
      setupResponseAndFlush(FORBIDDEN, errBody, false, ctx);
      return;
    }

    BlobPeersDiscoveryResponse response = new BlobPeersDiscoveryResponse();
    try {
      // gets the instances for a FULL_PUSH for the store's version and partitionId
      // gets the instance's hostnames from its keys & filter to include only live instances
      Map<CharSequence, Integer> instances = pushStatusStoreReader.getPartitionStatus(
          storeName,
          Integer.parseInt(storeVersion),
          Integer.parseInt(storePartition),
          Optional.empty());

      if (instances.isEmpty()) {
        LOGGER.info(
            "No instances found for store: {} version: {} partition: {}",
            storeName,
            storeVersion,
            storePartition);
      } else {
        LOGGER.info("{} instances were found", instances.size());
      }

      List<String> readyToServeNodeHostNames = instances.entrySet()
          .stream()
          .filter(entry -> entry.getValue() == ExecutionStatus.COMPLETED.getValue())
          .map(Map.Entry::getKey)
          .map(CharSequence::toString)
          .collect(Collectors.toList());

      if (!readyToServeNodeHostNames.isEmpty()) {
        LOGGER.info("{} ready to serve nodes were found", readyToServeNodeHostNames.size());
      } else {
        LOGGER.info(
            "No ready to serve nodes found for store: {} version: {} partition: {}",
            storeName,
            storeVersion,
            storePartition);
      }

      response.setDiscoveryResult(readyToServeNodeHostNames);
    } catch (VeniceException e) {
      byte[] errBody =
          (String.format(REQUEST_BLOB_DISCOVERY_ERROR_PUSH_STORE, storeName, storeVersion, storePartition)).getBytes();
      setupResponseAndFlush(INTERNAL_SERVER_ERROR, errBody, false, ctx);
      return;
    }

    setupResponseAndFlush(OK, OBJECT_MAPPER.writeValueAsBytes(response), true, ctx);
  }

  private void handleResourceStateLookup(ChannelHandlerContext ctx, VenicePathParserHelper helper) throws IOException {
    String resourceName = helper.getResourceName();
    boolean isResourceReadyToServe = true;
    checkResourceName(resourceName, "/" + TYPE_RESOURCE_STATE + "/${resourceName}");
    if (!Version.isVersionTopic(resourceName)) {
      byte[] errBody = ("Invalid resource name: " + resourceName).getBytes();
      setupResponseAndFlush(BAD_REQUEST, errBody, false, ctx);
      return;
    }
    String storeName = Version.parseStoreFromKafkaTopicName(resourceName);
    if (storeRepository.getStore(storeName) == null) {
      byte[] errBody = ("Cannot fetch the state for resource: " + resourceName + " because the store: " + storeName
          + " cannot be found in cluster: " + clusterName).getBytes();
      setupResponseAndFlush(NOT_FOUND, errBody, false, ctx);
      return;
    }
    List<ReplicaState> replicaStates = new ArrayList<>();
    List<ReplicaState> partitionReplicaStates;
    List<Integer> unretrievablePartitions = new ArrayList<>();
    for (int p = 0; p < routingDataRepository.getNumberOfPartitions(resourceName); p++) {
      try {
        partitionReplicaStates = routingDataRepository.getReplicaStates(resourceName, p);
        if (partitionReplicaStates.isEmpty()) {
          unretrievablePartitions.add(p);
          continue;
        }
      } catch (VeniceNoHelixResourceException e) {
        byte[] errBody = ("Cannot find metadata for resource: " + resourceName).getBytes();
        setupResponseAndFlush(NOT_FOUND, errBody, false, ctx);
        return;
      }
      if (isResourceReadyToServe) {
        isResourceReadyToServe = partitionReplicaStates.stream()
            .filter(ReplicaState::isReadyToServe)
            .count() > (partitionReplicaStates.size() / 2);
      }
      replicaStates.addAll(partitionReplicaStates);
    }
    ResourceStateResponse response = new ResourceStateResponse();
    if (!unretrievablePartitions.isEmpty()) {
      response.setUnretrievablePartitions(unretrievablePartitions);
      response.setError(
          "Unable to retrieve replica states for partition(s): " + Arrays.toString(unretrievablePartitions.toArray()));
      isResourceReadyToServe = false;
    }
    response.setCluster(clusterName);
    response.setName(resourceName);
    response.setReplicaStates(replicaStates);
    response.setReadyToServe(isResourceReadyToServe);
    setupResponseAndFlush(OK, OBJECT_MAPPER.writeValueAsBytes(response), true, ctx);
  }

  /**
   * Get push status for STREAM_REPROCESSING job via router.
   */
  private void handlePushStatusLookUp(ChannelHandlerContext ctx, VenicePathParserHelper helper) throws IOException {
    String resourceName = helper.getResourceName();
    checkResourceName(resourceName, "/" + TYPE_PUSH_STATUS + "/${resourceName}");

    if (!storeConfigRepo.getStoreConfig(Version.parseStoreFromKafkaTopicName(resourceName)).isPresent()) {
      byte[] errBody = ("Cannot fetch the push status for resource: " + resourceName + " because the store: "
          + Version.parseStoreFromKafkaTopicName(resourceName) + " cannot be found").getBytes();
      setupResponseAndFlush(NOT_FOUND, errBody, false, ctx);
      return;
    }
    // TODO: Add push status look up support in HelixCustomizedViewOfflinePushRepository.
    PushStatusResponse pushStatusResponse = new PushStatusResponse();
    pushStatusResponse.setName(resourceName);
    pushStatusResponse.setError("Only support getting push status for stores running in Leader/Follower mode");
    setupResponseAndFlush(BAD_REQUEST, OBJECT_MAPPER.writeValueAsBytes(pushStatusResponse), true, ctx);
  }

  /**
   * Get hybrid store quota status from {@link HelixHybridStoreQuotaRepository} for stores.
   */
  private void handleStreamHybridStoreQuotaStatusLookup(ChannelHandlerContext ctx, VenicePathParserHelper helper)
      throws IOException {
    String storeName = helper.getResourceName();
    checkResourceName(storeName, "/" + TYPE_STREAM_HYBRID_STORE_QUOTA + "/${storeName}");
    Store store = storeRepository.getStore(storeName);
    if (!storeConfigRepo.getStoreConfig(storeName).isPresent() || store == null) {
      byte[] errBody = ("Cannot fetch the hybrid store quota status for store: " + storeName + " because the store: "
          + storeName + " cannot be found").getBytes();
      setupResponseAndFlush(NOT_FOUND, errBody, false, ctx);
      return;
    }
    String topicName = Version.composeKafkaTopic(storeName, store.getCurrentVersion());
    prepareHybridStoreQuotaStatusResponse(topicName, ctx);
  }

  /**
   * Get hybrid store quota status from {@link HelixHybridStoreQuotaRepository} for stores.
   */
  private void handleStreamReprocessingHybridStoreQuotaStatusLookup(
      ChannelHandlerContext ctx,
      VenicePathParserHelper helper) throws IOException {
    String resourceName = helper.getResourceName();
    checkResourceName(resourceName, "/" + TYPE_STREAM_REPROCESSING_HYBRID_STORE_QUOTA + "/${resourceName}");
    if (!storeConfigRepo.getStoreConfig(Version.parseStoreFromKafkaTopicName(resourceName)).isPresent()) {
      byte[] errBody =
          ("Cannot fetch the hybrid store quota status for resource: " + resourceName + " because the store: "
              + Version.parseStoreFromKafkaTopicName(resourceName) + " cannot be found").getBytes();
      setupResponseAndFlush(NOT_FOUND, errBody, false, ctx);
      return;
    }
    prepareHybridStoreQuotaStatusResponse(resourceName, ctx);
  }

  /**
   * Get the current Store object for a given storeName, including Venice system stores.
   */
  private void handleStoreStateLookup(ChannelHandlerContext ctx, VenicePathParserHelper helper) throws IOException {
    String storeName = helper.getResourceName();
    checkResourceName(storeName, "/" + TYPE_STORE_STATE + "/${storeName}");
    Store store = storeRepository.getStore(storeName);
    if (store == null) {
      byte[] errBody = ("Cannot fetch the store state for store: " + storeName
          + " because the store cannot be found in cluster: " + clusterName).getBytes();
      setupResponseAndFlush(NOT_FOUND, errBody, false, ctx);
      return;
    }
    byte[] body;
    if (store instanceof SystemStore) {
      SystemStore systemStore = (SystemStore) store;
      body = SYSTEM_STORE_SERIALIZER.serialize(systemStore.getSerializableSystemStore(), null);
    } else {
      body = STORE_SERIALIZER.serialize(store, null);
    }
    setupResponseAndFlush(OK, body, true, ctx);
  }

  private void handleRequestTopic(ChannelHandlerContext ctx, VenicePathParserHelper helper, HttpRequest request)
      throws IOException {
    String storeName = helper.getResourceName();
    checkResourceName(storeName, "/" + TYPE_REQUEST_TOPIC + "/${storeName}");

    Store store = storeRepository.getStore(storeName);

    if (!store.isEnableWrites()) {
      setupResponseAndFlush(BAD_REQUEST, REQUEST_TOPIC_ERROR_WRITES_DISABLED.getBytes(), false, ctx);
      return;
    }

    // Only allow router request_topic for hybrid stores
    if (!store.isHybrid()) {
      setupResponseAndFlush(BAD_REQUEST, REQUEST_TOPIC_ERROR_BATCH_ONLY_STORE.getBytes(), false, ctx);
      return;
    }

    int currentVersionNumber = store.getCurrentVersion();
    if (currentVersionNumber == Store.NON_EXISTING_VERSION) {
      setupResponseAndFlush(BAD_REQUEST, REQUEST_TOPIC_ERROR_NO_CURRENT_VERSION.getBytes(), false, ctx);
      return;
    }

    Version currentVersion = store.getVersion(currentVersionNumber);
    if (currentVersion == null) {
      setupResponseAndFlush(INTERNAL_SERVER_ERROR, REQUEST_TOPIC_ERROR_MISSING_CURRENT_VERSION.getBytes(), false, ctx);
      return;
    }

    final HybridStoreConfig hybridStoreConfig;
    if (currentVersion.isUseVersionLevelHybridConfig()) {
      if (currentVersion.getHybridStoreConfig() == null) {
        setupResponseAndFlush(BAD_REQUEST, REQUEST_TOPIC_ERROR_CURRENT_VERSION_NOT_HYBRID.getBytes(), false, ctx);
        return;
      }
      hybridStoreConfig = currentVersion.getHybridStoreConfig();
    } else {
      hybridStoreConfig = store.getHybridStoreConfig();
    }

    /**
     * Only allow router request_topic for hybrid stores that have either
     * 1. AA enabled
     * 2. AA disabled and data replication policy is NON_AGGREGATE
     */
    DataReplicationPolicy dataReplicationPolicy = hybridStoreConfig.getDataReplicationPolicy();
    if (!currentVersion.isActiveActiveReplicationEnabled() && !dataReplicationPolicy.equals(NON_AGGREGATE)) {
      setupResponseAndFlush(BAD_REQUEST, REQUEST_TOPIC_ERROR_UNSUPPORTED_REPLICATION_POLICY.getBytes(), false, ctx);
      return;
    }

    // Retrieve partitioner config from the store
    PartitionerConfig storePartitionerConfig = store.getPartitionerConfig();
    Map<String, String> queryParams = helper.extractQueryParameters(request);
    if (queryParams.get(PARTITIONERS) != null) {
      // Retrieve provided partitioner class list from the request
      boolean hasMatchedPartitioner = false;
      for (String partitioner: queryParams.get(PARTITIONERS).split(",")) {
        if (partitioner.equals(storePartitionerConfig.getPartitionerClass())) {
          hasMatchedPartitioner = true;
          break;
        }
      }
      if (!hasMatchedPartitioner) {
        String errorMsg = String
            .format(REQUEST_TOPIC_ERROR_FORMAT_UNSUPPORTED_PARTITIONER, storePartitionerConfig.getPartitionerClass());
        setupResponseAndFlush(BAD_REQUEST, errorMsg.getBytes(), false, ctx);
        return;
      }
    }

    VersionCreationResponse responseObject = new VersionCreationResponse();
    responseObject.setCluster(clusterName);
    responseObject.setName(storeName);
    responseObject.setPartitions(currentVersion.getPartitionCount());
    responseObject.setKafkaTopic(Version.composeRealTimeTopic(storeName));

    // RT topic only supports NO_OP compression
    responseObject.setCompressionStrategy(CompressionStrategy.NO_OP);
    // disable amplificationFactor logic on real-time topic
    responseObject.setAmplificationFactor(1);
    responseObject.setKafkaBootstrapServers(kafkaBootstrapServers);
    responseObject.setEnableSSL(isSslToKafka);
    responseObject.setDaVinciPushStatusStoreEnabled(store.isDaVinciPushStatusStoreEnabled());
    responseObject.setPartitionerClass(storePartitionerConfig.getPartitionerClass());
    responseObject.setPartitionerParams(storePartitionerConfig.getPartitionerParams());

    setupResponseAndFlush(OK, OBJECT_MAPPER.writeValueAsBytes(responseObject), true, ctx);
  }

  private void prepareHybridStoreQuotaStatusResponse(String resourceName, ChannelHandlerContext ctx)
      throws IOException {
    HybridStoreQuotaStatusResponse hybridStoreQuotaStatusResponse = new HybridStoreQuotaStatusResponse();
    hybridStoreQuotaStatusResponse.setName(resourceName);
    if (hybridStoreQuotaRepository.isPresent()) {
      hybridStoreQuotaStatusResponse
          .setQuotaStatus(hybridStoreQuotaRepository.get().getHybridStoreQuotaStatus(resourceName));
    } else {
      hybridStoreQuotaStatusResponse.setQuotaStatus(HybridStoreQuotaStatus.UNKNOWN);
    }
    setupResponseAndFlush(OK, OBJECT_MAPPER.writeValueAsBytes(hybridStoreQuotaStatusResponse), true, ctx);
  }

  private String getD2ServiceByClusterName(String clusterName) {
    return clusterToD2Map.get(clusterName);
  }

  private String getServerD2ServiceByClusterName(String clusterName) {
    return clusterToServerD2Map.get(clusterName);
  }

  private void checkResourceName(String resourceName, String path) {
    if (StringUtils.isEmpty(resourceName)) {
      throw new VeniceHttpException(BAD_REQUEST.code(), "Resource name required, valid path should be : " + path);
    }
  }

  private Optional<DerivedSchemaEntry> getLatestUpdateSchemaWithValueSchemaId(String storeName, int valueSchemaId) {
    DerivedSchemaEntry latestUpdateSchemaEntry = null;
    for (DerivedSchemaEntry entry: schemaRepo.getDerivedSchemas(storeName)) {
      if (entry.getValueSchemaID() == valueSchemaId) {
        if (latestUpdateSchemaEntry == null || entry.getId() > latestUpdateSchemaEntry.getId()) {
          latestUpdateSchemaEntry = entry;
        }
      }
    }
    return Optional.ofNullable(latestUpdateSchemaEntry);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) throws Exception {
    InetSocketAddress sockAddr = (InetSocketAddress) (ctx.channel().remoteAddress());
    String remoteAddr = sockAddr.getHostName() + ":" + sockAddr.getPort();
    if (!EXCEPTION_FILTER.isRedundantException(sockAddr.getHostName(), e)) {
      LOGGER.error("Got exception while handling meta data request from {}. ", remoteAddr, e);
    }

    try {
      if (ExceptionUtils.recursiveClassEquals(e, CertificateExpiredException.class)) {
        String errorMsg = "Your certificate has expired. Please renew.";
        setupResponseAndFlush(UNAUTHORIZED, errorMsg.getBytes(), false, ctx);
        LOGGER.info("Sent an error message to client about expired certificate");
      } else {
        String stackTraceStr = ExceptionUtils.stackTraceToString(e);
        setupResponseAndFlush(INTERNAL_SERVER_ERROR, stackTraceStr.getBytes(), false, ctx);
      }
    } catch (Exception ex) {
      LOGGER.error("Got exception while trying to send error response", ex);
    } finally {
      ctx.channel().close();
    }
  }
}
