package com.linkedin.venice.router;

import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponseV2;
import com.linkedin.venice.controllerapi.MasterControllerResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoHelixResourceException;
import com.linkedin.venice.helix.HelixHybridStoreQuotaRepository;
import com.linkedin.venice.helix.StoreJSONSerializer;
import com.linkedin.venice.helix.SystemStoreJSONSerializer;
import com.linkedin.venice.meta.OnlineInstanceFinder;
import com.linkedin.venice.meta.OnlineInstanceFinderDelegator;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreConfigRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.SystemStore;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.HybridStoreQuotaStatus;
import com.linkedin.venice.pushmonitor.PartitionStatusOnlineInstanceFinder;
import com.linkedin.venice.router.api.VenicePathParserHelper;
import com.linkedin.venice.routerapi.HybridStoreQuotaStatusResponse;
import com.linkedin.venice.routerapi.PushStatusResponse;
import com.linkedin.venice.routerapi.ReplicaState;
import com.linkedin.venice.routerapi.ResourceStateResponse;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.Utils;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.CertificateExpiredException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import static com.linkedin.venice.VeniceConstants.*;
import static com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponseV2.*;
import static com.linkedin.venice.router.api.VenicePathParser.*;
import static com.linkedin.venice.router.api.VenicePathParserHelper.*;
import static com.linkedin.venice.utils.NettyUtils.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;


/**
 * This MetaDataHandle is used to handle the following meta data requests:
 * 1. Controller lookup: /controller, and it will return master controller url as the content.
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
  private static final Logger logger = Logger.getLogger(MetaDataHandler.class);
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final StoreJSONSerializer storeSerializer = new StoreJSONSerializer();
  private static final SystemStoreJSONSerializer systemStoreSerializer = new SystemStoreJSONSerializer();
  private final RoutingDataRepository routing;
  private final ReadOnlySchemaRepository schemaRepo;
  private final ReadOnlyStoreConfigRepository storeConfigRepo;
  private final Map<String, String> clusterToD2Map;
  private final OnlineInstanceFinderDelegator onlineInstanceFinder;
  private final Optional<HelixHybridStoreQuotaRepository> hybridStoreQuotaRepository;
  private final ReadOnlyStoreRepository storeRepository;
  private final String clusterName;
  private final String zkAddress;
  private final String kafkaZkAddress;
  private final String kafkaBootstrapServers;

  private static RedundantExceptionFilter filter = RedundantExceptionFilter.getRedundantExceptionFilter();

  public MetaDataHandler(RoutingDataRepository routing, ReadOnlySchemaRepository schemaRepo,
      ReadOnlyStoreConfigRepository storeConfigRepo, Map<String, String> clusterToD2Map,
      OnlineInstanceFinderDelegator onlineInstanceFinder, ReadOnlyStoreRepository storeRepository,
      Optional<HelixHybridStoreQuotaRepository> hybridStoreQuotaRepository, String clusterName, String zkAddress,
      String kafkaZkAddress, String kafkaBootstrapServers) {
    super();
    this.routing = routing;
    this.schemaRepo = schemaRepo;
    this.storeConfigRepo = storeConfigRepo;
    this.clusterToD2Map = clusterToD2Map;
    this.onlineInstanceFinder = onlineInstanceFinder;
    this.hybridStoreQuotaRepository = hybridStoreQuotaRepository;
    this.storeRepository = storeRepository;
    this.clusterName = clusterName;
    this.zkAddress = zkAddress;
    this.kafkaZkAddress = kafkaZkAddress;
    this.kafkaBootstrapServers = kafkaBootstrapServers;
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpRequest req) throws IOException {
    VenicePathParserHelper helper = parseRequest(req);

    String resourceType = helper.getResourceType(); //may be null
    if (TYPE_MASTER_CONTROLLER.equals(resourceType)) {
      // URI: /master_controller
      handleControllerLookup(ctx);
    } else if (TYPE_KEY_SCHEMA.equals(resourceType)) {
      // URI: /key_schema/${storeName}
      // For key schema lookup, we only consider storeName
      handleKeySchemaLookup(ctx, helper);
    } else if (TYPE_VALUE_SCHEMA.equals(resourceType)) {
      // The request could fetch one value schema by id or all the value schema for the given store
      // URI: /value_schema/{$storeName} - Get all the value schema
      // URI: /value_schema/{$storeName}/{$valueSchemaId} - Get single value schema
      handleValueSchemaLookup(ctx, helper);
    } else if (TYPE_CLUSTER_DISCOVERY.equals(resourceType)) {
      // URI: /discover_cluster/${storeName}
      hanldeD2ServiceLookup(ctx, helper, req.headers());
    } else if (TYPE_RESOURCE_STATE.equals(resourceType)) {
      // URI: /resource_state
      handleResourceStateLookup(ctx, helper);
    } else if (TYPE_PUSH_STATUS.equals(resourceType)) {
      // URI: /push_status
      handlePushStatusLookUp(ctx, helper);
    } else if (TYPE_STREAM_HYBRID_STORE_QUOTA.equals(resourceType)) {
      handleStreamHybridStoreQuotaStatusLookup(ctx, helper);
    } else if (TYPE_STREAM_REPROCESSING_HYBRID_STORE_QUOTA.equals(resourceType)) {
      handleStreamReprocessingHybridStoreQuotaStatusLookup(ctx, helper);
    } else if (TYPE_STORE_STATE.equals(resourceType)) {
      handleStoreStateLookup(ctx, helper);
    } else {
      // SimpleChannelInboundHandler automatically releases the request after channelRead0 is done.
      // since we're passing it on to the next handler, we need to retain an extra reference.
      ReferenceCountUtil.retain(req);
      ctx.fireChannelRead(req);
      return;
    }
  }

  private void handleControllerLookup(ChannelHandlerContext ctx) throws IOException {
    MasterControllerResponse responseObject = new MasterControllerResponse();
    responseObject.setCluster(clusterName);
    responseObject.setUrl(routing.getMasterController().getUrl());
    logger.info(
        "For cluster " + responseObject.getCluster() + ", the master controller url is " + responseObject.getUrl()
            + ", last refreshed at " + routing.getMasterControllerChangeTimeMs());
    setupResponseAndFlush(OK, mapper.writeValueAsBytes(responseObject), true, ctx);
  }

  private void handleKeySchemaLookup(ChannelHandlerContext ctx, VenicePathParserHelper helper) throws IOException {
    String storeName = helper.getResourceName();
    checkResourceName(storeName, "/" + TYPE_KEY_SCHEMA + "/${storeName}");
    SchemaEntry keySchema = schemaRepo.getKeySchema(storeName);
    if (null == keySchema) {
      byte[] errBody = new String("Key schema for store: " + storeName + " doesn't exist").getBytes();
      setupResponseAndFlush(NOT_FOUND, errBody, false, ctx);
      return;
    }
    SchemaResponse responseObject = new SchemaResponse();
    responseObject.setCluster(clusterName);
    responseObject.setName(storeName);
    responseObject.setId(keySchema.getId());
    responseObject.setSchemaStr(keySchema.getSchema().toString());
    setupResponseAndFlush(OK, mapper.writeValueAsBytes(responseObject), true, ctx);
  }

  private void handleValueSchemaLookup(ChannelHandlerContext ctx, VenicePathParserHelper helper) throws IOException {
    String storeName = helper.getResourceName();
    checkResourceName(storeName,
        "/" + TYPE_VALUE_SCHEMA + "/${storeName} or /" + TYPE_VALUE_SCHEMA + "/${storeName}/${valueSchemaId}");
    String id = helper.getKey();
    if (null == id || id.isEmpty()) {
      // URI: /value_schema/{$storeName}
      // Get all the value schema
      MultiSchemaResponse responseObject = new MultiSchemaResponse();
      responseObject.setCluster(clusterName);
      responseObject.setName(storeName);
      int superSetSchemaId = storeRepository.getStore(storeName).getLatestSuperSetValueSchemaId();
      if (superSetSchemaId != -1) {
        responseObject.setSuperSetSchemaId(superSetSchemaId);
      }
      Collection<SchemaEntry> valueSchemaEntries = schemaRepo.getValueSchemas(storeName);
      int schemaNum = valueSchemaEntries.size();
      MultiSchemaResponse.Schema[] schemas = new MultiSchemaResponse.Schema[schemaNum];
      int cur = 0;
      for (SchemaEntry entry : valueSchemaEntries) {
        schemas[cur] = new MultiSchemaResponse.Schema();
        schemas[cur].setId(entry.getId());
        schemas[cur].setSchemaStr(entry.getSchema().toString());
        ++cur;
      }
      responseObject.setSchemas(schemas);
      setupResponseAndFlush(OK, mapper.writeValueAsBytes(responseObject), true, ctx);
    } else {
      // URI: /value_schema/{$storeName}/{$valueSchemaId}
      // Get single value schema
      SchemaResponse responseObject = new SchemaResponse();
      responseObject.setCluster(clusterName);
      responseObject.setName(storeName);
      SchemaEntry valueSchema = schemaRepo.getValueSchema(storeName, Integer.parseInt(id));
      if (null == valueSchema) {
        byte[] errBody =
            new String("Value schema doesn't exist for schema id: " + id + " of store: " + storeName).getBytes();
        setupResponseAndFlush(NOT_FOUND, errBody, false, ctx);
        return;
      }
      responseObject.setId(valueSchema.getId());
      responseObject.setSchemaStr(valueSchema.getSchema().toString());
      setupResponseAndFlush(OK, mapper.writeValueAsBytes(responseObject), true, ctx);
    }
  }

  private void hanldeD2ServiceLookup(ChannelHandlerContext ctx, VenicePathParserHelper helper, HttpHeaders headers)
      throws IOException {
    String storeName = helper.getResourceName();
    checkResourceName(storeName, "/" + TYPE_CLUSTER_DISCOVERY + "/${storeName}");
    Optional<StoreConfig> config = storeConfigRepo.getStoreConfig(storeName);
    if (!config.isPresent() || Utils.isNullOrEmpty(config.get().getCluster())) {
      String errorMsg = "Cluster for store: " + storeName + " doesn't exist";
      setupErrorD2DiscoveryResponseAndFlush(NOT_FOUND, errorMsg, headers, ctx);
      return;
    }
    String clusterName = config.get().getCluster();
    String d2Service = getD2ServiceByClusterName(clusterName);
    if (Utils.isNullOrEmpty(d2Service)) {
      String errorMsg = "D2 service for store: " + storeName + " doesn't exist";
      setupErrorD2DiscoveryResponseAndFlush(NOT_FOUND, errorMsg, headers, ctx);
      return;
    }
    if (headers.contains(D2_SERVICE_DISCOVERY_RESPONSE_V2_ENABLED)) {
      D2ServiceDiscoveryResponseV2 responseObject = new D2ServiceDiscoveryResponseV2();
      responseObject.setCluster(config.get().getCluster());
      responseObject.setName(config.get().getStoreName());
      responseObject.setD2Service(d2Service);
      responseObject.setZkAddress(zkAddress);
      responseObject.setKafkaZkAddress(kafkaZkAddress);
      responseObject.setKafkaBootstrapServers(kafkaBootstrapServers);
      setupResponseAndFlush(OK, mapper.writeValueAsBytes(responseObject), true, ctx);
    } else {
      D2ServiceDiscoveryResponse responseObject = new D2ServiceDiscoveryResponse();
      responseObject.setCluster(config.get().getCluster());
      responseObject.setName(config.get().getStoreName());
      responseObject.setD2Service(d2Service);
      setupResponseAndFlush(OK, mapper.writeValueAsBytes(responseObject), true, ctx);
    }
  }

  private void setupErrorD2DiscoveryResponseAndFlush(HttpResponseStatus status, String errorMsg, HttpHeaders headers,
      ChannelHandlerContext ctx) throws IOException {
    if (headers.contains(D2_SERVICE_DISCOVERY_RESPONSE_V2_ENABLED)) {
      D2ServiceDiscoveryResponseV2 responseObject = new D2ServiceDiscoveryResponseV2();
      responseObject.setError(errorMsg);
      setupResponseAndFlush(status, mapper.writeValueAsBytes(responseObject), true, ctx);
    } else {
      D2ServiceDiscoveryResponse responseObject = new D2ServiceDiscoveryResponse();
      responseObject.setError(errorMsg);
      setupResponseAndFlush(status, mapper.writeValueAsBytes(responseObject), true, ctx);
    }
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
    for (int p = 0; p < onlineInstanceFinder.getNumberOfPartitions(resourceName); p++) {
      try {
        partitionReplicaStates = onlineInstanceFinder.getReplicaStates(resourceName, p);
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
        isResourceReadyToServe = partitionReplicaStates.stream().filter(ReplicaState::isReadyToServe).count() > (
            partitionReplicaStates.size() / 2);
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
    setupResponseAndFlush(OK, mapper.writeValueAsBytes(response), true, ctx);
  }

  /**
   * Get push status from {@link PartitionStatusOnlineInstanceFinder} for stores running in L/F mode.
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

    PushStatusResponse pushStatusResponse = new PushStatusResponse();
    pushStatusResponse.setName(resourceName);
    OnlineInstanceFinder instanceFinder = onlineInstanceFinder.getInstanceFinder(resourceName);
    if (!(instanceFinder instanceof PartitionStatusOnlineInstanceFinder)) {
      pushStatusResponse.setError("Only support getting push status for stores running in Leader/Follower mode");
      setupResponseAndFlush(BAD_REQUEST, mapper.writeValueAsBytes(pushStatusResponse), true, ctx);
      return;
    }

    PartitionStatusOnlineInstanceFinder onlineInstanceFinder = (PartitionStatusOnlineInstanceFinder) instanceFinder;
    pushStatusResponse.setExecutionStatus(onlineInstanceFinder.getPushJobStatus(resourceName));
    setupResponseAndFlush(OK, mapper.writeValueAsBytes(pushStatusResponse), true, ctx);
  }

  /**
   * Get hybrid store quota status from {@link HelixHybridStoreQuotaRepository} for stores.
   */
  private void handleStreamHybridStoreQuotaStatusLookup(ChannelHandlerContext ctx, VenicePathParserHelper helper)
      throws IOException {
    String storeName = helper.getResourceName();
    checkResourceName(storeName, "/" + TYPE_STREAM_HYBRID_STORE_QUOTA + "/${storeName}");
    if (!storeConfigRepo.getStoreConfig(storeName).isPresent()) {
      byte[] errBody =
          ("Cannot fetch the hybrid store quota status for store: " + storeName + " because the store: " + storeName
              + " cannot be found").getBytes();
      setupResponseAndFlush(NOT_FOUND, errBody, false, ctx);
      return;
    }
    String topicName = Version.composeKafkaTopic(storeName, storeRepository.getStore(storeName).getCurrentVersion());
    prepareHybridStoreQuotaStatusResponse(topicName, ctx);
  }

  /**
   * Get hybrid store quota status from {@link HelixHybridStoreQuotaRepository} for stores.
   */
  private void handleStreamReprocessingHybridStoreQuotaStatusLookup(ChannelHandlerContext ctx,
      VenicePathParserHelper helper) throws IOException {
    String resourceName = helper.getResourceName();
    checkResourceName(resourceName, "/" + TYPE_STREAM_REPROCESSING_HYBRID_STORE_QUOTA + "/${resourceName}");
    if (!storeConfigRepo.getStoreConfig(Version.parseStoreFromKafkaTopicName(resourceName)).isPresent()) {
      byte[] errBody =
          ("Cannot fetch the hybrid store quota status for resource: " + resourceName + " because the store: " + Version
              .parseStoreFromKafkaTopicName(resourceName) + " cannot be found").getBytes();
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
      byte[] errBody =
          ("Cannot fetch the store state for store: " + storeName + " because the store cannot be found in cluster: "
              + clusterName).getBytes();
      setupResponseAndFlush(NOT_FOUND, errBody, false, ctx);
      return;
    }
    byte[] body;
    if (store instanceof SystemStore) {
      SystemStore systemStore = (SystemStore) store;
      body = systemStoreSerializer.serialize(systemStore.getSerializableSystemStore(), null);
    } else {
      body = storeSerializer.serialize(store, null);
    }
    setupResponseAndFlush(OK, body, true, ctx);
  }

  private void prepareHybridStoreQuotaStatusResponse(String resourceName, ChannelHandlerContext ctx)
      throws IOException {
    HybridStoreQuotaStatusResponse hybridStoreQuotaStatusResponse = new HybridStoreQuotaStatusResponse();
    hybridStoreQuotaStatusResponse.setName(resourceName);
    if (hybridStoreQuotaRepository.isPresent()) {
      hybridStoreQuotaStatusResponse.setQuotaStatus(
          hybridStoreQuotaRepository.get().getHybridStoreQuotaStatus(resourceName));
    } else {
      hybridStoreQuotaStatusResponse.setQuotaStatus(HybridStoreQuotaStatus.UNKNOWN);
    }
    setupResponseAndFlush(OK, mapper.writeValueAsBytes(hybridStoreQuotaStatusResponse), true, ctx);
  }

  private String getD2ServiceByClusterName(String clusterName) {
    return clusterToD2Map.get(clusterName);
  }

  private void checkResourceName(String resourceName, String path) {
    if (Utils.isNullOrEmpty(resourceName)) {
      throw new VeniceException("Resource name required, valid path should be : " + path);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) throws Exception {
    InetSocketAddress sockAddr = (InetSocketAddress) (ctx.channel().remoteAddress());
    String remoteAddr = sockAddr.getHostName() + ":" + sockAddr.getPort();
    if (!filter.isRedundantException(sockAddr.getHostName(), e)) {
      logger.error("Got exception while handling meta data request from " + remoteAddr + ": ", e);
    }

    try {
      if (ExceptionUtils.recursiveClassEquals(e, CertificateExpiredException.class)) {
        String errorMsg = "Your certificate has expired. Please renew.";
        setupResponseAndFlush(UNAUTHORIZED, errorMsg.getBytes(), false, ctx);
        logger.info("Sent an error message to client about expired certificate");
      } else {
        String stackTraceStr = ExceptionUtils.stackTraceToString(e);
        setupResponseAndFlush(INTERNAL_SERVER_ERROR, stackTraceStr.getBytes(), false, ctx);
      }
    } catch (Exception ex) {
      logger.error("Got exception while trying to send error response", ex);
    } finally {
      ctx.channel().close();
    }
  }
}
