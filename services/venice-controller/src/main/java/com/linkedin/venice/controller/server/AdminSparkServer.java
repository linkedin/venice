package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerRoute.ABORT_MIGRATION;
import static com.linkedin.venice.controllerapi.ControllerRoute.ADD_DERIVED_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerRoute.ADD_VALUE_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerRoute.ADD_VERSION;
import static com.linkedin.venice.controllerapi.ControllerRoute.ALLOW_LIST_ADD_NODE;
import static com.linkedin.venice.controllerapi.ControllerRoute.ALLOW_LIST_REMOVE_NODE;
import static com.linkedin.venice.controllerapi.ControllerRoute.BACKUP_VERSION;
import static com.linkedin.venice.controllerapi.ControllerRoute.CHECK_RESOURCE_CLEANUP_FOR_STORE_CREATION;
import static com.linkedin.venice.controllerapi.ControllerRoute.CLEANUP_INSTANCE_CUSTOMIZED_STATES;
import static com.linkedin.venice.controllerapi.ControllerRoute.CLUSTER_DISCOVERY;
import static com.linkedin.venice.controllerapi.ControllerRoute.CLUSTER_HEALTH_STORES;
import static com.linkedin.venice.controllerapi.ControllerRoute.COMPARE_STORE;
import static com.linkedin.venice.controllerapi.ControllerRoute.COMPLETE_MIGRATION;
import static com.linkedin.venice.controllerapi.ControllerRoute.CONFIGURE_ACTIVE_ACTIVE_REPLICATION_FOR_CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerRoute.CREATE_STORAGE_PERSONA;
import static com.linkedin.venice.controllerapi.ControllerRoute.ClUSTER_HEALTH_INSTANCES;
import static com.linkedin.venice.controllerapi.ControllerRoute.DATA_RECOVERY;
import static com.linkedin.venice.controllerapi.ControllerRoute.DELETE_ACL;
import static com.linkedin.venice.controllerapi.ControllerRoute.DELETE_ALL_VERSIONS;
import static com.linkedin.venice.controllerapi.ControllerRoute.DELETE_KAFKA_TOPIC;
import static com.linkedin.venice.controllerapi.ControllerRoute.DELETE_OLD_VERSION;
import static com.linkedin.venice.controllerapi.ControllerRoute.DELETE_STORAGE_PERSONA;
import static com.linkedin.venice.controllerapi.ControllerRoute.DELETE_STORE;
import static com.linkedin.venice.controllerapi.ControllerRoute.EMPTY_PUSH;
import static com.linkedin.venice.controllerapi.ControllerRoute.ENABLE_MAX_CAPACITY_PROTECTION;
import static com.linkedin.venice.controllerapi.ControllerRoute.ENABLE_QUOTA_REBALANCED;
import static com.linkedin.venice.controllerapi.ControllerRoute.ENABLE_STORE;
import static com.linkedin.venice.controllerapi.ControllerRoute.ENABLE_THROTTLING;
import static com.linkedin.venice.controllerapi.ControllerRoute.END_OF_PUSH;
import static com.linkedin.venice.controllerapi.ControllerRoute.EXECUTION;
import static com.linkedin.venice.controllerapi.ControllerRoute.FUTURE_VERSION;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_ACL;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_ADMIN_TOPIC_METADATA;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_ALL_MIGRATION_PUSH_STRATEGIES;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_ALL_REPLICATION_METADATA_SCHEMAS;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_ALL_VALUE_AND_DERIVED_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_ALL_VALUE_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_CLUSTER_STORAGE_PERSONAS;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_DELETABLE_STORE_TOPICS;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_HEARTBEAT_TIMESTAMP_FROM_SYSTEM_STORE;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_INUSE_SCHEMA_IDS;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_KAFKA_TOPIC_CONFIGS;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_KEY_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_ONGOING_INCREMENTAL_PUSH_VERSIONS;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_REGION_PUSH_DETAILS;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_REPUSH_INFO;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_ROUTERS_CLUSTER_CONFIG;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_STALE_STORES_IN_CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_STORAGE_PERSONA;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_STORAGE_PERSONA_ASSOCIATED_WITH_STORE;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_STORES_IN_CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_STORE_LARGEST_USED_VERSION;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_VALUE_OR_DERIVED_SCHEMA_ID;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_VALUE_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_VALUE_SCHEMA_ID;
import static com.linkedin.venice.controllerapi.ControllerRoute.IS_STORE_VERSION_READY_FOR_DATA_RECOVERY;
import static com.linkedin.venice.controllerapi.ControllerRoute.JOB;
import static com.linkedin.venice.controllerapi.ControllerRoute.KILL_OFFLINE_PUSH_JOB;
import static com.linkedin.venice.controllerapi.ControllerRoute.LAST_SUCCEED_EXECUTION_ID;
import static com.linkedin.venice.controllerapi.ControllerRoute.LEADER_CONTROLLER;
import static com.linkedin.venice.controllerapi.ControllerRoute.LIST_BOOTSTRAPPING_VERSIONS;
import static com.linkedin.venice.controllerapi.ControllerRoute.LIST_CHILD_CLUSTERS;
import static com.linkedin.venice.controllerapi.ControllerRoute.LIST_NODES;
import static com.linkedin.venice.controllerapi.ControllerRoute.LIST_REPLICAS;
import static com.linkedin.venice.controllerapi.ControllerRoute.LIST_STORES;
import static com.linkedin.venice.controllerapi.ControllerRoute.LIST_STORE_PUSH_INFO;
import static com.linkedin.venice.controllerapi.ControllerRoute.MASTER_CONTROLLER;
import static com.linkedin.venice.controllerapi.ControllerRoute.MIGRATE_STORE;
import static com.linkedin.venice.controllerapi.ControllerRoute.NEW_STORE;
import static com.linkedin.venice.controllerapi.ControllerRoute.NODE_REMOVABLE;
import static com.linkedin.venice.controllerapi.ControllerRoute.NODE_REPLICAS;
import static com.linkedin.venice.controllerapi.ControllerRoute.NODE_REPLICAS_READINESS;
import static com.linkedin.venice.controllerapi.ControllerRoute.OFFLINE_PUSH_INFO;
import static com.linkedin.venice.controllerapi.ControllerRoute.PREPARE_DATA_RECOVERY;
import static com.linkedin.venice.controllerapi.ControllerRoute.REMOVE_DERIVED_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerRoute.REMOVE_NODE;
import static com.linkedin.venice.controllerapi.ControllerRoute.REMOVE_STORE_FROM_GRAVEYARD;
import static com.linkedin.venice.controllerapi.ControllerRoute.REPLICATE_META_DATA;
import static com.linkedin.venice.controllerapi.ControllerRoute.REQUEST_TOPIC;
import static com.linkedin.venice.controllerapi.ControllerRoute.ROLLBACK_TO_BACKUP_VERSION;
import static com.linkedin.venice.controllerapi.ControllerRoute.ROLL_FORWARD_TO_FUTURE_VERSION;
import static com.linkedin.venice.controllerapi.ControllerRoute.SEND_HEARTBEAT_TIMESTAMP_TO_SYSTEM_STORE;
import static com.linkedin.venice.controllerapi.ControllerRoute.SEND_PUSH_JOB_DETAILS;
import static com.linkedin.venice.controllerapi.ControllerRoute.SET_MIGRATION_PUSH_STRATEGY;
import static com.linkedin.venice.controllerapi.ControllerRoute.SET_OWNER;
import static com.linkedin.venice.controllerapi.ControllerRoute.SET_PARTITION_COUNT;
import static com.linkedin.venice.controllerapi.ControllerRoute.SET_TOPIC_COMPACTION;
import static com.linkedin.venice.controllerapi.ControllerRoute.SET_VERSION;
import static com.linkedin.venice.controllerapi.ControllerRoute.SKIP_ADMIN;
import static com.linkedin.venice.controllerapi.ControllerRoute.STORAGE_ENGINE_OVERHEAD_RATIO;
import static com.linkedin.venice.controllerapi.ControllerRoute.STORE;
import static com.linkedin.venice.controllerapi.ControllerRoute.STORE_MIGRATION_ALLOWED;
import static com.linkedin.venice.controllerapi.ControllerRoute.UPDATE_ACL;
import static com.linkedin.venice.controllerapi.ControllerRoute.UPDATE_ADMIN_TOPIC_METADATA;
import static com.linkedin.venice.controllerapi.ControllerRoute.UPDATE_CLUSTER_CONFIG;
import static com.linkedin.venice.controllerapi.ControllerRoute.UPDATE_KAFKA_TOPIC_LOG_COMPACTION;
import static com.linkedin.venice.controllerapi.ControllerRoute.UPDATE_KAFKA_TOPIC_MIN_IN_SYNC_REPLICA;
import static com.linkedin.venice.controllerapi.ControllerRoute.UPDATE_KAFKA_TOPIC_RETENTION;
import static com.linkedin.venice.controllerapi.ControllerRoute.UPDATE_STORAGE_PERSONA;
import static com.linkedin.venice.controllerapi.ControllerRoute.UPDATE_STORE;
import static com.linkedin.venice.controllerapi.ControllerRoute.UPLOAD_PUSH_JOB_STATUS;
import static com.linkedin.venice.controllerapi.ControllerRoute.WIPE_CLUSTER;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.AuditInfo;
import com.linkedin.venice.controller.spark.VeniceSparkServerFactory;
import com.linkedin.venice.controller.stats.SparkServerStats;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import spark.Request;
import spark.Response;
import spark.Service;
import spark.embeddedserver.EmbeddedServers;


/**
 * Controller admin API leveraging sparkjava: http://sparkjava.com/documentation.html
 * <p>
 * AdminSparkServer is shared by multiple clusters' controllers running in one physical Venice controller instance.
 */
public class AdminSparkServer extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(AdminSparkServer.class);

  private final int port;
  private final Admin admin;
  private final boolean enforceSSL;
  private final boolean sslEnabled;
  private final boolean checkReadMethodForKafka;
  private final Optional<SSLConfig> sslConfig;
  private final Optional<DynamicAccessController> accessController;

  protected static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
  final private Map<String, SparkServerStats> statsMap;
  final private SparkServerStats nonclusterSpecificStats;

  private static String REQUEST_START_TIME = "startTime";
  private static String REQUEST_SUCCEED = "succeed";

  // In order to build multiple controller in a single JVM, we create a new http service instance for each of
  // AdminSparkServer instance.
  private final Service httpService;

  private final List<ControllerRoute> disabledRoutes;

  private final boolean disableParentRequestTopicForStreamPushes;
  private final PubSubTopicRepository pubSubTopicRepository;

  private final VeniceControllerRequestHandler requestHandler;

  public AdminSparkServer(
      int port,
      Admin admin,
      MetricsRepository metricsRepository,
      Set<String> clusters,
      boolean enforceSSL,
      Optional<SSLConfig> sslConfig,
      boolean checkReadMethodForKafka,
      Optional<DynamicAccessController> accessController,
      List<ControllerRoute> disabledRoutes,
      VeniceProperties jettyConfigOverrides,
      boolean disableParentRequestTopicForStreamPushes,
      PubSubTopicRepository pubSubTopicRepository,
      VeniceControllerRequestHandler requestHandler) {
    this.port = port;
    this.enforceSSL = enforceSSL;
    this.sslEnabled = sslConfig.isPresent();
    this.sslConfig = sslConfig;
    this.checkReadMethodForKafka = checkReadMethodForKafka;
    this.accessController = accessController;
    // Note: admin is passed in as a reference. The expectation is the source of the admin will
    // close it so we don't close it in stopInner()
    this.admin = admin;
    statsMap = new HashMap<>(clusters.size());
    String statsPrefix = sslEnabled ? "secure_" : "";
    for (String cluster: clusters) {
      statsMap.put(
          cluster,
          new SparkServerStats(metricsRepository, cluster + "." + statsPrefix + "controller_spark_server"));
    }
    nonclusterSpecificStats = new SparkServerStats(metricsRepository, "." + statsPrefix + "controller_spark_server");
    EmbeddedServers.add(EmbeddedServers.Identifiers.JETTY, new VeniceSparkServerFactory(jettyConfigOverrides));

    httpService = Service.ignite();
    this.disabledRoutes = disabledRoutes;
    this.disableParentRequestTopicForStreamPushes = disableParentRequestTopicForStreamPushes;
    this.pubSubTopicRepository = pubSubTopicRepository;
    this.requestHandler = requestHandler;
  }

  @Override
  public boolean startInner() throws Exception {
    AtomicReference<Exception> initFailure = new AtomicReference<>();
    httpService.initExceptionHandler(initFailure::set);

    httpService.port(port);

    if (sslEnabled) {
      SSLConfig config = sslConfig.get();
      httpService.secure(
          config.getSslKeyStoreLocation(),
          config.getSslKeyStorePassword(),
          null,
          config.getSslTrustStoreLocation(),
          config.getSslTrustStorePassword(),
          config.isSslNeedsClientCert());
    }

    httpService.before((request, response) -> {
      AuditInfo audit = new AuditInfo(request);
      LOGGER.info(audit.toString());
      SparkServerStats stats = statsMap.get(request.queryParams(CLUSTER));
      if (stats == null) {
        stats = nonclusterSpecificStats;
      }
      stats.recordRequest();
      /**
       * If SSL is enforced, there is nothing to do in the secure admin server which has SSL enabled already;
       * but in the insecure admin server, we need to fail most of the routes except cluster/leader-controller
       * discovery.
       *
       * TODO: Currently we allow insecure access to cluster/leader-controller discovery because D2Client inside
       *       VeniceSystemProducer is not secure yet; once the new D2Client is used everywhere, we are safe to
       *       switch the controller D2 announcement result to the secure URL and stop insecure access to
       *       cluster/leader-controller discovery.
       */
      if (enforceSSL && !sslEnabled) {
        if (!CLUSTER_DISCOVERY.pathEquals(request.uri()) && !LEADER_CONTROLLER.pathEquals(request.uri())
            && !MASTER_CONTROLLER.pathEquals(request.uri())) {
          httpService.halt(403, "Access denied, Venice Controller has enforced SSL.");
        }
      }
      request.attribute(REQUEST_START_TIME, System.currentTimeMillis());
      request.attribute(REQUEST_SUCCEED, true);
    });

    // filter for blocked api calls
    httpService.before((request, response) -> {
      if (disabledRoutes.contains(ControllerRoute.valueOfPath(request.uri()))) {
        httpService.halt(403, String.format("Route %s has been disabled in venice controller config!!", request.uri()));
      }
    });

    httpService.after((request, response) -> {
      AuditInfo audit = new AuditInfo(request);
      SparkServerStats stats = statsMap.get(request.queryParams(CLUSTER));
      if (stats == null) {
        stats = nonclusterSpecificStats;
      }
      long latency = System.currentTimeMillis() - (long) request.attribute(REQUEST_START_TIME);
      if ((boolean) request.attribute(REQUEST_SUCCEED)) {
        LOGGER.info(audit.successString());
        stats.recordSuccessfulRequestLatency(latency);
      } else {
        LOGGER.info(audit.failureString(response.body()));
        stats.recordFailedRequestLatency(latency);
      }
    });

    // Build all different routes
    ControllerRoutes controllerRoutes =
        new ControllerRoutes(sslEnabled, accessController, pubSubTopicRepository, requestHandler);
    StoresRoutes storesRoutes = new StoresRoutes(sslEnabled, accessController, pubSubTopicRepository, requestHandler);
    JobRoutes jobRoutes = new JobRoutes(sslEnabled, accessController, requestHandler);
    SkipAdminRoute skipAdminRoute = new SkipAdminRoute(sslEnabled, accessController, requestHandler);
    CreateVersion createVersion = new CreateVersion(
        sslEnabled,
        accessController,
        this.checkReadMethodForKafka,
        disableParentRequestTopicForStreamPushes,
        requestHandler);
    CreateStore createStoreRoute = new CreateStore(sslEnabled, accessController, requestHandler);
    NodesAndReplicas nodesAndReplicas = new NodesAndReplicas(sslEnabled, accessController, requestHandler);
    SchemaRoutes schemaRoutes = new SchemaRoutes(sslEnabled, accessController, requestHandler);
    AdminCommandExecutionRoutes adminCommandExecutionRoutes =
        new AdminCommandExecutionRoutes(sslEnabled, accessController, requestHandler);
    RoutersClusterConfigRoutes routersClusterConfigRoutes =
        new RoutersClusterConfigRoutes(sslEnabled, accessController, requestHandler);
    MigrationRoutes migrationRoutes = new MigrationRoutes(sslEnabled, accessController, requestHandler);
    VersionRoute versionRoute = new VersionRoute(sslEnabled, accessController, requestHandler);
    ClusterRoutes clusterRoutes = new ClusterRoutes(sslEnabled, accessController, requestHandler);
    NewClusterBuildOutRoutes newClusterBuildOutRoutes =
        new NewClusterBuildOutRoutes(sslEnabled, accessController, requestHandler);
    DataRecoveryRoutes dataRecoveryRoutes = new DataRecoveryRoutes(sslEnabled, accessController, requestHandler);
    AdminTopicMetadataRoutes adminTopicMetadataRoutes =
        new AdminTopicMetadataRoutes(sslEnabled, accessController, requestHandler);
    StoragePersonaRoutes storagePersonaRoutes = new StoragePersonaRoutes(sslEnabled, accessController, requestHandler);

    httpService.get(SET_VERSION.getPath(), (request, response) -> {
      response.type(HttpConstants.TEXT_HTML);
      return writeMenu("Set Active Version", SET_VERSION.getPath(), SET_VERSION.getParams());
    });

    httpService.get(
        LIST_CHILD_CLUSTERS.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, controllerRoutes.getChildControllers(admin)));
    httpService.get(
        LIST_STORES.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.getAllStores(admin)));
    httpService.get(
        CLUSTER_HEALTH_STORES.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.getAllStoresStatuses(admin)));
    httpService.get(STORE.getPath(), new VeniceParentControllerRegionStateHandler(admin, storesRoutes.getStore(admin)));
    httpService.get(
        FUTURE_VERSION.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.getFutureVersion(admin)));
    httpService.get(
        BACKUP_VERSION.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.getBackupVersion(admin)));
    httpService.post(
        SET_TOPIC_COMPACTION.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.setTopicCompaction(admin)));

    httpService.post(
        UPDATE_CLUSTER_CONFIG.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, clusterRoutes.updateClusterConfig(admin)));
    httpService.post(
        WIPE_CLUSTER.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, clusterRoutes.wipeCluster(admin)));
    httpService.post(
        REPLICATE_META_DATA.getPath(),
        new VeniceParentControllerRegionStateHandler(
            admin,
            newClusterBuildOutRoutes.copyOverStoreSchemasAndConfigs(admin)));

    httpService.get(JOB.getPath(), new VeniceParentControllerRegionStateHandler(admin, jobRoutes.jobStatus(admin)));
    httpService.post(
        KILL_OFFLINE_PUSH_JOB.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, jobRoutes.killOfflinePushJob(admin)));
    httpService.post(
        SKIP_ADMIN.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, skipAdminRoute.skipAdminMessage(admin)));

    httpService.post(
        EMPTY_PUSH.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, createVersion.emptyPush(admin)));
    httpService.post(
        END_OF_PUSH.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, createVersion.writeEndOfPush(admin)));
    httpService.post(
        REQUEST_TOPIC.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, createVersion.requestTopicForPushing(admin)));
    httpService.post(
        ADD_VERSION.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, createVersion.addVersionAndStartIngestion(admin)));
    httpService.post(
        NEW_STORE.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, createStoreRoute.createStore(admin)));
    httpService.get(
        CHECK_RESOURCE_CLEANUP_FOR_STORE_CREATION.getPath(),
        new VeniceParentControllerRegionStateHandler(
            admin,
            createStoreRoute.checkResourceCleanupForStoreCreation(admin)));
    httpService.post(
        DELETE_STORE.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.deleteStore(admin)));
    httpService.post(
        UPDATE_STORE.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.updateStore(admin)));

    httpService.get(
        STORE_MIGRATION_ALLOWED.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, clusterRoutes.isStoreMigrationAllowed(admin)));
    httpService.post(
        MIGRATE_STORE.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.migrateStore(admin)));
    httpService.post(
        COMPLETE_MIGRATION.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.completeMigration(admin)));
    httpService.post(
        ABORT_MIGRATION.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.abortMigration(admin)));

    httpService.post(
        ENABLE_STORE.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.enableStore(admin)));
    httpService.post(
        DELETE_ALL_VERSIONS.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.deleteAllVersions(admin)));
    httpService.post(
        DELETE_OLD_VERSION.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.deleteOldVersions(admin)));
    httpService.post(
        SET_VERSION.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.setCurrentVersion(admin)));
    httpService.post(
        ROLLBACK_TO_BACKUP_VERSION.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.rollbackToBackupVersion(admin)));
    httpService.post(
        ROLL_FORWARD_TO_FUTURE_VERSION.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.rollForwardToFutureVersion(admin)));
    httpService.post(
        SEND_HEARTBEAT_TIMESTAMP_TO_SYSTEM_STORE.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.sendHeartbeatToSystemStore(admin)));
    httpService.get(
        GET_HEARTBEAT_TIMESTAMP_FROM_SYSTEM_STORE.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.getHeartbeatFromSystemStore(admin)));
    httpService.get(
        ClUSTER_HEALTH_INSTANCES.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, nodesAndReplicas.listAllNodesStatus(admin)));
    httpService.get(
        LIST_NODES.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, nodesAndReplicas.listAllNodes(admin)));
    httpService.get(
        LIST_REPLICAS.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, nodesAndReplicas.listReplicasForStore(admin)));
    httpService.get(
        NODE_REPLICAS.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, nodesAndReplicas.listReplicasForStorageNode(admin)));
    httpService.get(
        NODE_REMOVABLE.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, nodesAndReplicas.isNodeRemovable(admin)));
    httpService.get(
        NODE_REPLICAS_READINESS.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, nodesAndReplicas.nodeReplicasReadiness(admin)));
    httpService.post(
        ALLOW_LIST_ADD_NODE.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, nodesAndReplicas.addNodeIntoAllowList(admin)));
    httpService.post(
        ALLOW_LIST_REMOVE_NODE.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, nodesAndReplicas.removeNodeFromAllowList(admin)));
    httpService.post(
        REMOVE_NODE.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, nodesAndReplicas.removeNodeFromCluster(admin)));

    // Operations for key schema/value schema
    httpService.get(
        GET_KEY_SCHEMA.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, schemaRoutes.getKeySchema(admin)));
    httpService.post(
        ADD_VALUE_SCHEMA.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, schemaRoutes.addValueSchema(admin)));
    httpService.post(
        ADD_DERIVED_SCHEMA.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, schemaRoutes.addDerivedSchema(admin)));
    httpService.get(
        GET_VALUE_SCHEMA.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, schemaRoutes.getValueSchema(admin)));
    httpService.post(
        GET_VALUE_SCHEMA_ID.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, schemaRoutes.getValueSchemaID(admin)));
    httpService.post(
        GET_VALUE_OR_DERIVED_SCHEMA_ID.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, schemaRoutes.getValueOrDerivedSchemaID(admin)));
    httpService.get(
        GET_ALL_VALUE_SCHEMA.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, schemaRoutes.getAllValueSchema(admin)));
    httpService.get(
        GET_ALL_VALUE_AND_DERIVED_SCHEMA.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, schemaRoutes.getAllValueAndDerivedSchema(admin)));
    httpService.post(
        REMOVE_DERIVED_SCHEMA.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, schemaRoutes.removeDerivedSchema(admin)));
    httpService.get(
        GET_ALL_REPLICATION_METADATA_SCHEMAS.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, schemaRoutes.getAllReplicationMetadataSchemas(admin)));

    httpService
        .post(SET_OWNER.getPath(), new VeniceParentControllerRegionStateHandler(admin, storesRoutes.setOwner(admin)));
    httpService.post(
        SET_PARTITION_COUNT.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.setPartitionCount(admin)));

    httpService.get(
        MASTER_CONTROLLER.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, controllerRoutes.getLeaderController(admin)));
    // This API should be used by CORP controller only. VPJ could talk to any of controllers in CORP to find who is the
    // current leader CORP controller. In other colos, router will find the leader controller instead of calling this
    // API.
    httpService.get(
        LEADER_CONTROLLER.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, controllerRoutes.getLeaderController(admin)));

    httpService.get(
        EXECUTION.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, adminCommandExecutionRoutes.getExecution(admin)));
    httpService.get(
        LAST_SUCCEED_EXECUTION_ID.getPath(),
        new VeniceParentControllerRegionStateHandler(
            admin,
            adminCommandExecutionRoutes.getLastSucceedExecutionId(admin)));

    httpService.get(
        STORAGE_ENGINE_OVERHEAD_RATIO.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.getStorageEngineOverheadRatio(admin)));

    httpService.post(
        ENABLE_THROTTLING.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, routersClusterConfigRoutes.enableThrottling(admin)));
    httpService.post(
        ENABLE_MAX_CAPACITY_PROTECTION.getPath(),
        new VeniceParentControllerRegionStateHandler(
            admin,
            routersClusterConfigRoutes.enableMaxCapacityProtection(admin)));
    httpService.post(
        ENABLE_QUOTA_REBALANCED.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, routersClusterConfigRoutes.enableQuotaRebalanced(admin)));

    httpService.get(
        GET_ROUTERS_CLUSTER_CONFIG.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, routersClusterConfigRoutes.getRoutersClusterConfig(admin)));

    httpService.get(
        GET_ALL_MIGRATION_PUSH_STRATEGIES.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, migrationRoutes.getAllMigrationPushStrategies(admin)));
    httpService.get(
        SET_MIGRATION_PUSH_STRATEGY.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, migrationRoutes.setMigrationPushStrategy(admin)));

    httpService.get(
        CLUSTER_DISCOVERY.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, ClusterDiscovery.discoverCluster(requestHandler, admin)));
    httpService.get(
        LIST_BOOTSTRAPPING_VERSIONS.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, versionRoute.listBootstrappingVersions(admin)));

    httpService.post(
        OFFLINE_PUSH_INFO.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, createVersion.uploadPushInfo(admin)));

    httpService.post(
        UPLOAD_PUSH_JOB_STATUS.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, jobRoutes.uploadPushJobStatus(admin)));
    httpService.post(
        SEND_PUSH_JOB_DETAILS.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, jobRoutes.sendPushJobDetails(admin)));
    httpService.post(
        CONFIGURE_ACTIVE_ACTIVE_REPLICATION_FOR_CLUSTER.getPath(),
        new VeniceParentControllerRegionStateHandler(
            admin,
            storesRoutes.enableActiveActiveReplicationForCluster(admin)));
    httpService.post(
        UPDATE_ACL.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, createStoreRoute.updateAclForStore(admin)));
    httpService.get(
        GET_ACL.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, createStoreRoute.getAclForStore(admin)));
    httpService.get(
        DELETE_ACL.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, createStoreRoute.deleteAclForStore(admin)));
    httpService.get(
        GET_DELETABLE_STORE_TOPICS.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.getDeletableStoreTopics(admin)));
    httpService.get(
        GET_ONGOING_INCREMENTAL_PUSH_VERSIONS.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, jobRoutes.getOngoingIncrementalPushVersions(admin)));
    httpService.get(
        GET_REPUSH_INFO.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.getRepushInfo(admin)));
    httpService.get(
        COMPARE_STORE.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.compareStore(admin)));
    httpService.get(
        GET_STALE_STORES_IN_CLUSTER.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.getStaleStoresInCluster(admin)));
    httpService.get(
        GET_STORES_IN_CLUSTER.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.getStoresInCluster(admin)));
    httpService.get(
        GET_STORE_LARGEST_USED_VERSION.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.getStoreLargestUsedVersion(admin)));
    httpService.get(
        GET_REGION_PUSH_DETAILS.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.getRegionPushDetails(admin)));
    httpService.get(
        LIST_STORE_PUSH_INFO.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.listStorePushInfo(admin)));

    httpService.post(
        PREPARE_DATA_RECOVERY.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, dataRecoveryRoutes.prepareDataRecovery(admin)));
    httpService.get(
        IS_STORE_VERSION_READY_FOR_DATA_RECOVERY.getPath(),
        new VeniceParentControllerRegionStateHandler(
            admin,
            dataRecoveryRoutes.isStoreVersionReadyForDataRecovery(admin)));
    httpService.post(
        DATA_RECOVERY.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, dataRecoveryRoutes.dataRecovery(admin)));
    httpService.get(
        GET_KAFKA_TOPIC_CONFIGS.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, controllerRoutes.getKafkaTopicConfigs(admin)));
    httpService.post(
        UPDATE_KAFKA_TOPIC_LOG_COMPACTION.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, controllerRoutes.updateKafkaTopicLogCompaction(admin)));
    httpService.post(
        UPDATE_KAFKA_TOPIC_RETENTION.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, controllerRoutes.updateKafkaTopicRetention(admin)));
    httpService.post(
        UPDATE_KAFKA_TOPIC_MIN_IN_SYNC_REPLICA.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, controllerRoutes.updateKafkaTopicMinInSyncReplica(admin)));

    httpService.get(
        GET_ADMIN_TOPIC_METADATA.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, adminTopicMetadataRoutes.getAdminTopicMetadata(admin)));
    httpService.post(
        UPDATE_ADMIN_TOPIC_METADATA.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, adminTopicMetadataRoutes.updateAdminTopicMetadata(admin)));

    httpService.post(
        DELETE_KAFKA_TOPIC.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.deleteKafkaTopic(admin)));
    httpService.post(
        REMOVE_STORE_FROM_GRAVEYARD.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.removeStoreFromGraveyard(admin)));

    httpService.post(
        CREATE_STORAGE_PERSONA.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storagePersonaRoutes.createStoragePersona(admin)));
    httpService.get(
        GET_STORAGE_PERSONA.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storagePersonaRoutes.getStoragePersona(admin)));
    httpService.post(
        DELETE_STORAGE_PERSONA.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storagePersonaRoutes.deleteStoragePersona(admin)));
    httpService.post(
        UPDATE_STORAGE_PERSONA.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storagePersonaRoutes.updateStoragePersona(admin)));
    httpService.get(
        GET_STORAGE_PERSONA_ASSOCIATED_WITH_STORE.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storagePersonaRoutes.getPersonaAssociatedWithStore(admin)));
    httpService.get(
        GET_CLUSTER_STORAGE_PERSONAS.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storagePersonaRoutes.getClusterStoragePersonas(admin)));
    httpService.get(
        GET_INUSE_SCHEMA_IDS.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, storesRoutes.getInUseSchemaIds(admin)));

    httpService.post(
        CLEANUP_INSTANCE_CUSTOMIZED_STATES.getPath(),
        new VeniceParentControllerRegionStateHandler(admin, clusterRoutes.cleanupInstanceCustomizedStates(admin)));

    httpService.awaitInitialization(); // Wait for server to be initialized
    Exception e = initFailure.get();
    if (e != null) {
      LOGGER.error("Unable to initialize spark server", e);
      throw e;
    }

    // There is no async process in this function, so we are completely finished with the start up process.
    return true;
  }

  @Override
  public void stopInner() {
    httpService.stop();
  }

  int getPort() {
    return port;
  }

  private String writeMenu(String title, String postAction, List<String> parameters) {
    StringBuilder sb = new StringBuilder();
    sb.append("<html>");
    sb.append("<head><title>Venice " + title + "</title></head>\r\n");
    sb.append("<body bgcolor=white><style>td{font-size: 12pt;}</style>");
    sb.append("<table border=\"0\">");
    sb.append("<tr><td><h1>Venice " + title + "</h1></td></tr>");
    sb.append("</table>\r\n");
    // FORM
    sb.append("<CENTER><HR WIDTH=\"100%\" NOSHADE color=\"blue\"></CENTER>");
    sb.append("<FORM ACTION=\"" + postAction + "\" METHOD=\"POST\">");
    sb.append("<table border=\"0\">");
    for (String param: parameters) {
      sb.append("<tr><td>" + param + ": <br> <input type=text name=\"" + param + "\" size=20></td></tr>");
    }
    sb.append("<tr><td><INPUT TYPE=\"submit\" NAME=\"Send\" VALUE=\"Send\"></INPUT></td>");
    sb.append("<td><INPUT TYPE=\"reset\" NAME=\"Clear\" VALUE=\"Clear\" ></INPUT></td></tr>");
    sb.append("</table></FORM>\r\n");
    sb.append("<CENTER><HR WIDTH=\"100%\" NOSHADE color=\"blue\"></CENTER>");
    sb.append("</body>");
    sb.append("</html>");
    return sb.toString();
  }

  protected static void validateParams(Request request, List<String> requiredParams, Admin admin) {
    String clusterName = request.queryParams(CLUSTER);
    if (StringUtils.isEmpty(clusterName) && !CLUSTER_DISCOVERY.pathEquals(request.pathInfo())) {
      throw new VeniceHttpException(
          HttpStatus.SC_BAD_REQUEST,
          CLUSTER + " is a required parameter",
          ErrorType.BAD_REQUEST);
    }
    // go/inclusivecode deprecated (alias="LEADER_CONTROLLER")
    if (!LEADER_CONTROLLER.pathEquals(request.pathInfo()) && !MASTER_CONTROLLER.pathEquals(request.pathInfo())
        && !CLUSTER_DISCOVERY.pathEquals(request.pathInfo()) && !admin.isLeaderControllerFor(clusterName)) {
      // Skip leader controller check for '/leader_controller' and '/discover_cluster' request
      throw new VeniceHttpException(
          HttpConstants.SC_MISDIRECTED_REQUEST,
          "This controller " + Utils.getHostName() + " is not the active controller",
          ErrorType.INCORRECT_CONTROLLER);
    }
    for (String param: requiredParams) {
      if (StringUtils.isEmpty(request.queryParams(param))) {
        throw new VeniceHttpException(
            HttpStatus.SC_BAD_REQUEST,
            param + " is a required parameter",
            ErrorType.BAD_REQUEST);
      }
    }
  }

  protected static String getOptionalParameterValue(Request request, String parameter) {
    return getOptionalParameterValue(request, parameter, null);
  }

  protected static String getOptionalParameterValue(Request request, String parameter, String defaultValue) {
    String value = request.queryParams(parameter);
    if (value == null) {
      value = defaultValue;
    }
    return value;
  }

  protected static void handleError(Throwable e, Request request, Response response) {
    handleError(e, request, response, true);
  }

  protected static void handleError(Throwable e, Request request, Response response, boolean logErrorMessage) {
    if (logErrorMessage) {
      StringBuilder sb = new StringBuilder("Request params were: ");
      request.queryMap().toMap().forEach((k, v) -> { /* Map<String, String[]> */
        sb.append(k).append("=").append(String.join(",", v)).append(" ");
      });
      String errMsg = sb.toString();
      LOGGER.error(errMsg, e);
    }
    if (e instanceof Error) {
      throw (Error) e;
    }
    int statusCode =
        e instanceof VeniceException ? ((VeniceException) e).getHttpStatusCode() : HttpStatus.SC_INTERNAL_SERVER_ERROR;
    response.status(statusCode);

    request.attribute(REQUEST_SUCCEED, false);
  }
}
