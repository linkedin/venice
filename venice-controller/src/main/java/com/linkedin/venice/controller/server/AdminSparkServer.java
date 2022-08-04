package com.linkedin.venice.controller.server;

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

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;


/**
 * Controller admin API leveraging sparkjava: http://sparkjava.com/documentation.html
 * <p>
 * AdminSparkServer is shared by multiple clusters' controllers running in one physical Venice controller instance.
 */
public class AdminSparkServer extends AbstractVeniceService {
  private static final Logger logger = LogManager.getLogger(AdminSparkServer.class);

  private final int port;
  private final Admin admin;
  private final boolean enforceSSL;
  private final boolean sslEnabled;
  private final boolean checkReadMethodForKafka;
  private final Optional<SSLConfig> sslConfig;
  private final Optional<DynamicAccessController> accessController;

  protected static final ObjectMapper mapper = ObjectMapperFactory.getInstance();
  final private Map<String, SparkServerStats> statsMap;
  final private SparkServerStats nonclusterSpecificStats;

  private static String REQUEST_START_TIME =  "startTime";
  private static String REQUEST_SUCCEED = "succeed";

  // In order to build multiple controller in a single JVM, we create a new http service instance for each of AdminSparkServer instance.
  private final Service httpService;

  private final List<ControllerRoute> disabledRoutes;

  private final boolean disableParentRequestTopicForStreamPushes;


  public AdminSparkServer(int port, Admin admin, MetricsRepository metricsRepository, Set<String> clusters, boolean enforceSSL,
      Optional<SSLConfig> sslConfig, boolean checkReadMethodForKafka, Optional<DynamicAccessController> accessController,
      List<ControllerRoute> disabledRoutes, VeniceProperties jettyConfigOverrides, boolean disableParentRequestTopicForStreamPushes) {
    this.port = port;
    this.enforceSSL = enforceSSL;
    this.sslEnabled = sslConfig.isPresent();
    this.sslConfig = sslConfig;
    this.checkReadMethodForKafka = checkReadMethodForKafka;
    this.accessController = accessController;
    //Note: admin is passed in as a reference.  The expectation is the source of the admin will
    //      close it so we don't close it in stopInner()
    this.admin = admin;
    statsMap = new HashMap<>(clusters.size());
    String statsPrefix = sslEnabled ? "secure_" : "";
    for(String cluster : clusters){
      statsMap.put(cluster, new SparkServerStats(metricsRepository, cluster + "." + statsPrefix + "controller_spark_server"));
    }
    nonclusterSpecificStats = new SparkServerStats(metricsRepository, "." + statsPrefix + "controller_spark_server");
    EmbeddedServers.add(
        EmbeddedServers.Identifiers.JETTY,
        new VeniceSparkServerFactory(jettyConfigOverrides));

    httpService = Service.ignite();
    this.disabledRoutes = disabledRoutes;
    this.disableParentRequestTopicForStreamPushes = disableParentRequestTopicForStreamPushes;
  }

  @Override
  public boolean startInner() throws Exception {
    AtomicReference<Exception> initFailure = new AtomicReference<>();
    httpService.initExceptionHandler(initFailure::set);

    httpService.port(port);

    if (sslEnabled) {
      SSLConfig config = sslConfig.get();
      httpService.secure(config.getSslKeyStoreLocation(),
                         config.getSslKeyStorePassword(),
                         null,
                         config.getSslTrustStoreLocation(),
                         config.getSslTrustStorePassword(),
                         config.isSslNeedsClientCert());
    }

    httpService.before((request, response) -> {
      AuditInfo audit = new AuditInfo(request);
      logger.info(audit.toString());
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
      if(disabledRoutes.contains(ControllerRoute.valueOfPath(request.uri()))) {
        httpService.halt(403, String.format("Route %s has been disabled in venice controller config!!", request.uri()));
      }
    });

    httpService.after((request, response) -> {
      AuditInfo audit = new AuditInfo(request);
      SparkServerStats stats = statsMap.get(request.queryParams(CLUSTER));
      if (stats == null) {
        stats = nonclusterSpecificStats;
      }
      long latency = System.currentTimeMillis() - (long)request.attribute(REQUEST_START_TIME);
      if ((boolean)request.attribute(REQUEST_SUCCEED)) {
        logger.info(audit.successString());
        stats.recordSuccessfulRequestLatency(latency);
      } else {
        logger.info(audit.failureString(response.body()));
        stats.recordFailedRequestLatency(latency);
      }
    });

    // Build all different routes
    ControllerRoutes controllerRoutes = new ControllerRoutes(accessController);
    StoresRoutes storesRoutes = new StoresRoutes(accessController);
    JobRoutes jobRoutes = new JobRoutes(accessController);
    SkipAdminRoute skipAdminRoute = new SkipAdminRoute(accessController);
    CreateVersion createVersion = new CreateVersion(accessController, this.checkReadMethodForKafka,
        disableParentRequestTopicForStreamPushes);
    CreateStore createStoreRoute = new CreateStore(accessController);
    NodesAndReplicas nodesAndReplicas = new NodesAndReplicas(accessController);
    SchemaRoutes schemaRoutes = new SchemaRoutes(accessController);
    AdminCommandExecutionRoutes adminCommandExecutionRoutes = new AdminCommandExecutionRoutes(accessController);
    RoutersClusterConfigRoutes routersClusterConfigRoutes = new RoutersClusterConfigRoutes(accessController);
    MigrationRoutes migrationRoutes = new MigrationRoutes(accessController);
    VersionRoute versionRoute = new VersionRoute(accessController);
    ClusterRoutes clusterRoutes = new ClusterRoutes(accessController);
    NewClusterBuildOutRoutes newClusterBuildOutRoutes = new NewClusterBuildOutRoutes(accessController);
    DataRecoveryRoutes dataRecoveryRoutes = new DataRecoveryRoutes(accessController);
    AdminTopicMetadataRoutes adminTopicMetadataRoutes = new AdminTopicMetadataRoutes(accessController);
    StoragePersonaRoutes storagePersonaRoutes = new StoragePersonaRoutes(accessController);

    httpService.get(SET_VERSION.getPath(), (request, response) -> {
      response.type(HttpConstants.TEXT_HTML);
      return writeMenu("Set Active Version", SET_VERSION.getPath(), SET_VERSION.getParams());
    });

    httpService.get(LIST_CHILD_CLUSTERS.getPath(), controllerRoutes.getChildControllers(admin));
    httpService.get(LIST_STORES.getPath(), storesRoutes.getAllStores(admin));
    httpService.get(CLUSTER_HEALTH_STORES.getPath(), storesRoutes.getAllStoresStatuses(admin));
    httpService.get(STORE.getPath(), storesRoutes.getStore(admin));
    httpService.get(FUTURE_VERSION.getPath(), storesRoutes.getFutureVersion(admin));
    httpService.post(SET_TOPIC_COMPACTION.getPath(), storesRoutes.setTopicCompaction(admin));

    httpService.post(UPDATE_CLUSTER_CONFIG.getPath(), clusterRoutes.updateClusterConfig(admin));
    httpService.post(WIPE_CLUSTER.getPath(), clusterRoutes.wipeCluster(admin));
    httpService.post(REPLICATE_META_DATA.getPath(), newClusterBuildOutRoutes.copyOverStoreSchemasAndConfigs(admin));

    httpService.get(JOB.getPath(), jobRoutes.jobStatus(admin));
    httpService.post(KILL_OFFLINE_PUSH_JOB.getPath(), jobRoutes.killOfflinePushJob(admin));
    httpService.post(SKIP_ADMIN.getPath(), skipAdminRoute.skipAdminMessage(admin));

    httpService.post(EMPTY_PUSH.getPath(), createVersion.emptyPush(admin));
    httpService.post(END_OF_PUSH.getPath(), createVersion.writeEndOfPush(admin));
    httpService.post(REQUEST_TOPIC.getPath(), createVersion.requestTopicForPushing(admin));
    httpService.post(ADD_VERSION.getPath(), createVersion.addVersionAndStartIngestion(admin));
    httpService.post(NEW_STORE.getPath(), createStoreRoute.createStore(admin));
    httpService.get(CHECK_RESOURCE_CLEANUP_FOR_STORE_CREATION.getPath(), createStoreRoute.checkResourceCleanupForStoreCreation(admin));
    httpService.post(DELETE_STORE.getPath(), storesRoutes.deleteStore(admin));
    httpService.post(UPDATE_STORE.getPath(), storesRoutes.updateStore(admin));

    httpService.get(STORE_MIGRATION_ALLOWED.getPath(), clusterRoutes.isStoreMigrationAllowed(admin));
    httpService.post(MIGRATE_STORE.getPath(), storesRoutes.migrateStore(admin));
    httpService.post(COMPLETE_MIGRATION.getPath(), storesRoutes.completeMigration(admin));
    httpService.post(ABORT_MIGRATION.getPath(), storesRoutes.abortMigration(admin));

    httpService.post(ENABLE_STORE.getPath(), storesRoutes.enableStore(admin));
    httpService.post(DELETE_ALL_VERSIONS.getPath(), storesRoutes.deleteAllVersions(admin));
    httpService.post(DELETE_OLD_VERSION.getPath(), storesRoutes.deleteOldVersions(admin));
    httpService.post(SET_VERSION.getPath(), storesRoutes.setCurrentVersion(admin));

    httpService.get(ClUSTER_HEALTH_INSTANCES.getPath(), nodesAndReplicas.listAllNodesStatus(admin));
    httpService.get(LIST_NODES.getPath(), nodesAndReplicas.listAllNodes(admin));
    httpService.get(LIST_REPLICAS.getPath(), nodesAndReplicas.listReplicasForStore(admin));
    httpService.get(NODE_REPLICAS.getPath(), nodesAndReplicas.listReplicasForStorageNode(admin));
    httpService.get(NODE_REMOVABLE.getPath(), nodesAndReplicas.isNodeRemovable(admin));
    httpService.get(NODE_REPLICAS_READINESS.getPath(), nodesAndReplicas.nodeReplicasReadiness(admin));
    httpService.post(WHITE_LIST_ADD_NODE.getPath(), nodesAndReplicas.addNodeIntoAllowList(admin));
    httpService.post(ALLOW_LIST_ADD_NODE.getPath(), nodesAndReplicas.addNodeIntoAllowList(admin));
    httpService.post(WHITE_LIST_REMOVE_NODE.getPath(), nodesAndReplicas.removeNodeFromAllowList(admin));
    httpService.post(ALLOW_LIST_REMOVE_NODE.getPath(), nodesAndReplicas.removeNodeFromAllowList(admin));
    httpService.post(REMOVE_NODE.getPath(), nodesAndReplicas.removeNodeFromCluster(admin));

    // Operations for key schema/value schema
    httpService.get(GET_KEY_SCHEMA.getPath(), schemaRoutes.getKeySchema(admin));
    httpService.post(ADD_VALUE_SCHEMA.getPath(), schemaRoutes.addValueSchema(admin));
    httpService.post(ADD_DERIVED_SCHEMA.getPath(), schemaRoutes.addDerivedSchema(admin));
    httpService.get(GET_VALUE_SCHEMA.getPath(), schemaRoutes.getValueSchema(admin));
    httpService.post(GET_VALUE_SCHEMA_ID.getPath(), schemaRoutes.getValueSchemaID(admin));
    httpService.post(GET_VALUE_OR_DERIVED_SCHEMA_ID.getPath(), schemaRoutes.getDerivedSchemaID(admin));
    httpService.get(GET_ALL_VALUE_SCHEMA.getPath(), schemaRoutes.getAllValueSchema(admin));
    httpService.get(GET_ALL_VALUE_AND_DERIVED_SCHEMA.getPath(), schemaRoutes.getAllValueAndDerivedSchema(admin));
    httpService.post(REMOVE_DERIVED_SCHEMA.getPath(), schemaRoutes.removeDerivedSchema(admin));
    httpService.get(GET_ALL_REPLICATION_METADATA_SCHEMAS.getPath(), schemaRoutes.getAllReplicationMetadataSchemas(admin));

    httpService.post(SET_OWNER.getPath(), storesRoutes.setOwner(admin));
    httpService.post(SET_PARTITION_COUNT.getPath(), storesRoutes.setPartitionCount(admin));

    httpService.get(MASTER_CONTROLLER.getPath(), controllerRoutes.getLeaderController(admin));
    // This API should be used by CORP controller only. H2V could talk to any of controllers in CORP to find who is the
    // current leader CORP controller. In other colos, router will find the leader controller instead of calling this API.
    httpService.get(LEADER_CONTROLLER.getPath(), controllerRoutes.getLeaderController(admin));

    httpService.get(EXECUTION.getPath(), adminCommandExecutionRoutes.getExecution(admin));
    httpService.get(LAST_SUCCEED_EXECUTION_ID.getPath(), adminCommandExecutionRoutes.getLastSucceedExecutionId(admin));

    httpService.get(STORAGE_ENGINE_OVERHEAD_RATIO.getPath(), storesRoutes.getStorageEngineOverheadRatio(admin));

    httpService.post(ENABLE_THROTTLING.getPath(), routersClusterConfigRoutes.enableThrottling(admin));
    httpService.post(ENABLE_MAX_CAPACITY_PROTECTION.getPath(), routersClusterConfigRoutes.enableMaxCapacityProtection(admin));
    httpService.post(ENABLE_QUOTA_REBALANCED.getPath(), routersClusterConfigRoutes.enableQuotaRebalanced(admin));

    httpService.get(GET_ROUTERS_CLUSTER_CONFIG.getPath(), routersClusterConfigRoutes.getRoutersClusterConfig(admin));

    httpService.get(GET_ALL_MIGRATION_PUSH_STRATEGIES.getPath(), migrationRoutes.getAllMigrationPushStrategies(admin));
    httpService.get(SET_MIGRATION_PUSH_STRATEGY.getPath(), migrationRoutes.setMigrationPushStrategy(admin));

    httpService.get(CLUSTER_DISCOVERY.getPath(), ClusterDiscovery.discoverCluster(admin));
    httpService.get(LIST_BOOTSTRAPPING_VERSIONS.getPath(), versionRoute.listBootstrappingVersions(admin));

    httpService.post(OFFLINE_PUSH_INFO.getPath(), createVersion.uploadPushInfo(admin));

    httpService.post(UPLOAD_PUSH_JOB_STATUS.getPath(), jobRoutes.uploadPushJobStatus(admin));
    httpService.post(SEND_PUSH_JOB_DETAILS.getPath(), jobRoutes.sendPushJobDetails(admin));

    httpService.get(LIST_LF_STORES.getPath(), storesRoutes.getLFModelStores(admin));
    httpService.post(ENABLE_LF_MODEL.getPath(), storesRoutes.enableLFModelForStores(admin));
    httpService.post(CONFIGURE_NATIVE_REPLICATION_FOR_CLUSTER.getPath(), storesRoutes.enableNativeReplicationForCluster(admin));
    httpService.post(CONFIGURE_ACTIVE_ACTIVE_REPLICATION_FOR_CLUSTER.getPath(), storesRoutes.enableActiveActiveReplicationForCluster(admin));
    httpService.post(UPDATE_ACL.getPath(), createStoreRoute.updateAclForStore(admin));
    httpService.get(GET_ACL.getPath(), createStoreRoute.getAclForStore(admin));
    httpService.get(DELETE_ACL.getPath(), createStoreRoute.deleteAclForStore(admin));
    httpService.get(GET_DELETABLE_STORE_TOPICS.getPath(), storesRoutes.getDeletableStoreTopics(admin));
    httpService.post(CONFIGURE_INCREMENTAL_PUSH_FOR_CLUSTER.getPath(), storesRoutes.configureIncrementalPushForCluster(admin));
    httpService.get(GET_ONGOING_INCREMENTAL_PUSH_VERSIONS.getPath(), jobRoutes.getOngoingIncrementalPushVersions(admin));
    httpService.get(GET_REPUSH_INFO.getPath(), storesRoutes.getRepushInfo(admin));
    httpService.get(COMPARE_STORE.getPath(), storesRoutes.compareStore(admin));
    httpService.get(GET_STALE_STORES_IN_CLUSTER.getPath(), storesRoutes.getStaleStoresInCluster(admin));
    httpService.get(GET_STORES_IN_CLUSTER.getPath(), storesRoutes.getStoresInCluster(admin));
    httpService.get(GET_STORE_LARGEST_USED_VERSION.getPath(), storesRoutes.getStoreLargestUsedVersion(admin));
    httpService.get(GET_REGION_PUSH_DETAILS.getPath(), storesRoutes.getRegionPushDetails(admin));
    httpService.get(LIST_STORE_PUSH_INFO.getPath(), storesRoutes.listStorePushInfo(admin));

    httpService.post(PREPARE_DATA_RECOVERY.getPath(), dataRecoveryRoutes.prepareDataRecovery(admin));
    httpService.get(IS_STORE_VERSION_READY_FOR_DATA_RECOVERY.getPath(), dataRecoveryRoutes.isStoreVersionReadyForDataRecovery(admin));
    httpService.post(DATA_RECOVERY.getPath(), dataRecoveryRoutes.dataRecovery(admin));
    httpService.get(UPDATE_KAFKA_TOPIC_LOG_COMPACTION.getPath(), controllerRoutes.updateKafkaTopicLogCompaction(admin));
    httpService.get(UPDATE_KAFKA_TOPIC_RETENTION.getPath(), controllerRoutes.updateKafkaTopicRetention(admin));

    httpService.get(GET_ADMIN_TOPIC_METADATA.getPath(), adminTopicMetadataRoutes.getAdminTopicMetadata(admin));
    httpService.post(UPDATE_ADMIN_TOPIC_METADATA.getPath(), adminTopicMetadataRoutes.updateAdminTopicMetadata(admin));

    httpService.post(DELETE_KAFKA_TOPIC.getPath(), storesRoutes.deleteKafkaTopic(admin));

    httpService.post(CREATE_STORAGE_PERSONA.getPath(), storagePersonaRoutes.createStoragePersona(admin));
    httpService.get(GET_STORAGE_PERSONA.getPath(), storagePersonaRoutes.getStoragePersona(admin));
    httpService.post(DELETE_STORAGE_PERSONA.getPath(), storagePersonaRoutes.deleteStoragePersona(admin));
    httpService.post(UPDATE_STORAGE_PERSONA.getPath(), storagePersonaRoutes.updateStoragePersona(admin));

    httpService.awaitInitialization(); // Wait for server to be initialized
    Exception e = initFailure.get();
    if (e != null) {
      logger.error("Unable to initialize spark server", e);
      throw e;
    }

    // There is no async process in this function, so we are completely finished with the start up process.
    return true;
  }

  @Override
  public void stopInner() {
    httpService.stop();
  }

  public int getPort(){
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
    for (String param : parameters){
      sb.append("<tr><td>"+param+": <br> <input type=text name=\""+param+"\" size=20></td></tr>");
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
      throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, CLUSTER + " is a required parameter", ErrorType.BAD_REQUEST);
    }
    if (!LEADER_CONTROLLER.pathEquals(request.pathInfo()) && !MASTER_CONTROLLER.pathEquals(request.pathInfo())
        && !CLUSTER_DISCOVERY.pathEquals(request.pathInfo()) && !admin.isLeaderControllerFor(clusterName)) {
      // go/inclusivecode deprecated (alias="leader_controller")
      // Skip leader controller check for '/master_controller' and '/discover_cluster' request
      throw new VeniceHttpException(HttpConstants.SC_MISDIRECTED_REQUEST, "This controller " + Utils.getHostName() + " is not the active controller", ErrorType.INCORRECT_CONTROLLER);
    }
    for (String param : requiredParams) {
      if (StringUtils.isEmpty(request.queryParams(param))) {
        throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, param + " is a required parameter", ErrorType.BAD_REQUEST);
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
    StringBuilder sb = new StringBuilder("Request params were: ");
    request.queryMap().toMap().forEach((k, v) -> {  /* Map<String, String[]> */
      sb.append(k).append("=").append(String.join(",",v)).append(" ");
    });
    String errMsg = sb.toString();
    logger.error(errMsg, e);
    if (e instanceof Error) {
      throw (Error) e;
    }
    int statusCode = e instanceof VeniceException ?
        ((VeniceException) e).getHttpStatusCode() :
        HttpStatus.SC_INTERNAL_SERVER_ERROR;
    response.status(statusCode);

    request.attribute(REQUEST_SUCCEED, false);
  }
}
