package com.linkedin.venice.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.server.endpoint.JobStatusRequest;
import com.linkedin.venice.controller.spark.VeniceSparkServerFactory;
import com.linkedin.venice.controller.stats.SparkServerStats;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import spark.embeddedserver.EmbeddedServers;


public class VeniceControllerApiHandler {
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

  private final List<ControllerRoute> disabledRoutes;

  private final boolean disableParentRequestTopicForStreamPushes;
  private final PubSubTopicRepository pubSubTopicRepository;

  // Use this for access controls and other security related checks
  VeniceControllerAccessControlService veniceControllerAccessControlService;

  public VeniceControllerApiHandler(
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
      PubSubTopicRepository pubSubTopicRepository) {
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

    this.disabledRoutes = disabledRoutes;
    this.disableParentRequestTopicForStreamPushes = disableParentRequestTopicForStreamPushes;
    this.pubSubTopicRepository = pubSubTopicRepository;
    this.veniceControllerAccessControlService = new VeniceControllerAccessControlService(sslEnabled, accessController);
  }

  public JobStatusQueryResponse populateJobStatus(JobStatusRequest jobStatusRequest) {
    JobStatusQueryResponse responseObject = new JobStatusQueryResponse();

    String store = jobStatusRequest.getStore();
    int versionNumber = jobStatusRequest.getVersionNumber();
    String cluster = jobStatusRequest.getCluster();
    String incrementalPushVersion = jobStatusRequest.getIncrementalPushVersion();
    String region = jobStatusRequest.getRegion();
    String targetedRegions = jobStatusRequest.getTargetedRegions();

    String kafkaTopicName = Version.composeKafkaTopic(store, versionNumber);
    Admin.OfflinePushStatusInfo offlineJobStatus = admin.getOffLinePushStatus(
        cluster,
        kafkaTopicName,
        Optional.ofNullable(incrementalPushVersion),
        region,
        targetedRegions);
    responseObject.setStatus(offlineJobStatus.getExecutionStatus().toString());
    responseObject.setStatusUpdateTimestamp(offlineJobStatus.getStatusUpdateTimestamp());
    responseObject.setStatusDetails(offlineJobStatus.getStatusDetails());
    responseObject.setExtraInfo(offlineJobStatus.getExtraInfo());
    responseObject.setExtraInfoUpdateTimestamp(offlineJobStatus.getExtraInfoUpdateTimestamp());
    responseObject.setExtraDetails(offlineJobStatus.getExtraDetails());
    responseObject.setUncompletedPartitions(offlineJobStatus.getUncompletedPartitions());

    responseObject.setCluster(cluster);
    responseObject.setName(store);
    responseObject.setVersion(versionNumber);
    return responseObject;
  }
}
