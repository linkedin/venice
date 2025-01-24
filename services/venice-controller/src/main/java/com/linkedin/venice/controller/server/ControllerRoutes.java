package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.KAFKA_TOPIC_LOG_COMPACTION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.KAFKA_TOPIC_MIN_IN_SYNC_REPLICA;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.KAFKA_TOPIC_RETENTION_IN_MS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.TOPIC;
import static com.linkedin.venice.controllerapi.ControllerRoute.AGGREGATED_HEALTH_STATUS;
import static com.linkedin.venice.controllerapi.ControllerRoute.LEADER_CONTROLLER;
import static com.linkedin.venice.controllerapi.ControllerRoute.LIST_CHILD_CLUSTERS;
import static com.linkedin.venice.controllerapi.ControllerRoute.UPDATE_KAFKA_TOPIC_LOG_COMPACTION;
import static com.linkedin.venice.controllerapi.ControllerRoute.UPDATE_KAFKA_TOPIC_MIN_IN_SYNC_REPLICA;
import static com.linkedin.venice.controllerapi.ControllerRoute.UPDATE_KAFKA_TOPIC_RETENTION;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.InstanceRemovableStatuses;
import com.linkedin.venice.controllerapi.AggregatedHealthStatusRequest;
import com.linkedin.venice.controllerapi.ChildAwareResponse;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.LeaderControllerResponse;
import com.linkedin.venice.controllerapi.PubSubTopicConfigResponse;
import com.linkedin.venice.controllerapi.StoppableNodeStatusResponse;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcRequest;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcResponse;
import com.linkedin.venice.pubsub.PubSubTopicConfiguration;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.Utils;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import spark.Request;
import spark.Route;


public class ControllerRoutes extends AbstractRoute {
  private static final Logger LOGGER = LogManager.getLogger(ControllerRoutes.class);
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

  private final PubSubTopicRepository pubSubTopicRepository;
  private final VeniceControllerRequestHandler requestHandler;

  public ControllerRoutes(
      boolean sslEnabled,
      Optional<DynamicAccessController> accessController,
      PubSubTopicRepository pubSubTopicRepository,
      VeniceControllerRequestHandler requestHandler) {
    super(sslEnabled, accessController);
    this.pubSubTopicRepository = pubSubTopicRepository;
    this.requestHandler = requestHandler;
  }

  /**
   * No ACL check; any user is allowed to check leader controller.
   * @see Admin#getLeaderController(String)
   */
  public Route getLeaderController(Admin admin) {
    return (request, response) -> {
      LeaderControllerResponse responseObject = new LeaderControllerResponse();
      try {
        AdminSparkServer.validateParams(request, LEADER_CONTROLLER.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        LeaderControllerGrpcResponse inernalResponse = requestHandler
            .getLeaderControllerDetails(LeaderControllerGrpcRequest.newBuilder().setClusterName(cluster).build());
        responseObject.setCluster(inernalResponse.getClusterName());
        responseObject.setUrl(inernalResponse.getHttpUrl());
        if (StringUtils.isNotBlank(inernalResponse.getHttpsUrl())) {
          responseObject.setSecureUrl(inernalResponse.getHttpsUrl());
        }
        if (StringUtils.isNotBlank(inernalResponse.getGrpcUrl())) {
          responseObject.setGrpcUrl(inernalResponse.getGrpcUrl());
        }
        if (StringUtils.isNotBlank(inernalResponse.getSecureGrpcUrl())) {
          responseObject.setSecureGrpcUrl(inernalResponse.getSecureGrpcUrl());
        }
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  /**
   * @see Admin#getChildDataCenterControllerUrlMap(String)
   * @see Admin#getChildDataCenterControllerD2Map(String)
   * @see Admin#getChildControllerD2ServiceName(String)
   */
  public Route getChildControllers(Admin admin) {
    return new VeniceRouteHandler<ChildAwareResponse>(ChildAwareResponse.class) {
      @Override
      public void internalHandle(Request request, ChildAwareResponse veniceResponse) {
        AdminSparkServer.validateParams(request, LIST_CHILD_CLUSTERS.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);

        veniceResponse.setCluster(clusterName);

        if (admin.isParent()) {
          veniceResponse.setChildDataCenterControllerUrlMap(admin.getChildDataCenterControllerUrlMap(clusterName));
          veniceResponse.setChildDataCenterControllerD2Map(admin.getChildDataCenterControllerD2Map(clusterName));
          veniceResponse.setD2ServiceName(admin.getChildControllerD2ServiceName(clusterName));
        }
      }
    };
  }

  /**
   * @see TopicManager#updateTopicCompactionPolicy(PubSubTopic, boolean)
   */
  public Route updateKafkaTopicLogCompaction(Admin admin) {
    return updateKafkaTopicConfig(admin, adminRequest -> {
      AdminSparkServer.validateParams(adminRequest, UPDATE_KAFKA_TOPIC_LOG_COMPACTION.getParams(), admin);
      PubSubTopic topicName = pubSubTopicRepository.getTopic(adminRequest.queryParams(TOPIC));
      boolean kafkaTopicLogCompactionEnabled = Utils.parseBooleanFromString(
          adminRequest.queryParams(KAFKA_TOPIC_LOG_COMPACTION_ENABLED),
          KAFKA_TOPIC_LOG_COMPACTION_ENABLED);

      TopicManager topicManager = admin.getTopicManager();
      topicManager.updateTopicCompactionPolicy(topicName, kafkaTopicLogCompactionEnabled);
    });
  }

  /**
   * No ACL check; any user is allowed to check topic configs.
   */
  public Route getKafkaTopicConfigs(Admin admin) {
    return (request, response) -> {
      PubSubTopicConfigResponse responseObject = new PubSubTopicConfigResponse();
      response.type(HttpConstants.JSON);

      try {
        responseObject.setCluster("");
        PubSubTopic topicName = pubSubTopicRepository.getTopic(request.queryParams(TOPIC));
        TopicManager topicManager = admin.getTopicManager();
        PubSubTopicConfiguration pubSubTopicConfiguration = topicManager.getTopicConfigWithRetry(topicName);
        responseObject.setTopicConfigsResponse(pubSubTopicConfiguration.toString());
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  /**
   * @see TopicManager#updateTopicRetention(PubSubTopic, long)
   */
  public Route updateKafkaTopicRetention(Admin admin) {
    return updateKafkaTopicConfig(admin, adminRequest -> {
      AdminSparkServer.validateParams(adminRequest, UPDATE_KAFKA_TOPIC_RETENTION.getParams(), admin);
      PubSubTopic topicName = pubSubTopicRepository.getTopic(adminRequest.queryParams(TOPIC));
      long kafkaTopicRetentionIsMs =
          Utils.parseLongFromString(adminRequest.queryParams(KAFKA_TOPIC_RETENTION_IN_MS), KAFKA_TOPIC_RETENTION_IN_MS);
      TopicManager topicManager = admin.getTopicManager();
      topicManager.updateTopicRetention(topicName, kafkaTopicRetentionIsMs);
    });
  }

  public Route updateKafkaTopicMinInSyncReplica(Admin admin) {
    return updateKafkaTopicConfig(admin, adminRequest -> {
      AdminSparkServer.validateParams(adminRequest, UPDATE_KAFKA_TOPIC_MIN_IN_SYNC_REPLICA.getParams(), admin);
      PubSubTopic topicName = pubSubTopicRepository.getTopic(adminRequest.queryParams(TOPIC));
      int kafkaTopicMinISR = Utils.parseIntFromString(
          adminRequest.queryParams(KAFKA_TOPIC_MIN_IN_SYNC_REPLICA),
          KAFKA_TOPIC_MIN_IN_SYNC_REPLICA);
      TopicManager topicManager = admin.getTopicManager();
      topicManager.updateTopicMinInSyncReplica(topicName, kafkaTopicMinISR);
    });
  }

  private Route updateKafkaTopicConfig(Admin admin, UpdateTopicConfigFunction updateTopicConfigFunction) {
    return (request, response) -> {
      ControllerResponse responseObject = new ControllerResponse();
      response.type(HttpConstants.JSON);
      // Only allow allowlist users to run this command
      if (!isAllowListUser(request)) {
        response.status(HttpStatus.SC_FORBIDDEN);
        responseObject.setError("Only admin users are allowed to run " + request.url());
        responseObject.setErrorType(ErrorType.BAD_REQUEST);
        return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
      }
      try {
        responseObject.setCluster(request.queryParams(CLUSTER));
        updateTopicConfigFunction.apply(request);
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  public Route getAggregatedHealthStatus(Admin admin) {
    return (request, response) -> {
      StoppableNodeStatusResponse responseObject = new StoppableNodeStatusResponse();
      response.type(HttpConstants.JSON);

      try {
        LOGGER.info("[AggregatedHealthStatus] Received request: {}", request.body());
        AggregatedHealthStatusRequest statusRequest =
            OBJECT_MAPPER.readValue(request.body(), AggregatedHealthStatusRequest.class);
        String cluster = statusRequest.getClusterId();
        List<String> instanceList = statusRequest.getInstances();
        if (instanceList.isEmpty()) {
          responseObject.setError("Empty instances list");
          responseObject.setErrorType(ErrorType.BAD_REQUEST);
          response.status(HttpStatus.SC_BAD_REQUEST);
          return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
        }

        List<String> toBeStoppedInstanceList = statusRequest.getToBeStoppedInstances();
        responseObject.setCluster(cluster);

        InstanceRemovableStatuses statuses =
            admin.getAggregatedHealthStatus(cluster, instanceList, toBeStoppedInstanceList, isSslEnabled());
        String redirectUrl = statuses.getRedirectUrl();
        if (redirectUrl != null) {
          LOGGER.info("[AggregatedHealthStatus] Redirecting to: {}", redirectUrl);
          response.redirect(redirectUrl + AGGREGATED_HEALTH_STATUS.getPath(), HttpStatus.SC_MOVED_TEMPORARILY);
          return null;
        } else {
          responseObject.setNonStoppableInstancesWithReason(statuses.getNonStoppableInstancesWithReasons());
          responseObject.setStoppableInstances(statuses.getStoppableInstances());
        }
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      String responseContent = AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
      LOGGER.info("[AggregatedHealthStatus] Response: {}", responseContent);
      return responseContent;
    };
  }

  @FunctionalInterface
  interface UpdateTopicConfigFunction {
    void apply(Request request);
  }
}
