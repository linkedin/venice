package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ChildAwareResponse;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.LeaderControllerResponse;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.utils.Utils;
import java.util.Optional;
import org.apache.http.HttpStatus;
import spark.Request;
import spark.Route;


public class ControllerRoutes extends AbstractRoute {
  public ControllerRoutes(boolean sslEnabled, Optional<DynamicAccessController> accessController) {
    super(sslEnabled, accessController);
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
        responseObject.setCluster(cluster);
        responseObject.setUrl(admin.getLeaderController(cluster).getUrl(isSslEnabled()));
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
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
   * @see TopicManager#updateTopicCompactionPolicy(String, boolean)
   */
  public Route updateKafkaTopicLogCompaction(Admin admin) {
    return updateKafkaTopicConfig(admin, adminRequest -> {
      AdminSparkServer.validateParams(adminRequest, UPDATE_KAFKA_TOPIC_LOG_COMPACTION.getParams(), admin);
      String topicName = adminRequest.queryParams(TOPIC);
      boolean kafkaTopicLogCompactionEnabled = Utils.parseBooleanFromString(
          adminRequest.queryParams(KAFKA_TOPIC_LOG_COMPACTION_ENABLED),
          KAFKA_TOPIC_LOG_COMPACTION_ENABLED);

      TopicManager topicManager = admin.getTopicManager();
      topicManager.updateTopicCompactionPolicy(topicName, kafkaTopicLogCompactionEnabled);
    });
  }

  /**
   * @see TopicManager#updateTopicRetention(String, long)
   */
  public Route updateKafkaTopicRetention(Admin admin) {
    return updateKafkaTopicConfig(admin, adminRequest -> {
      AdminSparkServer.validateParams(adminRequest, UPDATE_KAFKA_TOPIC_RETENTION.getParams(), admin);
      String topicName = adminRequest.queryParams(TOPIC);
      long kafkaTopicRetentionIsMs =
          Utils.parseLongFromString(adminRequest.queryParams(KAFKA_TOPIC_RETENTION_IN_MS), KAFKA_TOPIC_RETENTION_IN_MS);
      TopicManager topicManager = admin.getTopicManager();
      topicManager.updateTopicRetention(topicName, kafkaTopicRetentionIsMs);
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
        return AdminSparkServer.mapper.writeValueAsString(responseObject);
      }
      try {
        responseObject.setCluster(request.queryParams(CLUSTER));
        updateTopicConfigFunction.apply(request);
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  @FunctionalInterface
  interface UpdateTopicConfigFunction {
    void apply(Request request);
  }
}
