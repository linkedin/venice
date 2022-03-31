package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ChildAwareResponse;
import com.linkedin.venice.controllerapi.LeaderControllerResponse;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.utils.Utils;
import java.util.Map;
import java.util.Optional;
import org.apache.http.HttpStatus;
import spark.Request;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;


public class ControllerRoutes extends AbstractRoute {

  public ControllerRoutes(Optional<DynamicAccessController> accessController) {
    super(accessController);
  }

  /**
   * No ACL check; any user is allowed to check leader controller.
   */
  public Route getLeaderController(Admin admin) {
    return (request, response) -> {
      LeaderControllerResponse responseObject = new LeaderControllerResponse();
      try {
        AdminSparkServer.validateParams(request, LEADER_CONTROLLER.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        responseObject.setCluster(cluster);
        responseObject.setUrl(admin.getLeaderController(cluster).getUrl(isAclEnabled()));
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public Route getChildControllers(Admin admin) {
    return new VeniceRouteHandler<ChildAwareResponse>(ChildAwareResponse.class) {
      @Override
      public void internalHandle(Request request, ChildAwareResponse veniceResponse) {
        AdminSparkServer.validateParams(request, LIST_CHILD_CLUSTERS.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);

        veniceResponse.setCluster(clusterName);

        if (admin.isParent()) {
          Map<String, String> childControllerUrls = admin.getChildDataCenterControllerUrlMap(clusterName);
          veniceResponse.setChildClusterMap(childControllerUrls);
        }
      }
    };
  }

  public Route updateKafkaTopicLogCompaction(Admin admin) {
    return updateKafkaTopicConfig(admin, adminRequest -> {
      AdminSparkServer.validateParams(adminRequest, UPDATE_KAFKA_TOPIC_LOG_COMPACTION.getParams(), admin);
      String topicName = adminRequest.queryParams(TOPIC);
      boolean kafkaTopicLogCompactionEnabled = Utils.parseBooleanFromString(adminRequest.queryParams(KAFKA_TOPIC_LOG_COMPACTION_ENABLED),
          KAFKA_TOPIC_LOG_COMPACTION_ENABLED);

      TopicManager topicManager = admin.getTopicManager();
      topicManager.updateTopicCompactionPolicy(topicName, kafkaTopicLogCompactionEnabled);
    });
  }

  public Route updateKafkaTopicRetention(Admin admin) {
    return updateKafkaTopicConfig(admin, adminRequest -> {
      AdminSparkServer.validateParams(adminRequest, UPDATE_KAFKA_TOPIC_RETENTION.getParams(), admin);
      String topicName = adminRequest.queryParams(TOPIC);
      long kafkaTopicRetentionIsMs = Utils.parseLongFromString(adminRequest.queryParams(KAFKA_TOPIC_RETENTION_IN_MS),
          KAFKA_TOPIC_RETENTION_IN_MS);
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
