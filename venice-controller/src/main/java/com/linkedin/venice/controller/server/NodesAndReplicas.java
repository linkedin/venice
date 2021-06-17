package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.NodeRemovableResult;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.MultiNodesStatusResponse;
import com.linkedin.venice.controllerapi.MultiNodeResponse;
import com.linkedin.venice.controllerapi.MultiReplicaResponse;
import com.linkedin.venice.controllerapi.NodeStatusResponse;
import com.linkedin.venice.helix.Replica;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.Utils;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;


public class NodesAndReplicas extends AbstractRoute {
  private static final Logger LOGGER = Logger.getLogger(NodesAndReplicas.class);
  private static final RedundantExceptionFilter REDUNDANT_LOGGING_FILTER = RedundantExceptionFilter.getRedundantExceptionFilter();

  /**
   * TODO: Make sure services "venice-hooks-deployable" is also in whitelist
   */
  public NodesAndReplicas(Optional<DynamicAccessController> accessController) {
    super(accessController);
  }

  public Route listAllNodes(Admin admin) {
    return (request, response) -> {
      MultiNodeResponse responseObject = new MultiNodeResponse();
      response.type(HttpConstants.JSON);
      try {
        // Only allow whitelist users to run this command
        if (!isWhitelistUsers(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError("Only admin users are allowed to run " + request.url());
          return AdminSparkServer.mapper.writeValueAsString(responseObject);
        }
        AdminSparkServer.validateParams(request, LIST_NODES.getParams(), admin);
        responseObject.setCluster(request.queryParams(CLUSTER));
        List<String> nodeList = admin.getStorageNodes(responseObject.getCluster());
        String[] nodeListArray = new String[nodeList.size()];
        for (int i=0; i<nodeList.size(); i++){
          nodeListArray[i] = nodeList.get(i);
        }
        responseObject.setNodes(nodeListArray);
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public Route listAllNodesStatus(Admin admin) {
    return (request, response) -> {
      MultiNodesStatusResponse responseObject = new MultiNodesStatusResponse();
      response.type(HttpConstants.JSON);
      try {
        // Only allow whitelist users to run this command
        if (!isWhitelistUsers(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError("Only admin users are allowed to run " + request.url());
          return AdminSparkServer.mapper.writeValueAsString(responseObject);
        }
        AdminSparkServer.validateParams(request, ClUSTER_HEALTH_INSTANCES.getParams(), admin);
        responseObject.setCluster(request.queryParams(CLUSTER));
        Map<String, String> nodesStatusesMap = admin.getStorageNodesStatus(responseObject.getCluster());
        responseObject.setInstancesStatusMap(nodesStatusesMap);
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public Route listReplicasForStore(Admin admin) {
    return (request, response) -> {
      MultiReplicaResponse responseObject = new MultiReplicaResponse();
      response.type(HttpConstants.JSON);
      try {
        // Only allow whitelist users to run this command
        if (!isWhitelistUsers(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError("Only admin users are allowed to run " + request.url());
          return AdminSparkServer.mapper.writeValueAsString(responseObject);
        }
        AdminSparkServer.validateParams(request, LIST_REPLICAS.getParams(), admin);
        responseObject.setCluster(request.queryParams(CLUSTER));
        responseObject.setName(request.queryParams(NAME));
        responseObject.setVersion(Utils.parseIntFromString(request.queryParams(VERSION), "VERSION"));
        List<Replica> replicaList = admin.getReplicas(responseObject.getCluster(), responseObject.getTopic());
        Replica[] replicaArray = new Replica[replicaList.size()];
        for (int i=0; i<replicaList.size(); i++){
          replicaArray[i] = replicaList.get(i);
        }
        responseObject.setReplicas(replicaArray);
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public Route listReplicasForStorageNode(Admin admin){
    return (request, response) -> {
      MultiReplicaResponse responseObject = new MultiReplicaResponse();
      response.type(HttpConstants.JSON);
      try {
        // Only allow whitelist users to run this command
        if (!isWhitelistUsers(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError("Only admin users are allowed to run " + request.url());
          return AdminSparkServer.mapper.writeValueAsString(responseObject);
        }
        AdminSparkServer.validateParams(request, NODE_REPLICAS.getParams(), admin);
        responseObject.setCluster(request.queryParams(CLUSTER));
        String nodeId = request.queryParams(STORAGE_NODE_ID);
        List<Replica> replicaList = admin.getReplicasOfStorageNode(responseObject.getCluster(), nodeId);
        Replica[] replicaArray = new Replica[replicaList.size()];
        for (int i=0; i<replicaList.size(); i++){
          replicaArray[i] = replicaList.get(i);
        }
        responseObject.setReplicas(replicaArray);
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public Route isNodeRemovable(Admin admin){
    return (request, response) -> {
      NodeStatusResponse responseObject = new NodeStatusResponse();
      response.type(HttpConstants.JSON);
      try {
        // Only allow whitelist users to run this command
        if (!isWhitelistUsers(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError("Only admin users are allowed to run " + request.url());
          return AdminSparkServer.mapper.writeValueAsString(responseObject);
        }
        AdminSparkServer.validateParams(request, NODE_REMOVABLE.getParams(), admin);
        responseObject.setCluster(request.queryParams(CLUSTER));
        String nodeId = request.queryParams(STORAGE_NODE_ID);
        String[] instanceView = request.queryMap().toMap().get(INSTANCE_VIEW);
        NodeRemovableResult result;
        if (instanceView != null && Boolean.valueOf(instanceView[0])) {
          result = admin.isInstanceRemovable(responseObject.getCluster(), nodeId, true);
        } else {
          result = admin.isInstanceRemovable(responseObject.getCluster(), nodeId, false);
        }
        responseObject.setRemovable(result.isRemovable());
        // Add detail reason why this instance could not be removed.
        if (!result.isRemovable()) {
          StringBuilder msgBuilder = new StringBuilder();
          msgBuilder.append(nodeId)
                    .append(" could not be removed from cluster: ")
                    .append(responseObject.getCluster())
                    .append(", because resource: ")
                    .append(result.getBlockingResource())
                    .append(" will ")
                    .append(result.getBlockingReason())
                    .append(" after removing this node. Details: ")
                    .append(result.getDetails());
          String errorResponseMessage = msgBuilder.toString();
          if (!REDUNDANT_LOGGING_FILTER.isRedundantException(nodeId)) {
            LOGGER.warn(errorResponseMessage);
          }
          responseObject.setDetails(errorResponseMessage);
        }
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public Route removeNodeFromCluster(Admin admin) {
    return (request, response) -> {
      ControllerResponse responseObject = new ControllerResponse();
      response.type(HttpConstants.JSON);
      try {
        // Only allow whitelist users to run this command
        if (!isWhitelistUsers(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError("Only admin users are allowed to run " + request.url());
          return AdminSparkServer.mapper.writeValueAsString(responseObject);
        }
        AdminSparkServer.validateParams(request, REMOVE_NODE.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        responseObject.setCluster(cluster);
        String nodeId = request.queryParams(STORAGE_NODE_ID);
        admin.removeStorageNode(cluster, nodeId);
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public Route addNodeIntoWhiteList(Admin admin) {
    return (request, response) -> {
      ControllerResponse responseObject = new ControllerResponse();
      response.type(HttpConstants.JSON);
      try {
        // Only allow whitelist users to run this command
        if (!isWhitelistUsers(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError("Only admin users are allowed to run " + request.url());
          return AdminSparkServer.mapper.writeValueAsString(responseObject);
        }
        AdminSparkServer.validateParams(request, WHITE_LIST_ADD_NODE.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        responseObject.setCluster(cluster);
        String nodeId = request.queryParams(STORAGE_NODE_ID);
        admin.addInstanceToWhitelist(cluster, nodeId);
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public Route removeNodeFromWhiteList(Admin admin){
    return (request, response) -> {
      ControllerResponse responseObject = new ControllerResponse();
      response.type(HttpConstants.JSON);
      try {
        // Only allow whitelist users to run this command
        if (!isWhitelistUsers(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError("Only admin users are allowed to run " + request.url());
          return AdminSparkServer.mapper.writeValueAsString(responseObject);
        }
        AdminSparkServer.validateParams(request, WHITE_LIST_REMOVE_NODE.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        responseObject.setCluster(cluster);
        String nodeId = request.queryParams(STORAGE_NODE_ID);
        admin.removeInstanceFromWhiteList(cluster, nodeId);
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }
}
