package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.MultiNodeResponse;
import com.linkedin.venice.controllerapi.MultiReplicaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.Replica;
import com.linkedin.venice.utils.Utils;
import java.util.List;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORAGE_NODE_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VERSION;
import static com.linkedin.venice.controllerapi.ControllerRoute.LIST_NODES;
import static com.linkedin.venice.controllerapi.ControllerRoute.LIST_REPLICAS;
import static com.linkedin.venice.controllerapi.ControllerRoute.NODE_REPLICAS;


public class NodesAndReplicas {

  public static Route listAllNodes(Admin admin) {
    return (request, response) -> {
      MultiNodeResponse responseObject = new MultiNodeResponse();
      try {
        AdminSparkServer.validateParams(request, LIST_NODES.getParams(), admin);
        responseObject.setCluster(request.queryParams(CLUSTER));
        List<String> nodeList = admin.getStorageNodes(responseObject.getCluster());
        String[] nodeListArray = new String[nodeList.size()];
        for (int i=0; i<nodeList.size(); i++){
          nodeListArray[i] = nodeList.get(i);
        }
        responseObject.setNodes(nodeListArray);
      } catch (VeniceException e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public static Route listReplicasForStore(Admin admin) {
    return (request, response) -> {
      MultiReplicaResponse responseObject = new MultiReplicaResponse();
      try {
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
      } catch (VeniceException e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  public static Route listReplicasForStorageNode(Admin admin){
    return (request, response) -> {
      MultiReplicaResponse responseObject = new MultiReplicaResponse();
      try {
        AdminSparkServer.validateParams(request, NODE_REPLICAS.getParams(), admin);
        responseObject.setCluster(request.queryParams(CLUSTER));
        String nodeId = request.queryParams(STORAGE_NODE_ID);
        List<Replica> replicaList = admin.getReplicasOfStorageNode(responseObject.getCluster(), nodeId);
        Replica[] replicaArray = new Replica[replicaList.size()];
        for (int i=0; i<replicaList.size(); i++){
          replicaArray[i] = replicaList.get(i);
        }
        responseObject.setReplicas(replicaArray);
      } catch (VeniceException e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

}
