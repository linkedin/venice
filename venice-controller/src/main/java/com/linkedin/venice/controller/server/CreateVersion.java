package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.Utils;
import org.apache.log4j.Logger;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.CREATE;


/**
 * Created by mwise on 5/18/16.
 *
 * TODO: remove store creation and schema creation from this class.
 * Right now, it seems only unit test is using this path, and it won't be used by other
 * production logic in the future, we should remove this class.
 */
public class CreateVersion {
  private static final Logger logger = Logger.getLogger(CreateVersion.class);

  public static Route getRoute(Admin admin) {
    return (request, response) -> {
      VersionCreationResponse responseObject = new VersionCreationResponse();
      try {
        AdminSparkServer.validateParams(request, CREATE.getParams(), admin);
        responseObject.setCluster(request.queryParams(CLUSTER));
        responseObject.setName(request.queryParams(NAME));
        responseObject.setOwner(request.queryParams(OWNER));
        // TODO we should verify the data size at first. If it exceeds the quota, controller should reject this request.
        // TODO And also we should use quota to calculate partition count to avoid this case that data size of first
        // push is very small but grow dramatically because quota of this store is very large.
        try { // TODO: use admin to update store with new owner?  Set owner at version level for audit history?
          admin.addStore(responseObject.getCluster(), responseObject.getName(), responseObject.getOwner());
        } catch (VeniceException e) { // TODO method on admin to see if store already created?
          logger.warn("Store" + responseObject.getName() + " probably already created.", e);
        }
        // Store size in Bytes
        long storeSize = Utils.parseLongFromString(request.queryParams(STORE_SIZE), STORE_SIZE);
        responseObject.setPartitions(admin.calculateNumberOfPartitions(responseObject.getCluster(),responseObject.getName(),storeSize));
        responseObject.setReplicas(admin.getReplicaFactor(responseObject.getCluster(),responseObject.getName()));
        Version version = admin
            .incrementVersion(responseObject.getCluster(), responseObject.getName(), responseObject.getPartitions(),
                responseObject.getReplicas());
        responseObject.setVersion(version.getNumber());
        responseObject.setKafkaTopic(version.kafkaTopicName());
        responseObject.setKafkaBootstrapServers(admin.getKafkaBootstrapServers());
        // Schema validation/creation
        admin.initKeySchema(responseObject.getCluster(), responseObject.getName(), request.queryParams(KEY_SCHEMA));
        SchemaEntry valueSchemaEntry = admin.addValueSchema(responseObject.getCluster(),
            responseObject.getName(), request.queryParams(VALUE_SCHEMA));
        responseObject.setValueSchemaId(valueSchemaEntry.getId());
      } catch (VeniceException e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }
}
