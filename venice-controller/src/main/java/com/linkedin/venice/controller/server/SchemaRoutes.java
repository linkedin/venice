package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.utils.Utils;
import java.util.Comparator;
import java.util.stream.Collectors;
import spark.Route;

import java.util.Collection;

import static com.linkedin.venice.controllerapi.ControllerRoute.*;


public class SchemaRoutes {
  private SchemaRoutes() {}

  // Route to handle retrieving key schema request
  public static Route getKeySchema(Admin admin) {
    return (request, response) -> {
      SchemaResponse responseObject = new SchemaResponse();
      try {
        AdminSparkServer.validateParams(request, GET_KEY_SCHEMA.getParams(), admin);
        responseObject.setCluster(request.queryParams(ControllerApiConstants.CLUSTER));
        responseObject.setName(request.queryParams(ControllerApiConstants.NAME));
        SchemaEntry keySchemaEntry = admin.getKeySchema(responseObject.getCluster(),
            responseObject.getName());
        if (null == keySchemaEntry) {
          throw new VeniceException("Key schema doesn't exist for store: " + responseObject.getName());
        }
        responseObject.setId(keySchemaEntry.getId());
        responseObject.setSchemaStr(keySchemaEntry.getSchema().toString());
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(new VeniceException(e), request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  // Route to handle adding value schema request
  public static Route addValueSchema(Admin admin) {
    return (request, response) -> {
      SchemaResponse responseObject = new SchemaResponse();
      try {
        AdminSparkServer.validateParams(request, ADD_VALUE_SCHEMA.getParams(), admin);
        responseObject.setCluster(request.queryParams(ControllerApiConstants.CLUSTER));
        responseObject.setName(request.queryParams(ControllerApiConstants.NAME));
        SchemaEntry valueSchemaEntry = admin.addValueSchema(
            responseObject.getCluster(),
            responseObject.getName(),
            request.queryParams(ControllerApiConstants.VALUE_SCHEMA),
            DirectionalSchemaCompatibilityType.FULL // TODO: Make compat type configurable to allow force registration
        );
        responseObject.setId(valueSchemaEntry.getId());
        responseObject.setSchemaStr(valueSchemaEntry.getSchema().toString());
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(new VeniceException(e), request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  // Route to handle retrieving value schema by id
  public static Route getValueSchema(Admin admin) {
    return (request, response) -> {
      SchemaResponse responseObject = new SchemaResponse();
      try {
        AdminSparkServer.validateParams(request, GET_VALUE_SCHEMA.getParams(), admin);
        responseObject.setCluster(request.queryParams(ControllerApiConstants.CLUSTER));
        responseObject.setName(request.queryParams(ControllerApiConstants.NAME));
        String schemaId = request.queryParams(ControllerApiConstants.SCHEMA_ID);
        SchemaEntry valueSchemaEntry = admin.getValueSchema(responseObject.getCluster(),
            responseObject.getName(),
            Utils.parseIntFromString(schemaId, "schema id"));
        if (null == valueSchemaEntry) {
          throw new VeniceException("Value schema for schema id: " + schemaId
              + " of store: " + responseObject.getName() + " doesn't exist");
        }
        responseObject.setId(valueSchemaEntry.getId());
        responseObject.setSchemaStr(valueSchemaEntry.getSchema().toString());
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(new VeniceException(e), request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  // Route to handle retrieving schema id by schema
  public static Route getValueSchemaID(Admin admin) {
    return (request, response) -> {
      SchemaResponse responseObject = new SchemaResponse();
      try {
        AdminSparkServer.validateParams(request, GET_VALUE_SCHEMA_ID.getParams(), admin);
        responseObject.setCluster(request.queryParams(ControllerApiConstants.CLUSTER));
        responseObject.setName(request.queryParams(ControllerApiConstants.NAME));
        responseObject.setSchemaStr(request.queryParams(ControllerApiConstants.VALUE_SCHEMA));
        int valueSchemaId = admin.getValueSchemaId(responseObject.getCluster(),
            responseObject.getName(),
            responseObject.getSchemaStr());
        if (SchemaData.INVALID_VALUE_SCHEMA_ID == valueSchemaId) {
          throw new VeniceException("Can not find any registered value schema for the store " +
              responseObject.getName() + " that matches the schema of data being pushed.");
        }
        responseObject.setId(valueSchemaId);
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(new VeniceException(e), request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  // Route to handle retrieving all value schema for a given store
  public static Route getAllValueSchema(Admin admin) {
    return (request, response) -> {
      MultiSchemaResponse responseObject = new MultiSchemaResponse();
      try {
        AdminSparkServer.validateParams(request, GET_ALL_VALUE_SCHEMA.getParams(), admin);
        responseObject.setCluster(request.queryParams(ControllerApiConstants.CLUSTER));
        responseObject.setName(request.queryParams(ControllerApiConstants.NAME));
        Collection<SchemaEntry> valueSchemaEntries =
            admin.getValueSchemas(responseObject.getCluster(), responseObject.getName())
                .stream()
                .sorted(Comparator.comparingInt(SchemaEntry::getId))
                .collect(Collectors.toList());

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
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(new VeniceException(e), request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }
}
