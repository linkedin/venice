package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.DerivedSchemaEntry;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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
            SchemaEntry.DEFAULT_SCHEMA_CREATION_COMPATIBILITY_TYPE
            // TODO: Make compat type configurable to allow force registration
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

  public static Route addDerivedSchema(Admin admin) {
    return (request, response) -> {
      SchemaResponse responseObject = new SchemaResponse();
      try {
        AdminSparkServer.validateParams(request, ADD_DERIVED_SCHEMA.getParams(), admin);
        String clusterName = request.queryParams(ControllerApiConstants.CLUSTER);
        String storeName = request.queryParams(ControllerApiConstants.NAME);
        int valueSchemaId = Utils.parseIntFromString(request.queryParams(ControllerApiConstants.SCHEMA_ID),
            "value schema id");

        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);

        DerivedSchemaEntry derivedSchemaEntry = admin.addDerivedSchema(clusterName, storeName, valueSchemaId,
            request.queryParams(ControllerApiConstants.DERIVED_SCHEMA));
        responseObject.setId(derivedSchemaEntry.getValueSchemaId());
        responseObject.setDerivedSchemaId(derivedSchemaEntry.getId());
        responseObject.setSchemaStr(derivedSchemaEntry.getSchema().toString());
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
        String cluster = request.queryParams(ControllerApiConstants.CLUSTER);
        String store = request.queryParams(ControllerApiConstants.NAME);
        String schemaStr = request.queryParams(ControllerApiConstants.VALUE_SCHEMA);

        responseObject.setCluster(cluster);
        responseObject.setName(store);
        responseObject.setSchemaStr(schemaStr);

        int valueSchemaId = admin.getValueSchemaId(cluster, store, schemaStr);

        if (SchemaData.INVALID_VALUE_SCHEMA_ID == valueSchemaId) {
          throw new VeniceException("Can not find any registered value schema for the store " + store
              + " that matches the schema of data being pushed.\n" + schemaStr);
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

  public static Route getDerivedSchemaID(Admin admin) {
    return (request, response) -> {
      SchemaResponse responseObject = new SchemaResponse();
      try {
        AdminSparkServer.validateParams(request, GET_VALUE_OR_DERIVED_SCHEMA_ID.getParams(), admin);
        String cluster = request.queryParams(ControllerApiConstants.CLUSTER);
        String store = request.queryParams(ControllerApiConstants.NAME);
        String schemaStr = request.queryParams(ControllerApiConstants.DERIVED_SCHEMA);

        responseObject.setCluster(cluster);
        responseObject.setName(store);
        responseObject.setSchemaStr(schemaStr);

        int id = admin.getValueSchemaId(cluster, store, schemaStr);

        if (SchemaData.INVALID_VALUE_SCHEMA_ID == id) {
          Pair<Integer, Integer> idPair = admin.getDerivedSchemaId(cluster, store, schemaStr);
          if (SchemaData.INVALID_VALUE_SCHEMA_ID == idPair.getFirst()) {
            throw new VeniceException("Can not find any registered derived schema for the store " + store +
                " that matches the schema of data being pushed. mismatched derived schema: \n" + schemaStr);
          }
          responseObject.setId(idPair.getFirst());
          responseObject.setDerivedSchemaId(idPair.getSecond());
        } else {
          responseObject.setId(id);
        }


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

  public static Route getAllValueAndDerivedSchema(Admin admin) {
    return (request, response) -> {
      MultiSchemaResponse responseObject = new MultiSchemaResponse();
      try {
        AdminSparkServer.validateParams(request, GET_ALL_VALUE_SCHEMA.getParams(), admin);
        String cluster = request.queryParams(ControllerApiConstants.CLUSTER);
        String store = request.queryParams(ControllerApiConstants.NAME);

        responseObject.setCluster(cluster);
        responseObject.setName(store);

        List<MultiSchemaResponse.Schema> schemas = new ArrayList<>();
        admin.getValueSchemas(cluster, store).forEach(schemaEntry -> {
          MultiSchemaResponse.Schema schema = new MultiSchemaResponse.Schema();
          schema.setId(schemaEntry.getId());
          schema.setSchemaStr(schemaEntry.getSchema().toString());
          schemas.add(schema);
        });

        admin.getDerivedSchemas(cluster, store).forEach(derivedSchemaEntry -> {
          MultiSchemaResponse.Schema schema = new MultiSchemaResponse.Schema();
          schema.setId(derivedSchemaEntry.getValueSchemaId());
          schema.setDerivedSchemaId(derivedSchemaEntry.getId());
          schema.setSchemaStr(derivedSchemaEntry.getSchema().toString());
          schemas.add(schema);
        });

        MultiSchemaResponse.Schema[] schemaArray = new MultiSchemaResponse.Schema[schemas.size()];
        schemas.toArray(schemaArray);

        responseObject.setSchemas(schemaArray);
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(new VeniceException(e), request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }
}
