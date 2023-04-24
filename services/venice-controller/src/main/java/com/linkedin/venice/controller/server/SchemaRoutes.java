package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.DERIVED_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.DERIVED_SCHEMA_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.SCHEMA_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VALUE_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerRoute.ADD_DERIVED_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerRoute.ADD_VALUE_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_ALL_REPLICATION_METADATA_SCHEMAS;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_ALL_VALUE_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_KEY_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_VALUE_OR_DERIVED_SCHEMA_ID;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_VALUE_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_VALUE_SCHEMA_ID;
import static com.linkedin.venice.controllerapi.ControllerRoute.REMOVE_DERIVED_SCHEMA;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.authentication.AuthenticationService;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.InvalidVeniceSchemaException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.schema.GeneratedSchemaID;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.http.HttpStatus;
import spark.Route;


public class SchemaRoutes extends AbstractRoute {
  public SchemaRoutes(
      boolean sslEnabled,
      Optional<DynamicAccessController> accessController,
      Optional<AuthenticationService> authenticationService,
      Optional<AuthorizerService> authorizerService) {
    super(sslEnabled, accessController, authenticationService, authorizerService);
  }

  /**
   * Route to handle retrieving key schema request.
   * @see Admin#getKeySchema(String, String)
   */
  public Route getKeySchema(Admin admin) {
    return (request, response) -> {
      SchemaResponse responseObject = new SchemaResponse();
      response.type(HttpConstants.JSON);
      try {
        // No ACL check on getting store metadata
        AdminSparkServer.validateParams(request, GET_KEY_SCHEMA.getParams(), admin);
        responseObject.setCluster(request.queryParams(CLUSTER));
        responseObject.setName(request.queryParams(NAME));
        SchemaEntry keySchemaEntry = admin.getKeySchema(responseObject.getCluster(), responseObject.getName());
        if (keySchemaEntry == null) {
          throw new VeniceException("Key schema doesn't exist for store: " + responseObject.getName());
        }
        responseObject.setId(keySchemaEntry.getId());
        responseObject.setSchemaStr(keySchemaEntry.getSchema().toString());
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(new VeniceException(e), request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  /**
   * Route to handle adding value schema request.
   * @see Admin#addValueSchema(String, String, String, int, boolean)
   */
  public Route addValueSchema(Admin admin) {
    return (request, response) -> {
      SchemaResponse responseObject = new SchemaResponse();
      response.type(HttpConstants.JSON);
      try {
        // Only allow allowlist users to run this command
        if (!isAllowListUser(request) && !hasWriteAccessToTopic(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError("User is neither Admin nor has write access to topic to run " + request.url());
          return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
        }
        AdminSparkServer.validateParams(request, ADD_VALUE_SCHEMA.getParams(), admin);
        responseObject.setCluster(request.queryParams(CLUSTER));
        responseObject.setName(request.queryParams(NAME));
        String schemaIdString = request.queryParams(SCHEMA_ID);
        SchemaEntry valueSchemaEntry;
        if (schemaIdString != null) {
          // Schema id is specified which suggests that the request is coming from metadata copy.
          valueSchemaEntry = admin.addValueSchema(
              responseObject.getCluster(),
              responseObject.getName(),
              request.queryParams(VALUE_SCHEMA),
              Integer.parseInt(schemaIdString),
              false);
        } else {
          valueSchemaEntry = admin.addValueSchema(
              responseObject.getCluster(),
              responseObject.getName(),
              request.queryParams(VALUE_SCHEMA),
              SchemaEntry.DEFAULT_SCHEMA_CREATION_COMPATIBILITY_TYPE
          // TODO: Make compat type configurable to allow force registration
          );
        }
        responseObject.setId(valueSchemaEntry.getId());
        responseObject.setSchemaStr(valueSchemaEntry.getSchema().toString());
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(new VeniceException(e), request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  /**
   * @see Admin#addDerivedSchema(String, String, int, int, String)
   */
  public Route addDerivedSchema(Admin admin) {
    return (request, response) -> {
      SchemaResponse responseObject = new SchemaResponse();
      response.type(HttpConstants.JSON);
      try {
        // Only allow allowlist users to run this command
        if (!isAllowListUser(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError("Only admin users are allowed to run " + request.url());
          responseObject.setErrorType(ErrorType.BAD_REQUEST);
          return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
        }
        AdminSparkServer.validateParams(request, ADD_DERIVED_SCHEMA.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        int valueSchemaId =
            Utils.parseIntFromString(request.queryParams(ControllerApiConstants.SCHEMA_ID), "value schema id");

        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);

        // Fail on adding derived schema if the value schema doesn't exist
        SchemaEntry valueSchemaEntry = admin.getValueSchema(clusterName, storeName, valueSchemaId);
        if (valueSchemaEntry == null) {
          throw new VeniceException(
              "Value schema for schema id: " + valueSchemaId + " of store: " + storeName + " doesn't exist");
        }

        String derivedSchemaIdString = request.queryParams(DERIVED_SCHEMA_ID);
        DerivedSchemaEntry derivedSchemaEntry;
        if (derivedSchemaIdString != null) {
          // Derived schema id is specified which suggests that the request is coming from metadata copy.
          derivedSchemaEntry = admin.addDerivedSchema(
              clusterName,
              storeName,
              valueSchemaId,
              Integer.parseInt(derivedSchemaIdString),
              request.queryParams(DERIVED_SCHEMA));
        } else {
          derivedSchemaEntry =
              admin.addDerivedSchema(clusterName, storeName, valueSchemaId, request.queryParams(DERIVED_SCHEMA));
        }
        responseObject.setId(derivedSchemaEntry.getValueSchemaID());
        responseObject.setDerivedSchemaId(derivedSchemaEntry.getId());
        responseObject.setSchemaStr(derivedSchemaEntry.getSchema().toString());
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(new VeniceException(e), request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  /**
   * Route to handle retrieving value schema by id.
   * @see Admin#getValueSchema(String, String, int)
   */
  public Route getValueSchema(Admin admin) {
    return (request, response) -> {
      SchemaResponse responseObject = new SchemaResponse();
      response.type(HttpConstants.JSON);
      try {
        // No ACL check on getting store metadata
        AdminSparkServer.validateParams(request, GET_VALUE_SCHEMA.getParams(), admin);
        responseObject.setCluster(request.queryParams(CLUSTER));
        responseObject.setName(request.queryParams(NAME));
        String schemaId = request.queryParams(SCHEMA_ID);
        SchemaEntry valueSchemaEntry = admin.getValueSchema(
            responseObject.getCluster(),
            responseObject.getName(),
            Utils.parseIntFromString(schemaId, "schema id"));
        if (valueSchemaEntry == null) {
          throw new VeniceException(
              "Value schema for schema id: " + schemaId + " of store: " + responseObject.getName() + " doesn't exist");
        }
        responseObject.setId(valueSchemaEntry.getId());
        responseObject.setSchemaStr(valueSchemaEntry.getSchema().toString());
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(new VeniceException(e), request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  /**
   * Route to handle retrieving schema id by schema.
   * @see Admin#getValueSchemaId(String, String, String)
   */
  public Route getValueSchemaID(Admin admin) {
    return (request, response) -> {
      SchemaResponse responseObject = new SchemaResponse();
      response.type(HttpConstants.JSON);
      try {
        // No ACL check on getting store metadata
        AdminSparkServer.validateParams(request, GET_VALUE_SCHEMA_ID.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        String store = request.queryParams(NAME);
        String schemaStr = request.queryParams(VALUE_SCHEMA);

        responseObject.setCluster(cluster);
        responseObject.setName(store);
        responseObject.setSchemaStr(schemaStr);

        int valueSchemaId = admin.getValueSchemaId(cluster, store, schemaStr);

        if (SchemaData.INVALID_VALUE_SCHEMA_ID == valueSchemaId) {
          throw new InvalidVeniceSchemaException(
              "Can not find any registered value schema for the store " + store
                  + " that matches the schema of data being pushed.\n" + schemaStr);
        }

        responseObject.setId(valueSchemaId);
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(new VeniceException(e), request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  /**
   * @see Admin#getValueSchemaId(String, String, String)
   */
  public Route getValueOrDerivedSchemaID(Admin admin) {
    return (request, response) -> {
      SchemaResponse responseObject = new SchemaResponse();
      response.type(HttpConstants.JSON);
      try {
        // No ACL check on getting store metadata
        AdminSparkServer.validateParams(request, GET_VALUE_OR_DERIVED_SCHEMA_ID.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        String store = request.queryParams(NAME);
        String schemaStr = request.queryParams(DERIVED_SCHEMA);

        responseObject = populateSchemaResponseForValueOrDerivedSchemaID(admin, cluster, store, schemaStr);
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(new VeniceException(e), request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  SchemaResponse populateSchemaResponseForValueOrDerivedSchemaID(
      Admin admin,
      String cluster,
      String store,
      String schemaStr) {
    SchemaResponse responseObject = new SchemaResponse();
    responseObject.setCluster(cluster);
    responseObject.setName(store);
    responseObject.setSchemaStr(schemaStr);

    int id = admin.getValueSchemaId(cluster, store, schemaStr);

    if (id == SchemaData.INVALID_VALUE_SCHEMA_ID) {
      Store storeInfo = admin.getStore(cluster, store);
      if (storeInfo == null) {
        throw new VeniceNoStoreException(store, cluster);
      }
      if (storeInfo.isWriteComputationEnabled()) {
        GeneratedSchemaID idPair = admin.getDerivedSchemaId(cluster, store, schemaStr);
        if (SchemaData.INVALID_VALUE_SCHEMA_ID == idPair.getValueSchemaID()) {
          throw new VeniceException(
              "Can not find any registered value schema nor derived schema for the store " + store
                  + " that matches the schema of data being pushed. Requested schema: \n" + schemaStr);
        }
        responseObject.setId(idPair.getValueSchemaID());
        responseObject.setDerivedSchemaId(idPair.getGeneratedSchemaVersion());
      } else {
        throw new VeniceException(
            "Can not find any registered value schema for the store " + store
                + " that matches the schema of data being pushed. Requested schema: \n" + schemaStr);
      }
    } else {
      responseObject.setId(id);
    }
    return responseObject;
  }

  /**
   * Route to handle retrieving all value schema for a given store.
   * @see Admin#getValueSchemas(String, String)
   */
  public Route getAllValueSchema(Admin admin) {
    return (request, response) -> {
      MultiSchemaResponse responseObject = new MultiSchemaResponse();
      response.type(HttpConstants.JSON);
      try {
        // No ACL check on getting store metadata
        AdminSparkServer.validateParams(request, GET_ALL_VALUE_SCHEMA.getParams(), admin);
        responseObject.setCluster(request.queryParams(CLUSTER));
        responseObject.setName(request.queryParams(NAME));
        Collection<SchemaEntry> valueSchemaEntries =
            admin.getValueSchemas(responseObject.getCluster(), responseObject.getName())
                .stream()
                .sorted(Comparator.comparingInt(SchemaEntry::getId))
                .collect(Collectors.toList());
        Store store = admin.getStore(responseObject.getCluster(), responseObject.getName());

        int schemaNum = valueSchemaEntries.size();
        MultiSchemaResponse.Schema[] schemas = new MultiSchemaResponse.Schema[schemaNum];
        int cur = 0;
        for (SchemaEntry entry: valueSchemaEntries) {
          schemas[cur] = new MultiSchemaResponse.Schema();
          schemas[cur].setId(entry.getId());
          schemas[cur].setSchemaStr(entry.getSchema().toString());
          ++cur;
        }
        responseObject.setSuperSetSchemaId(store.getLatestSuperSetValueSchemaId());
        responseObject.setSchemas(schemas);
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(new VeniceException(e), request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  /**
   * @see Admin#getValueSchemas(String, String)
   * @see Admin#getDerivedSchemas(String, String)
   */
  public Route getAllValueAndDerivedSchema(Admin admin) {
    return (request, response) -> {
      MultiSchemaResponse responseObject = new MultiSchemaResponse();
      response.type(HttpConstants.JSON);
      try {
        // No ACL check on getting store metadata
        AdminSparkServer.validateParams(request, GET_ALL_VALUE_SCHEMA.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        String store = request.queryParams(NAME);

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
          schema.setId(derivedSchemaEntry.getValueSchemaID());
          schema.setDerivedSchemaId(derivedSchemaEntry.getId());
          schema.setSchemaStr(derivedSchemaEntry.getSchema().toString());
          schemas.add(schema);
        });

        MultiSchemaResponse.Schema[] schemaArray = new MultiSchemaResponse.Schema[schemas.size()];
        schemas.toArray(schemaArray);

        responseObject.setSchemas(schemaArray);
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(new VeniceException(e), request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  /**
   * @see Admin#removeDerivedSchema(String, String, int, int)
   */
  public Route removeDerivedSchema(Admin admin) {
    return (request, response) -> {
      SchemaResponse responseObject = new SchemaResponse();
      response.type(HttpConstants.JSON);
      try {
        // Only allow allowlist users to run this command
        if (!isAllowListUser(request)) {
          response.status(HttpStatus.SC_FORBIDDEN);
          responseObject.setError("Only admin users are allowed to run " + request.url());
          responseObject.setErrorType(ErrorType.BAD_REQUEST);
          return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
        }

        AdminSparkServer.validateParams(request, REMOVE_DERIVED_SCHEMA.getParams(), admin);
        String cluster = request.queryParams(CLUSTER);
        String store = request.queryParams(NAME);
        int schemaId = Utils.parseIntFromString(request.queryParams(SCHEMA_ID), "schema id");
        int derivedSchemaId = Utils.parseIntFromString(request.queryParams(DERIVED_SCHEMA_ID), "derived schema id");

        DerivedSchemaEntry removedDerivedSchemaEntry =
            admin.removeDerivedSchema(cluster, store, schemaId, derivedSchemaId);
        if (removedDerivedSchemaEntry == null) {
          throw new VeniceException(
              "Derived schema for schema id: " + schemaId + " of store: " + responseObject.getName()
                  + " doesn't exist");
        }

        responseObject.setCluster(cluster);
        responseObject.setName(store);
        responseObject.setId(derivedSchemaId);
        responseObject.setDerivedSchemaId(derivedSchemaId);
        responseObject.setSchemaStr(removedDerivedSchemaEntry.getSchema().toString());
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(new VeniceException(e), request, response);
      }

      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }

  /**
   * @see Admin#getReplicationMetadataSchemas(String, String)
   */
  public Route getAllReplicationMetadataSchemas(Admin admin) {
    return (request, response) -> {
      MultiSchemaResponse responseObject = new MultiSchemaResponse();
      response.type(HttpConstants.JSON);
      try {
        AdminSparkServer.validateParams(request, GET_ALL_REPLICATION_METADATA_SCHEMAS.getParams(), admin);
        responseObject.setCluster(request.queryParams(CLUSTER));
        responseObject.setName(request.queryParams(NAME));
        Collection<RmdSchemaEntry> valueSchemaEntries =
            admin.getReplicationMetadataSchemas(responseObject.getCluster(), responseObject.getName())
                .stream()
                .sorted(Comparator.comparingInt(RmdSchemaEntry::getId))
                .collect(Collectors.toList());
        MultiSchemaResponse.Schema[] schemas = new MultiSchemaResponse.Schema[valueSchemaEntries.size()];
        int cur = 0;
        for (RmdSchemaEntry entry: valueSchemaEntries) {
          schemas[cur] = new MultiSchemaResponse.Schema();
          schemas[cur].setId(entry.getId());
          schemas[cur].setSchemaStr(entry.getSchema().toString());
          schemas[cur].setRmdValueSchemaId(entry.getValueSchemaID());
          ++cur;
        }
        responseObject.setSchemas(schemas);
      } catch (Throwable e) {
        responseObject.setError(e);
        AdminSparkServer.handleError(new VeniceException(e), request, response);
      }
      return AdminSparkServer.OBJECT_MAPPER.writeValueAsString(responseObject);
    };
  }
}
