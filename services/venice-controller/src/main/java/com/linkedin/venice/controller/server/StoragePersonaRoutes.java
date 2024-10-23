package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PERSONA_NAME;
import static com.linkedin.venice.controllerapi.ControllerRoute.CREATE_STORAGE_PERSONA;
import static com.linkedin.venice.controllerapi.ControllerRoute.DELETE_STORAGE_PERSONA;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_CLUSTER_STORAGE_PERSONAS;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_STORAGE_PERSONA;
import static com.linkedin.venice.controllerapi.ControllerRoute.GET_STORAGE_PERSONA_ASSOCIATED_WITH_STORE;
import static com.linkedin.venice.controllerapi.ControllerRoute.UPDATE_STORAGE_PERSONA;

import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.MultiStoragePersonaResponse;
import com.linkedin.venice.controllerapi.StoragePersonaResponse;
import com.linkedin.venice.controllerapi.UpdateStoragePersonaQueryParams;
import com.linkedin.venice.persona.StoragePersona;
import com.linkedin.venice.utils.Utils;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import spark.Request;
import spark.Route;


public class StoragePersonaRoutes extends AbstractRoute {
  public StoragePersonaRoutes(
      boolean sslEnabled,
      Optional<DynamicAccessController> accessController,
      VeniceControllerRequestHandler requestHandler) {
    super(sslEnabled, accessController, requestHandler);
  }

  /**
   * @see Admin#createStoragePersona(String, String, long, Set, Set)
   */
  public Route createStoragePersona(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        AdminSparkServer.validateParams(request, CREATE_STORAGE_PERSONA.getParams(), admin);

        Map<String, String> params = Utils.extractQueryParamsFromRequest(request.queryMap().toMap(), veniceResponse);
        UpdateStoragePersonaQueryParams personaParams = new UpdateStoragePersonaQueryParams(params);

        String clusterName = request.queryParams(CLUSTER);

        String name = personaParams.getName().get();
        long quota = personaParams.getQuota().get();
        Set<String> storesToEnforce = personaParams.getStoresToEnforce().get();
        Set<String> owners = personaParams.getOwners().get();

        try {
          admin.createStoragePersona(clusterName, name, quota, storesToEnforce, owners);
        } catch (Exception e) {
          veniceResponse.setError("Failed when creating persona " + name, e);
        }
      }
    };
  }

  /**
   * @see Admin#getStoragePersona(String, String)
   */
  public Route getStoragePersona(Admin admin) {
    return new VeniceRouteHandler<StoragePersonaResponse>(StoragePersonaResponse.class) {
      @Override
      public void internalHandle(Request request, StoragePersonaResponse veniceResponse) {
        AdminSparkServer.validateParams(request, GET_STORAGE_PERSONA.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String personaName = request.queryParams(PERSONA_NAME);
        try {
          StoragePersona persona = admin.getStoragePersona(clusterName, personaName);
          veniceResponse.setStoragePersona(persona);
        } catch (Exception e) {
          veniceResponse.setError("Failed when getting persona " + personaName + ".", e);
        }
      }
    };
  }

  /**
   * @see Admin#deleteStoragePersona(String, String)
   */
  public Route deleteStoragePersona(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        AdminSparkServer.validateParams(request, DELETE_STORAGE_PERSONA.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String personaName = request.queryParams(PERSONA_NAME);
        try {
          admin.deleteStoragePersona(clusterName, personaName);
        } catch (Exception e) {
          veniceResponse.setError("Failed when deleting persona " + personaName + ".", e);
        }
      }
    };
  }

  /**
   * @see Admin#updateStoragePersona(String, String, UpdateStoragePersonaQueryParams)
   */
  public Route updateStoragePersona(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        AdminSparkServer.validateParams(request, UPDATE_STORAGE_PERSONA.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String personaName = request.queryParams(PERSONA_NAME);
        Map<String, String> params = Utils.extractQueryParamsFromRequest(request.queryMap().toMap(), veniceResponse);
        try {
          admin.updateStoragePersona(clusterName, personaName, new UpdateStoragePersonaQueryParams(params));
        } catch (Exception e) {
          veniceResponse.setError("Failed when updating persona " + personaName + ".", e);
        }
      }
    };
  }

  /**
   * @see Admin#getPersonaAssociatedWithStore(String, String)
   */
  public Route getPersonaAssociatedWithStore(Admin admin) {
    return new VeniceRouteHandler<StoragePersonaResponse>(StoragePersonaResponse.class) {
      @Override
      public void internalHandle(Request request, StoragePersonaResponse veniceResponse) {
        AdminSparkServer.validateParams(request, GET_STORAGE_PERSONA_ASSOCIATED_WITH_STORE.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        try {
          StoragePersona persona = admin.getPersonaAssociatedWithStore(clusterName, storeName);
          veniceResponse.setStoragePersona(persona);
        } catch (Exception e) {
          veniceResponse.setError(
              "Failed when getting persona for store " + storeName + ".  Exception type: " + e.getClass().toString()
                  + ".  Detailed message = " + e.getMessage(),
              e);
        }
      }
    };
  }

  /**
   * @see Admin#getClusterStoragePersonas(String)
   */
  public Route getClusterStoragePersonas(Admin admin) {
    return new VeniceRouteHandler<MultiStoragePersonaResponse>(MultiStoragePersonaResponse.class) {
      @Override
      public void internalHandle(Request request, MultiStoragePersonaResponse veniceResponse) {
        AdminSparkServer.validateParams(request, GET_CLUSTER_STORAGE_PERSONAS.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        try {
          List<StoragePersona> personaList = admin.getClusterStoragePersonas(clusterName);
          veniceResponse.setStoragePersonas(personaList);
        } catch (Exception e) {
          veniceResponse.setError(
              "Failed when getting all personas for cluster " + clusterName + ".  Exception type: "
                  + e.getClass().toString() + ".  Detailed message = " + e.getMessage(),
              e);
        }
      }
    };
  }

}
