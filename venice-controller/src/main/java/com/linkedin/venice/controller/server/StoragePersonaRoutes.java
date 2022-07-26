package com.linkedin.venice.controller.server;

import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoragePersonaResponse;
import com.linkedin.venice.controllerapi.UpdateStoragePersonaQueryParams;
import com.linkedin.venice.persona.StoragePersona;
import com.linkedin.venice.utils.Utils;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import spark.Request;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;


public class StoragePersonaRoutes extends AbstractRoute {

  public StoragePersonaRoutes(Optional<DynamicAccessController> accessController) {
    super(accessController);
  }

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
          veniceResponse.setError("Failed when creating persona " + name + ". Exception type: " + e.getClass().toString() + ". Detailed message = " + e.getMessage());
        }
      }
    };
  }

  public Route getStoragePersona(Admin admin) {
    return new VeniceRouteHandler<StoragePersonaResponse>(StoragePersonaResponse.class) {
      @Override
      public void internalHandle(Request request, StoragePersonaResponse veniceResponse) {
        AdminSparkServer.validateParams(request, GET_STORAGE_PERSONA.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String personaName = request.queryParams(NAME);
        try {
          StoragePersona persona = admin.getStoragePersona(clusterName, personaName);
          veniceResponse.setStoragePersona(persona);
        } catch (Exception e) {
          veniceResponse.setError("Failed when getting persona " + personaName + ". Exception type: " + e.getClass().toString() + ". Detailed message = " + e.getMessage());
        }
      }
    };
  }

  public Route deleteStoragePersona(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        AdminSparkServer.validateParams(request, DELETE_STORAGE_PERSONA.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String personaName = request.queryParams(NAME);
        try {
          admin.deleteStoragePersona(clusterName, personaName);
        } catch (Exception e) {
          veniceResponse.setError("Failed when deleting persona " + personaName + ".  Exception type: " + e.getClass().toString() + ".  Detailed message = " + e.getMessage());
          veniceResponse.setError(
              "Failed when deleting persona " + personaName + ".  Exception type: " + e.getClass().toString() + ".  Detailed message = " + e.getMessage());
        }
      }
    };
  }

  public Route updateStoragePersona(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        AdminSparkServer.validateParams(request, UPDATE_STORAGE_PERSONA.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String personaName = request.queryParams(NAME);
        Map<String, String> params = Utils.extractQueryParamsFromRequest(request.queryMap().toMap(), veniceResponse);
        try {
          admin.updateStoragePersona(clusterName, personaName, new UpdateStoragePersonaQueryParams(params));
        } catch (Exception e) {
          veniceResponse.setError("Failed when updating persona " + personaName + ".  Exception type: " + e.getClass().toString() + ".  Detailed message = " + e.getMessage());
        }
      }
    };
  }

}
