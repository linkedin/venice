package com.linkedin.venice.controller.server;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;

import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.ReadyForDataRecoveryResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.VeniceJsonSerializer;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.util.Optional;
import org.apache.commons.lang.StringUtils;
import spark.Request;
import spark.Route;


public class DataRecoveryRoutes extends AbstractRoute {
  private final VeniceJsonSerializer<Version> versionVeniceJsonSerializer = new VeniceJsonSerializer<>(Version.class);

  public DataRecoveryRoutes(boolean sslEnabled, Optional<DynamicAccessController> accessController) {
    super(sslEnabled, accessController);
  }

  public Route dataRecovery(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        AdminSparkServer.validateParams(request, DATA_RECOVERY.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        int version = Utils.parseIntFromString(request.queryParams(VERSION), VERSION);
        String sourceFabric = request.queryParams(SOURCE_FABRIC);
        String destinationFabric = request.queryParams(FABRIC);
        boolean copyAllVersionConfigs = Utils.parseBooleanFromString(
            request.queryParams(DATA_RECOVERY_COPY_ALL_VERSION_CONFIGS),
            DATA_RECOVERY_COPY_ALL_VERSION_CONFIGS);
        boolean sourceVersionIncluded = Utils.parseBooleanFromString(
            request.queryParams(SOURCE_FABRIC_VERSION_INCLUDED),
            SOURCE_FABRIC_VERSION_INCLUDED);
        Optional<Version> sourceVersion;
        if (sourceVersionIncluded) {
          Version sourceVersionObject = null;
          try {
            sourceVersionObject = versionVeniceJsonSerializer.deserialize(request.bodyAsBytes(), "");
          } catch (IOException e) {
            throw new VeniceException("Failed to deserialize source Version object", e);
          }
          sourceVersion = Optional.of(sourceVersionObject);
        } else {
          sourceVersion = Optional.empty();
        }
        admin.initiateDataRecovery(
            clusterName,
            storeName,
            version,
            sourceFabric,
            destinationFabric,
            copyAllVersionConfigs,
            sourceVersion);
      }
    };
  }

  public Route prepareDataRecovery(Admin admin) {
    return new VeniceRouteHandler<ControllerResponse>(ControllerResponse.class) {
      @Override
      public void internalHandle(Request request, ControllerResponse veniceResponse) {
        AdminSparkServer.validateParams(request, PREPARE_DATA_RECOVERY.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        int version = Utils.parseIntFromString(request.queryParams(VERSION), VERSION);
        String sourceFabric = request.queryParams(SOURCE_FABRIC);
        String destinationFabric = request.queryParams(FABRIC);
        Optional<Integer> sourceAmplificationFactor;
        if (StringUtils.isEmpty(request.queryParams(AMPLIFICATION_FACTOR))) {
          sourceAmplificationFactor = Optional.empty();
        } else {
          sourceAmplificationFactor =
              Optional.of(Utils.parseIntFromString(request.queryParams(AMPLIFICATION_FACTOR), AMPLIFICATION_FACTOR));
        }
        admin.prepareDataRecovery(
            clusterName,
            storeName,
            version,
            sourceFabric,
            destinationFabric,
            sourceAmplificationFactor);
      }
    };
  }

  public Route isStoreVersionReadyForDataRecovery(Admin admin) {
    return new VeniceRouteHandler<ReadyForDataRecoveryResponse>(ReadyForDataRecoveryResponse.class) {
      @Override
      public void internalHandle(Request request, ReadyForDataRecoveryResponse veniceResponse) {
        AdminSparkServer.validateParams(request, IS_STORE_VERSION_READY_FOR_DATA_RECOVERY.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        int version = Utils.parseIntFromString(request.queryParams(VERSION), VERSION);
        String sourceFabric = request.queryParams(SOURCE_FABRIC);
        String destinationFabric = request.queryParams(FABRIC);
        Optional<Integer> sourceAmplificationFactor;
        if (StringUtils.isEmpty(request.queryParams(AMPLIFICATION_FACTOR))) {
          sourceAmplificationFactor = Optional.empty();
        } else {
          sourceAmplificationFactor =
              Optional.of(Utils.parseIntFromString(request.queryParams(AMPLIFICATION_FACTOR), AMPLIFICATION_FACTOR));
        }
        Pair<Boolean, String> checkResult = admin.isStoreVersionReadyForDataRecovery(
            clusterName,
            storeName,
            version,
            sourceFabric,
            destinationFabric,
            sourceAmplificationFactor);
        veniceResponse.setReady(checkResult.getFirst());
        veniceResponse.setReason(checkResult.getSecond());
      }
    };
  }
}
