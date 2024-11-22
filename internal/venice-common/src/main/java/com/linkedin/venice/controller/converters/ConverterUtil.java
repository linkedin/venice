package com.linkedin.venice.controller.converters;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;

import com.linkedin.venice.controller.requests.ControllerHttpRequest;
import com.linkedin.venice.controller.requests.CreateStoreRequest;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.QueryParams;
import com.linkedin.venice.protocols.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.CreateStoreGrpcResponse;
import java.util.Optional;


/**
 * A utility class to hold all the conversion logic from {@link com.linkedin.venice.controller.requests.ControllerRequest}
 * to transport specific request and transport specific responses back to {@link com.linkedin.venice.controllerapi.ControllerResponse}.
 * The utility methods are registered with {@link GrpcConvertersRegistry} and {@link HttpConvertersRegistry} for the
 * runtime to perform appropriate conversions. Refer to the documentation of the above converter registries for more
 * information.
 */
public final class ConverterUtil {

  // store related converters
  public static CreateStoreGrpcRequest convertCreateStoreToGrpcRequest(CreateStoreRequest request) {
    CreateStoreGrpcRequest.Builder builder = CreateStoreGrpcRequest.newBuilder()
        .setStoreName(request.getStoreName())
        .setKeySchema(request.getKeySchema())
        .setValueSchema(request.getValueSchema())
        .setIsSystemStore(request.isSystemStore());

    Optional.ofNullable(request.getOwner()).ifPresent(builder::setOwner);
    Optional.ofNullable(request.getAccessPermissions()).ifPresent(builder::setAccessPermission);

    return builder.build();
  }

  public static ControllerHttpRequest convertCreateStoreToHttpRequest(CreateStoreRequest request) {
    QueryParams params = new QueryParams();

    params.add(NAME, request.getStoreName())
        .add(OWNER, request.getOwner())
        .add(KEY_SCHEMA, request.getKeySchema())
        .add(VALUE_SCHEMA, request.getValueSchema())
        .add(IS_SYSTEM_STORE, request.isSystemStore())
        .add(CLUSTER, request.getClusterName());

    Optional.ofNullable(request.getAccessPermissions()).ifPresent(perm -> params.add(ACCESS_PERMISSION, perm));

    return ControllerHttpRequest.newBuilder().setParam(params).build();
  }

  public static NewStoreResponse convertCreateStoreGrpcResponse(CreateStoreGrpcResponse grpcResponse) {
    NewStoreResponse response = new NewStoreResponse();
    response.setOwner(grpcResponse.getOwner());
    return response;
  }
}
