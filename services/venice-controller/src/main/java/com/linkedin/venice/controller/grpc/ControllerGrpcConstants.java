package com.linkedin.venice.controller.grpc;

import com.linkedin.venice.controller.grpc.server.GrpcControllerClientDetails;
import io.grpc.Context;
import io.grpc.Metadata;


final public class ControllerGrpcConstants {
  public static final Metadata.Key<String> CLUSTER_NAME_METADATA_KEY =
      Metadata.Key.of("cluster-name", Metadata.ASCII_STRING_MARSHALLER);
  public static final Metadata.Key<String> STORE_NAME_METADATA_KEY =
      Metadata.Key.of("store-name", Metadata.ASCII_STRING_MARSHALLER);
  public static final Context.Key<GrpcControllerClientDetails> GRPC_CONTROLLER_CLIENT_DETAILS =
      Context.key("controller-client-details");
  public static final String UNKNOWN_REMOTE_ADDRESS = "unknown";

  private ControllerGrpcConstants() {
  }
}
