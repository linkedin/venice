package com.linkedin.venice.controller.transport;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.controller.converters.GrpcConvertersRegistry;
import com.linkedin.venice.controller.converters.RequestConverter;
import com.linkedin.venice.controller.converters.ResponseConverter;
import com.linkedin.venice.controller.requests.ControllerRequest;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.grpc.GrpcUtils;
import com.linkedin.venice.protocols.VeniceControllerGrpcServiceGrpc;
import com.linkedin.venice.security.SSLFactory;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.TlsChannelCredentials;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * Transport client implementation to interact with gRPC server. The current implementation keeps the lifecycle
 * of the client at a request granularity as opposed to a longer client due to its usage pattern across the codebase.
 * Consider expanding the lifecycle to per component or per JVM as we sit with evolution.
 */
public class ControllerGrpcTransportClient implements AutoCloseable {
  private static final long SHUTDOWN_TIMEOUT = 10000L;
  private final ManagedChannel channel;

  private final GrpcConvertersRegistry converterRegistry;

  public ControllerGrpcTransportClient(String server) {
    this(server, InsecureChannelCredentials.create());
  }

  public ControllerGrpcTransportClient(String server, SSLFactory sslFactory) {
    this(server, buildChannelCredentials(sslFactory));
  }

  private ControllerGrpcTransportClient(String server, ChannelCredentials channelCredentials) {
    this.channel = Grpc.newChannelBuilder(server, channelCredentials).build();
    this.converterRegistry = new GrpcConvertersRegistry();
  }

  public <REQ extends ControllerRequest, RES extends ControllerResponse> RES request(REQ request)
      throws ExecutionException, TimeoutException {

    GrpcRoute grpcRoute = RouteUtils.getGrpcRouteFor(request.getRoute());

    try {
      VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceBlockingStub stub = buildStub();

      Method method = stub.getClass().getMethod(grpcRoute.getMethod(), grpcRoute.getRequestType());
      Object responseObject = method.invoke(stub, convertToGrpcRequest(request));

      return convertToControllerResponse(grpcRoute.getResponseType(), responseObject);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceBlockingStub buildStub() {
    return VeniceControllerGrpcServiceGrpc.newBlockingStub(channel);
  }

  @SuppressWarnings("unchecked")
  private <RES extends ControllerResponse, T> RES convertToControllerResponse(
      Class<T> responseType,
      Object responseObject) {
    ResponseConverter<T, RES> converter = converterRegistry.getResponseConverter(responseType);
    return converter.convert((T) responseObject);
  }

  @SuppressWarnings("unchecked")
  <REQ extends ControllerRequest> Object convertToGrpcRequest(REQ request) {
    RequestConverter<REQ, ?> converter =
        (RequestConverter<REQ, ?>) converterRegistry.getRequestConverter(request.getClass());
    return converter.convert(request);
  }

  @VisibleForTesting
  static ChannelCredentials buildChannelCredentials(SSLFactory factory) {
    try {
      TlsChannelCredentials.Builder tlsBuilder = TlsChannelCredentials.newBuilder()
          .keyManager(GrpcUtils.getKeyManagers(factory))
          .trustManager(GrpcUtils.getTrustManagers(factory));
      return tlsBuilder.build();
    } catch (Exception e) {
      throw new VeniceClientException(
          "Failed to initialize SSL channel credentials for Venice gRPC Transport Client",
          e);
    }
  }

  @Override
  public void close() throws Exception {
    channel.shutdown().awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS);
  }
}
