package com.linkedin.venice.controller.converters;

import com.linkedin.venice.controller.requests.ControllerRequest;
import com.linkedin.venice.controller.requests.CreateStoreRequest;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.protocols.CreateStoreGrpcResponse;
import java.util.HashMap;
import java.util.Map;


/**
 * A registry to hold all the converters for {@link ControllerRequest} and {@link ControllerResponse} to and back
 * to gRPC specific request and responses. New addition of converters need to be registered below under the
 * {@link #initialize()}. e.g.,
 * <pre>
 *   // Register sample request converter
 *   registerRequestConverter(NewApiRequest.class, ConverterUtil::convertNewApiToGrpcRequest)
 *
 *   // Register sample response converter
 *   registerResponseConverter(NewApiResponse.class, ConverterUtil::converterNewApiGrpcResponse)
 * </pre>
 */
public class GrpcConvertersRegistry {
  private final Map<Class<?>, RequestConverter<? extends ControllerRequest, ?>> requestRegistry = new HashMap<>();
  private final Map<Class<?>, ResponseConverter<?, ? extends ControllerResponse>> responseRegistry = new HashMap<>();

  public GrpcConvertersRegistry() {
    initialize();
  }

  public void initialize() {
    registerRequestConverter(CreateStoreRequest.class, ConverterUtil::convertCreateStoreToGrpcRequest);
    registerResponseConverter(CreateStoreGrpcResponse.class, ConverterUtil::convertCreateStoreGrpcResponse);
  }

  private <T extends ControllerRequest, R> void registerRequestConverter(
      Class<T> requestType,
      RequestConverter<T, R> converter) {
    requestRegistry.computeIfAbsent(requestType, k -> converter);
  }

  @SuppressWarnings("unchecked")
  public <T extends ControllerRequest, R> RequestConverter<T, R> getRequestConverter(Class<T> requestType) {
    return (RequestConverter<T, R>) requestRegistry.get(requestType);
  }

  private <T, R extends ControllerResponse> void registerResponseConverter(
      Class<T> requestType,
      ResponseConverter<T, R> converter) {
    responseRegistry.computeIfAbsent(requestType, k -> converter);
  }

  @SuppressWarnings("unchecked")
  public <T, R extends ControllerResponse> ResponseConverter<T, R> getResponseConverter(Class<T> requestType) {
    return (ResponseConverter<T, R>) responseRegistry.get(requestType);
  }
}
