package com.linkedin.venice.controller.converters;

import com.linkedin.venice.controller.requests.ControllerHttpRequest;
import com.linkedin.venice.controller.requests.ControllerRequest;
import com.linkedin.venice.controller.requests.CreateStoreRequest;
import java.util.HashMap;
import java.util.Map;


/**
 * A registry to hold all the converters for {@link ControllerRequest} to Http specific request.
 * New addition of converters need to be registered below under the
 * {@link #initialize()}. e.g.,
 * <pre>
 *   // Register sample request converter
 *   registerRequestConverter(NewApiRequest.class, ConverterUtil::convertNewApiToHttpRequest)
 * </pre>
 */
public class HttpConvertersRegistry {
  private final Map<Class<?>, RequestConverter<? extends ControllerRequest, ControllerHttpRequest>> requestRegistry =
      new HashMap<>();

  public HttpConvertersRegistry() {
    initialize();
  }

  public void initialize() {
    registerRequestConverter(CreateStoreRequest.class, ConverterUtil::convertCreateStoreToHttpRequest);
  }

  private <T extends ControllerRequest> void registerRequestConverter(
      Class<T> requestType,
      RequestConverter<T, ControllerHttpRequest> converter) {
    requestRegistry.computeIfAbsent(requestType, k -> converter);
  }

  @SuppressWarnings("unchecked")
  public <T extends ControllerRequest> RequestConverter<T, ControllerHttpRequest> getRequestConverter(
      Class<T> requestType) {
    return (RequestConverter<T, ControllerHttpRequest>) requestRegistry.get(requestType);
  }
}
