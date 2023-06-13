package com.linkedin.venice.listener.grpc.interceptors;

import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;


public class AuthorizationInterceptor implements ServerInterceptor {

  /*
  Interceptor Class has to implement Server/Client Interceptor
  We override interceptCall Method and add our functionality within it.
  We obtain details about the metadata that the request came in, and perform some logic on it.
  Lastly, we return serverCallHandler.startCall(serverCall, metadata) so the next interceptor, or next request can be processed.
   */
  @Override
  public <ReqT, RespT> Listener<ReqT> interceptCall(
      final ServerCall<ReqT, RespT> serverCall,
      final Metadata metadata,
      final ServerCallHandler<ReqT, RespT> serverCallHandler) {
    final String authToken = metadata.get(Key.of("auth_token", Metadata.ASCII_STRING_MARSHALLER));

    if (authToken == null || !authToken.equals("valid_token")) {
      throw new StatusRuntimeException(Status.FAILED_PRECONDITION);
    }

    return serverCallHandler.startCall(serverCall, metadata);
  }
}
