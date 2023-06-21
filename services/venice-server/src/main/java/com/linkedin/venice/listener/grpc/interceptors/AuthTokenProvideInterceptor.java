package com.linkedin.venice.listener.grpc.interceptors;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.MethodDescriptor;


public class AuthTokenProvideInterceptor implements ClientInterceptor {
  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      final MethodDescriptor<ReqT, RespT> methodDescriptor,
      final CallOptions callOptions,
      final Channel channel) {
    return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
        channel.newCall(methodDescriptor, callOptions)) {
      String authToken;
      private Metadata headers;

      @Override
      public void sendMessage(ReqT message) {
        // commented out to avoid compilation error --> look into reserved keyword in protobuf to ensure backwards
        // compatibility
        // String key = ((VeniceClientRequest) message).get();
        String key = "valid";
        if (key.equals("invalid")) {
          authToken = "invalid";
        } else {
          authToken = "valid_token";
        }

        this.headers.put(Key.of("auth_token", Metadata.ASCII_STRING_MARSHALLER), authToken);
        super.sendMessage(message);
      }

      @Override
      public void start(final Listener<RespT> responseListener, final Metadata headers) {
        this.headers = headers;
        super.start(responseListener, headers);
      }
    };
  }
}
