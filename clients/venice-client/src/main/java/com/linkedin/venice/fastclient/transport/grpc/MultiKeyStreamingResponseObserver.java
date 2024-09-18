package com.linkedin.venice.fastclient.transport.grpc;

import com.linkedin.venice.client.store.transport.TransportClientResponse;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class MultiKeyStreamingResponseObserver implements StreamObserver<MultiKeyStreamingResponseObserver> {
  private static final Logger LOGGER = LogManager.getLogger(MultiKeyStreamingResponseObserver.class);

  private final CompletableFuture<TransportClientResponse> future;

  // used mainly for testing
  MultiKeyStreamingResponseObserver(CompletableFuture<TransportClientResponse> future) {
    this.future = future;
  }

  public MultiKeyStreamingResponseObserver() {
    this.future = new CompletableFuture<>();
  }

  public CompletableFuture<TransportClientResponse> getFuture() {
    return future;
  }

  @Override
  public void onNext(MultiKeyStreamingResponseObserver value) {

  }

  @Override
  public void onError(Throwable t) {

  }

  @Override
  public void onCompleted() {

  }
}
