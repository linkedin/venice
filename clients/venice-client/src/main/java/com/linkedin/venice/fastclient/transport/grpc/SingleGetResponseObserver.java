package com.linkedin.venice.fastclient.transport.grpc;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.exceptions.VeniceClientRateExceededException;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.protocols.SingleGetResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class SingleGetResponseObserver implements StreamObserver<SingleGetResponse> {
  private static final Logger LOGGER = LogManager.getLogger(SingleGetResponseObserver.class);

  private final CompletableFuture<TransportClientResponse> future;

  // used mainly for testing
  SingleGetResponseObserver(CompletableFuture<TransportClientResponse> future) {
    this.future = future;
  }

  public SingleGetResponseObserver() {
    this.future = new CompletableFuture<>();
  }

  public CompletableFuture<TransportClientResponse> getFuture() {
    return future;
  }

  @Override
  public void onNext(SingleGetResponse response) {
    try {
      VeniceReadResponseStatus readResponseStatus = VeniceReadResponseStatus.fromCode(response.getStatusCode());
      switch (readResponseStatus) {
        case OK:
          CompressionStrategy compressionStrategy = CompressionStrategy.valueOf(response.getCompressionStrategy());
          completeWithResponse(
              new TransportClientResponse(
                  response.getSchemaId(),
                  compressionStrategy,
                  response.getValue().toByteArray()));
          break;
        case KEY_NOT_FOUND:
          completeWithResponse(null);
          break;
        case BAD_REQUEST:
          completeWithException(new VeniceClientHttpException(response.getErrorMessage(), response.getStatusCode()));
          break;
        case TOO_MANY_REQUESTS:
          completeWithException(new VeniceClientRateExceededException(response.getErrorMessage()));
          break;
        default:
          handleUnexpectedError(response);
          break;
      }
    } catch (IllegalArgumentException e) {
      handleUnknownStatusCode(response, e);
    }
  }

  private void completeWithException(Exception e) {
    if (future.isDone()) {
      LOGGER.error("Completing future with exception, but future is already done", e);
      return;
    }
    future.completeExceptionally(e);
  }

  private void completeWithResponse(TransportClientResponse response) {
    if (future.isDone()) {
      LOGGER.error("Completing future with response, but future is already done");
      return;
    }
    future.complete(response);
  }

  private void handleUnexpectedError(SingleGetResponse response) {
    completeWithException(
        new VeniceClientException(
            String.format(
                "An unexpected error occurred with status code: %d, message: %s",
                response.getStatusCode(),
                response.getErrorMessage())));
  }

  private void handleUnknownStatusCode(SingleGetResponse response, IllegalArgumentException e) {
    completeWithException(
        new VeniceClientException(
            String.format("Unknown status code: %d, message: %s", response.getStatusCode(), response.getErrorMessage()),
            e));
  }

  @Override
  public void onError(Throwable t) {
    Status errorStatus = Status.fromThrowable(t);
    int statusCode = errorStatus.getCode().value();
    String errorDescription = errorStatus.getDescription() == null ? "" : errorStatus.getDescription();

    Exception exception = createExceptionForErrorStatus(errorStatus, statusCode, errorDescription, t);

    LOGGER.error("GRPC error occurred with status code: {}, message: {}", statusCode, errorDescription);
    completeWithException(exception);
  }

  private Exception createExceptionForErrorStatus(
      Status errorStatus,
      int statusCode,
      String errorDescription,
      Throwable t) {
    switch (errorStatus.getCode()) {
      case PERMISSION_DENIED:
      case UNAUTHENTICATED:
      case INVALID_ARGUMENT:
        return new VeniceClientHttpException(errorDescription, statusCode);
      default:
        return new VeniceClientException(
            String.format(
                "An unexpected gRPC error occurred with status code: %d, message: %s",
                statusCode,
                errorDescription),
            t);
    }
  }

  @Override
  public void onCompleted() {
    LOGGER.debug("GRPC call completed");
  }
}
