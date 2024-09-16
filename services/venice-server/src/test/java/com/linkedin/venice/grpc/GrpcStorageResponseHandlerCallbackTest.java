package com.linkedin.venice.grpc;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class GrpcStorageResponseHandlerCallbackTest {
  private GrpcRequestContext requestContext;
  private GrpcReplyProcessor grpcReplyProcessor;
  private GrpcStorageResponseHandlerCallback callback;

  @BeforeMethod
  public void setUp() {
    requestContext = mock(GrpcRequestContext.class);
    grpcReplyProcessor = mock(GrpcReplyProcessor.class);
    callback = GrpcStorageResponseHandlerCallback.create(requestContext, grpcReplyProcessor);
  }

  @Test
  public void testOnReadResponseVariousScenarios() {
    // Case 1: Response found
    ReadResponse foundResponse = mock(ReadResponse.class);
    when(foundResponse.isFound()).thenReturn(true);
    ByteBuf foundResponseBody = Unpooled.EMPTY_BUFFER;
    when(foundResponse.getResponseBody()).thenReturn(foundResponseBody);

    callback.onReadResponse(foundResponse);

    verify(requestContext).setReadResponseStatus(VeniceReadResponseStatus.OK);
    verify(requestContext).setReadResponse(foundResponse);
    verify(grpcReplyProcessor).sendResponse(requestContext);

    reset(requestContext, grpcReplyProcessor); // Resetting mocks before next scenario

    // Case 2: Response not found (key not found)
    ReadResponse notFoundResponse = mock(ReadResponse.class);
    when(notFoundResponse.isFound()).thenReturn(false);

    callback.onReadResponse(notFoundResponse);

    verify(requestContext).setReadResponseStatus(VeniceReadResponseStatus.KEY_NOT_FOUND);
    verify(requestContext).setReadResponse(notFoundResponse);
    verify(grpcReplyProcessor).sendResponse(requestContext);

    reset(requestContext, grpcReplyProcessor); // Resetting mocks before next scenario

    // Case 3: Null ReadResponse
    callback.onReadResponse(null);

    verify(requestContext).setReadResponseStatus(VeniceReadResponseStatus.INTERNAL_SERVER_ERROR);
    verify(requestContext).setReadResponse(null);
    verify(grpcReplyProcessor).sendResponse(requestContext);
  }

  @Test
  public void testOnErrorVariousScenarios() {
    // Case 1: Standard error case
    VeniceReadResponseStatus errorStatus = VeniceReadResponseStatus.INTERNAL_SERVER_ERROR;
    String errorMessage = "An error occurred";

    callback.onError(errorStatus, errorMessage);

    verify(requestContext).setReadResponseStatus(errorStatus);
    verify(requestContext).setErrorMessage(errorMessage);
    verify(requestContext).setReadResponse(null);
    verify(grpcReplyProcessor).sendResponse(requestContext);

    reset(requestContext, grpcReplyProcessor);

    // Case 2: Null error message
    callback.onError(errorStatus, null);
    verify(requestContext).setReadResponseStatus(errorStatus);
    verify(requestContext).setErrorMessage(null);
    verify(requestContext).setReadResponse(null);
    verify(grpcReplyProcessor).sendResponse(requestContext);
  }
}
