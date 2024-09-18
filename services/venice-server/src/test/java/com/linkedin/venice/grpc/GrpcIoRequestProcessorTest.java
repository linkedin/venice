package com.linkedin.venice.grpc;

import static com.linkedin.venice.listener.QuotaEnforcementHandler.QuotaEnforcementResult.REJECTED;
import static com.linkedin.venice.listener.ReadQuotaEnforcementHandler.INVALID_REQUEST_RESOURCE_MSG;
import static com.linkedin.venice.listener.ReadQuotaEnforcementHandler.SERVER_OVER_CAPACITY_MSG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.listener.QuotaEnforcementHandler;
import com.linkedin.venice.listener.QuotaEnforcementHandler.QuotaEnforcementResult;
import com.linkedin.venice.listener.StorageReadRequestHandler;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class GrpcIoRequestProcessorTest {
  private QuotaEnforcementHandler quotaEnforcementHandler;
  private StorageReadRequestHandler storageReadRequestHandler;
  private GrpcReplyProcessor grpcReplyProcessor;
  private GrpcServiceDependencies grpcServiceDependencies;
  private GrpcIoRequestProcessor processor;
  private GrpcRequestContext requestContext;
  private RouterRequest request;

  @BeforeMethod
  public void setUp() {
    quotaEnforcementHandler = mock(QuotaEnforcementHandler.class);
    storageReadRequestHandler = mock(StorageReadRequestHandler.class);
    grpcReplyProcessor = mock(GrpcReplyProcessor.class);
    grpcServiceDependencies = mock(GrpcServiceDependencies.class);
    when(grpcServiceDependencies.getQuotaEnforcementHandler()).thenReturn(quotaEnforcementHandler);
    when(grpcServiceDependencies.getStorageReadRequestHandler()).thenReturn(storageReadRequestHandler);
    when(grpcServiceDependencies.getGrpcReplyProcessor()).thenReturn(grpcReplyProcessor);

    request = mock(RouterRequest.class);
    when(request.getResourceName()).thenReturn("testResource_v1");
    requestContext = mock(GrpcRequestContext.class);
    when(requestContext.getRouterRequest()).thenReturn(request);
    when(requestContext.getGrpcRequestType()).thenReturn(GrpcRequestContext.GrpcRequestType.SINGLE_GET);

    processor = new GrpcIoRequestProcessor(grpcServiceDependencies);
  }

  @Test
  public void testProcessRequestAllowed() {
    // Case when quota enforcement result is ALLOWED
    when(quotaEnforcementHandler.enforceQuota(request)).thenReturn(QuotaEnforcementResult.ALLOWED);

    processor.processRequest(requestContext);

    // Verify that the request is handed off to the storage read request handler
    verify(storageReadRequestHandler)
        .queueIoRequestForAsyncProcessing(eq(request), any(GrpcStorageResponseHandlerCallback.class));
    verify(requestContext, never()).setErrorMessage(anyString());
    verify(requestContext, never()).setReadResponseStatus(any());

    // should not call grpcReplyProcessor.sendResponse() because the request is handed off to the storage read request
    // handler which is mocked
    verify(grpcReplyProcessor, never()).sendResponse(requestContext);
  }

  @Test
  public void testProcessRequestQuotaEnforcementErrors() {
    // BAD_REQUEST case
    when(quotaEnforcementHandler.enforceQuota(request)).thenReturn(QuotaEnforcementResult.BAD_REQUEST);
    when(request.getResourceName()).thenReturn("testResource");
    processor.processRequest(requestContext);
    verify(requestContext).setErrorMessage(INVALID_REQUEST_RESOURCE_MSG + "testResource");
    verify(requestContext).setReadResponseStatus(VeniceReadResponseStatus.BAD_REQUEST);
    verify(grpcReplyProcessor).sendResponse(requestContext);

    // REJECTED case
    when(quotaEnforcementHandler.enforceQuota(request)).thenReturn(REJECTED);
    processor.processRequest(requestContext);
    verify(requestContext).setErrorMessage("Quota exceeded for resource: testResource");
    verify(requestContext).setReadResponseStatus(VeniceReadResponseStatus.TOO_MANY_REQUESTS);
    verify(grpcReplyProcessor, times(2)).sendResponse(requestContext);

    // OVER_CAPACITY case
    when(quotaEnforcementHandler.enforceQuota(request)).thenReturn(QuotaEnforcementResult.OVER_CAPACITY);
    processor.processRequest(requestContext);
    verify(requestContext).setErrorMessage(SERVER_OVER_CAPACITY_MSG);
    verify(requestContext).setReadResponseStatus(VeniceReadResponseStatus.SERVICE_UNAVAILABLE);
    verify(grpcReplyProcessor, times(3)).sendResponse(requestContext);
  }
}
