package com.linkedin.venice.listener;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import org.testng.annotations.Test;


public class ServerStoreAclHandlerTest {
  @Test
  public void testCheckWhetherAccessHasAlreadyApproved() {
    Channel channel = mock(Channel.class);
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    doReturn(channel).when(ctx).channel();
    Attribute<Boolean> accessAttr = mock(Attribute.class);
    doReturn(true).when(accessAttr).get();
    doReturn(accessAttr).when(channel).attr(ServerAclHandler.SERVER_ACL_APPROVED_ATTRIBUTE_KEY);

    assertTrue(
        ServerStoreAclHandler.checkWhetherAccessHasAlreadyApproved(ctx),
        "Should return true if it is already approved by previous acl handler");

    doReturn(false).when(accessAttr).get();
    assertFalse(
        ServerStoreAclHandler.checkWhetherAccessHasAlreadyApproved(ctx),
        "Should return false if it is already denied by previous acl handler");
    doReturn(null).when(accessAttr).get();
    assertFalse(
        ServerStoreAclHandler.checkWhetherAccessHasAlreadyApproved(ctx),
        "Should return false if it hasn't been processed by acl handler");
  }

  @Test
  public void testCheckWhetherAccessHasAlreadyApprovedGrpc() {
    Metadata headers = new Metadata();
    headers.put(
        Metadata.Key.of(ServerAclHandler.GRPC_SERVER_ACL_APPROVED_ATTRIBUTE_KEY, Metadata.ASCII_STRING_MARSHALLER),
        "true");

    assertTrue(
        ServerStoreAclHandler.checkWhetherAccessHasAlreadyApproved(headers),
        "Should return true if it is already approved by previous acl handler");
  }

  @Test
  public void testInterceptor() {
    ServerCall call = mock(ServerCall.class);
    ServerCallHandler next = mock(ServerCallHandler.class);

    Metadata falseHeaders = new Metadata();
    falseHeaders.put(
        Metadata.Key.of(ServerAclHandler.GRPC_SERVER_ACL_APPROVED_ATTRIBUTE_KEY, Metadata.ASCII_STRING_MARSHALLER),
        "false");

    ServerStoreAclHandler handler =
        new ServerStoreAclHandler(mock(DynamicAccessController.class), mock(ReadOnlyStoreRepository.class));

    // next.intercept call should have been invoked
    handler.interceptCall(call, falseHeaders, next);
    verify(next, times(1)).startCall(call, falseHeaders);

    Metadata trueHeaders = new Metadata();
    trueHeaders.put(
        Metadata.Key.of(ServerAclHandler.GRPC_SERVER_ACL_APPROVED_ATTRIBUTE_KEY, Metadata.ASCII_STRING_MARSHALLER),
        "true");

    // next.intercept call should not have been invoked
    handler.interceptCall(call, trueHeaders, next);
    verify(next, times(1)).startCall(call, trueHeaders);
  }
}
