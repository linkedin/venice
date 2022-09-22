package com.linkedin.venice.listener;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

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

}
