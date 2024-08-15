package com.linkedin.venice.listener;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.acl.AclException;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.acl.handler.StoreAclHandler;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ServerAdminAction;
import com.linkedin.venice.meta.Version;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.Attribute;
import java.net.SocketAddress;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Set;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import org.testng.annotations.Test;


public class ServerStoreAclHandlerTest {
  // Store name can be in a version topic format
  private static final String TEST_STORE_NAME = "testStore_v1";
  private static final String TEST_STORE_VERSION = Version.composeKafkaTopic(TEST_STORE_NAME, 1);

  /**
   * Mock access controller to verify basic request parsing and handling for {@link ServerStoreAclHandler}
   */
  private class MockAccessController implements DynamicAccessController {
    private QueryAction queryAction;

    public MockAccessController(QueryAction queryAction) {
      this.queryAction = queryAction;
    }

    @Override
    public boolean hasAccessToTopic(X509Certificate clientCert, String resource, String method) throws AclException {
      assertNotNull(clientCert, queryAction.toString());
      validateStringArg(resource, "resource");
      validateStringArg(method, "method");
      return true;
    }

    @Override
    public boolean hasAccessToAdminOperation(X509Certificate clientCert, String operation) throws AclException {
      assertNotNull(clientCert, queryAction.toString());
      validateStringArg(operation, "operation");
      return true;
    }

    @Override
    public boolean isAllowlistUsers(X509Certificate clientCert, String resource, String method) {
      assertNotNull(clientCert, queryAction.toString());
      validateStringArg(resource, "resource");
      validateStringArg(method, "method");
      return true;
    }

    @Override
    public String getPrincipalId(X509Certificate clientCert) {
      assertNotNull(clientCert, queryAction.toString());
      return "testPrincipalId";
    }

    @Override
    public DynamicAccessController init(List<String> resources) {
      return this;
    }

    @Override
    public boolean hasAccess(X509Certificate clientCert, String resource, String method) throws AclException {
      assertNotNull(clientCert);
      validateStringArg(resource, "resource");
      validateStringArg(method, "method");
      return true;
    }

    @Override
    public boolean hasAcl(String resource) throws AclException {
      validateStringArg(resource, "resource");
      return true;
    }

    @Override
    public void addAcl(String resource) throws AclException {
      validateStringArg(resource, "resource");
    }

    @Override
    public void removeAcl(String resource) throws AclException {
      validateStringArg(resource, "resource");
    }

    @Override
    public Set<String> getAccessControlledResources() {
      return null;
    }

    @Override
    public boolean isFailOpen() {
      return false;
    }

    private void validateStringArg(String arg, String argName) {
      assertNotNull(arg, argName + " should not be null for query action " + queryAction.toString());
      assertFalse(arg.isEmpty(), argName + " should not be empty string for query action " + queryAction.toString());
    }
  }

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
    headers.put(Metadata.Key.of(ServerAclHandler.SERVER_ACL_APPROVED, Metadata.ASCII_STRING_MARSHALLER), "true");

    assertTrue(
        ServerStoreAclHandler.checkWhetherAccessHasAlreadyApproved(headers),
        "Should return true if it is already approved by previous acl handler");
  }

  @Test
  public void testInterceptor() {
    ServerCall call = mock(ServerCall.class);
    ServerCallHandler next = mock(ServerCallHandler.class);

    Metadata falseHeaders = new Metadata();
    falseHeaders.put(Metadata.Key.of(ServerAclHandler.SERVER_ACL_APPROVED, Metadata.ASCII_STRING_MARSHALLER), "false");

    ServerStoreAclHandler handler =
        new ServerStoreAclHandler(mock(DynamicAccessController.class), mock(ReadOnlyStoreRepository.class));

    // next.intercept call should have been invoked
    handler.interceptCall(call, falseHeaders, next);
    verify(next, times(1)).startCall(call, falseHeaders);

    Metadata trueHeaders = new Metadata();
    trueHeaders.put(Metadata.Key.of(ServerAclHandler.SERVER_ACL_APPROVED, Metadata.ASCII_STRING_MARSHALLER), "true");

    // next.intercept call should not have been invoked
    handler.interceptCall(call, trueHeaders, next);
    verify(next, times(1)).startCall(call, trueHeaders);
  }

  @Test
  public void testAllRequestTypes() throws SSLPeerUnverifiedException, AclException {
    ReadOnlyStoreRepository metadataRepo = mock(ReadOnlyStoreRepository.class);
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    HttpRequest request = mock(HttpRequest.class);
    Channel channel = mock(Channel.class);
    SocketAddress socketAddress = mock(SocketAddress.class);
    doReturn("testRemoteHost").when(socketAddress).toString();
    doReturn(socketAddress).when(channel).remoteAddress();
    Attribute<Boolean> accessAttr = mock(Attribute.class);
    doReturn(false).when(accessAttr).get();
    doReturn(accessAttr).when(channel).attr(ServerAclHandler.SERVER_ACL_APPROVED_ATTRIBUTE_KEY);
    doReturn(channel).when(ctx).channel();
    SslHandler sslHandler = mock(SslHandler.class);
    ChannelPipeline channelPipeline = mock(ChannelPipeline.class);
    doReturn(sslHandler).when(channelPipeline).get(SslHandler.class);
    SSLEngine sslEngine = mock(SSLEngine.class);
    SSLSession sslSession = mock(SSLSession.class);
    Certificate certificate = mock(X509Certificate.class);
    Certificate[] certificates = new Certificate[1];
    certificates[0] = certificate;
    doReturn(certificates).when(sslSession).getPeerCertificates();
    doReturn(sslSession).when(sslEngine).getSession();
    doReturn(sslEngine).when(sslHandler).engine();
    doReturn(channelPipeline).when(ctx).pipeline();
    doReturn(HttpMethod.GET).when(request).method();
    for (QueryAction queryAction: QueryAction.values()) {
      MockAccessController mockAccessController = new MockAccessController(queryAction);
      MockAccessController spyMockAccessController = spy(mockAccessController);
      StoreAclHandler storeAclHandler = new ServerStoreAclHandler(spyMockAccessController, metadataRepo);
      doReturn(buildTestURI(queryAction)).when(request).uri();
      storeAclHandler.channelRead0(ctx, request);
      switch (queryAction) {
        case ADMIN:
        case CURRENT_VERSION:
        case HEALTH:
        case METADATA:
        case TOPIC_PARTITION_INGESTION_CONTEXT:
          verify(spyMockAccessController, never()).hasAccess(any(), any(), any());
          break;
        case STORAGE:
        case COMPUTE:
        case DICTIONARY:
          verify(spyMockAccessController).hasAccess(any(), eq(TEST_STORE_NAME), any());
          break;
        default:
          throw new IllegalArgumentException("Invalid query action: " + queryAction);
      }
    }
  }

  private String buildTestURI(QueryAction queryAction) {
    switch (queryAction) {
      case STORAGE:
        return "/" + QueryAction.STORAGE.toString().toLowerCase() + "/" + TEST_STORE_VERSION + "/1/ABCDEFG";
      case HEALTH:
        return "/" + QueryAction.HEALTH.toString().toLowerCase();
      case COMPUTE:
        return "/" + QueryAction.COMPUTE.toString().toLowerCase() + "/" + TEST_STORE_VERSION;
      case DICTIONARY:
        return "/" + QueryAction.DICTIONARY.toString().toLowerCase() + "/" + TEST_STORE_NAME + "/1";
      case ADMIN:
        return "/" + QueryAction.ADMIN.toString().toLowerCase() + "/" + TEST_STORE_VERSION + "/"
            + ServerAdminAction.DUMP_INGESTION_STATE;
      case METADATA:
        return "/" + QueryAction.METADATA.toString().toLowerCase() + "/" + TEST_STORE_NAME;
      case CURRENT_VERSION:
        return "/" + QueryAction.CURRENT_VERSION.toString().toLowerCase() + "/" + TEST_STORE_NAME;
      case TOPIC_PARTITION_INGESTION_CONTEXT:
        return "/" + QueryAction.TOPIC_PARTITION_INGESTION_CONTEXT.toString().toLowerCase() + "/" + TEST_STORE_VERSION
            + "/" + TEST_STORE_VERSION + "/1";
      default:
        throw new IllegalArgumentException("Invalid query action: " + queryAction);
    }
  }
}
