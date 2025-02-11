package com.linkedin.venice.listener;

import static com.linkedin.venice.acl.handler.AbstractStoreAclHandler.STORE_ACL_CHECK_RESULT_ATTRIBUTE_KEY;
import static io.grpc.Status.Code.INVALID_ARGUMENT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.acl.AclException;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.authorization.IdentityParser;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ServerAdminAction;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.grpc.Attributes;
import io.grpc.Grpc;
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
import java.util.function.Consumer;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.Test;


public class ServerStoreAclHandlerTest {
  private static final Logger LOGGER = LogManager.getLogger(ServerStoreAclHandlerTest.class);
  // Store name can be in a version topic format
  private static final String TEST_STORE_NAME = "testStore_v1";
  private static final String TEST_STORE_VERSION = Version.composeKafkaTopic(TEST_STORE_NAME, 1);
  private static final int TEST_STORE_PARTITION = 1;

  /**
   * Mock access controller to verify basic request parsing and handling for {@link ServerStoreAclHandler}
   */
  private static class MockAccessController implements DynamicAccessController {
    private final QueryAction queryAction;

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
    ChannelPipeline mockPipeline = mock(ChannelPipeline.class);
    doReturn(mock(SslHandler.class)).when(mockPipeline).get(SslHandler.class);
    doReturn(mockPipeline).when(ctx).pipeline();
    Attribute<Boolean> accessAttr = mock(Attribute.class);
    doReturn(true).when(accessAttr).get();
    doReturn(accessAttr).when(channel).attr(ServerAclHandler.SERVER_ACL_APPROVED_ATTRIBUTE_KEY);

    ServerStoreAclHandler handler = new ServerStoreAclHandler(
        mock(IdentityParser.class),
        mock(DynamicAccessController.class),
        mock(ReadOnlyStoreRepository.class),
        1000);

    assertTrue(
        handler.isAccessAlreadyApproved(channel),
        "Should return true if it is already approved by previous acl handler");

    doReturn(false).when(accessAttr).get();
    assertFalse(
        handler.isAccessAlreadyApproved(channel),
        "Should return false if it is already denied by previous acl handler");
    doReturn(null).when(accessAttr).get();
    assertFalse(
        handler.isAccessAlreadyApproved(channel),
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

    ServerStoreAclHandler handler = new ServerStoreAclHandler(
        mock(IdentityParser.class),
        mock(DynamicAccessController.class),
        mock(ReadOnlyStoreRepository.class),
        1000);

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
  public void testAclCache() throws SSLPeerUnverifiedException, AclException, InterruptedException {
    ReadOnlyStoreRepository metadataRepo = mock(ReadOnlyStoreRepository.class);
    when(metadataRepo.hasStore(TEST_STORE_NAME)).thenReturn(true);
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    HttpRequest request = mock(HttpRequest.class);
    Channel channel = mock(Channel.class);
    SocketAddress socketAddress = mock(SocketAddress.class);
    doReturn("testRemoteHost").when(socketAddress).toString();
    doReturn(socketAddress).when(channel).remoteAddress();

    Attribute<Boolean> accessAttr = mock(Attribute.class);
    doReturn(false).when(accessAttr).get();
    doReturn(accessAttr).when(channel).attr(ServerAclHandler.SERVER_ACL_APPROVED_ATTRIBUTE_KEY);

    Attribute<VeniceConcurrentHashMap> aclCacheAttr = mock(Attribute.class);
    doReturn(new VeniceConcurrentHashMap<>()).when(aclCacheAttr).get();
    doReturn(aclCacheAttr).when(channel).attr(STORE_ACL_CHECK_RESULT_ATTRIBUTE_KEY);

    doReturn(channel).when(ctx).channel();
    SslHandler sslHandler = mock(SslHandler.class);
    ChannelPipeline channelPipeline = mock(ChannelPipeline.class);
    doReturn(sslHandler).when(channelPipeline).get(SslHandler.class);
    SSLEngine sslEngine = mock(SSLEngine.class);
    SSLSession sslSession = mock(SSLSession.class);
    X509Certificate certificate = mock(X509Certificate.class);
    Certificate[] certificates = new Certificate[1];
    certificates[0] = certificate;
    doReturn(certificates).when(sslSession).getPeerCertificates();
    doReturn(sslSession).when(sslEngine).getSession();
    doReturn(sslEngine).when(sslHandler).engine();
    doReturn(channelPipeline).when(ctx).pipeline();
    doReturn(HttpMethod.GET).when(request).method();
    IdentityParser identityParser = mock(IdentityParser.class);
    doReturn("testPrincipalId").when(identityParser).parseIdentityFromCert(certificate);
    MockAccessController mockAccessController = new MockAccessController(QueryAction.STORAGE);
    MockAccessController spyMockAccessController = spy(mockAccessController);
    doReturn(buildTestURI(QueryAction.STORAGE)).when(request).uri();
    Time mockTime = new TestMockTime(0);
    ServerStoreAclHandler storeAclHandler =
        new ServerStoreAclHandler(identityParser, spyMockAccessController, metadataRepo, 1000, mockTime);
    storeAclHandler.channelRead0(ctx, request);
    verify(spyMockAccessController).hasAccess(any(), eq(TEST_STORE_NAME), any());
    // Verify that acl cache is populated by sending another request
    storeAclHandler.channelRead0(ctx, request);
    verify(spyMockAccessController, times(1)).hasAccess(any(), eq(TEST_STORE_NAME), any());
    // Make sure the cache is expired
    mockTime.sleep(2000);
    storeAclHandler.channelRead0(ctx, request);
    verify(spyMockAccessController, times(2)).hasAccess(any(), eq(TEST_STORE_NAME), any());
  }

  @Test
  public void testAllRequestTypes() throws SSLPeerUnverifiedException, AclException {
    ReadOnlyStoreRepository metadataRepo = mock(ReadOnlyStoreRepository.class);
    when(metadataRepo.hasStore(TEST_STORE_NAME)).thenReturn(true);
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    HttpRequest request = mock(HttpRequest.class);
    Channel channel = mock(Channel.class);
    SocketAddress socketAddress = mock(SocketAddress.class);
    doReturn("testRemoteHost").when(socketAddress).toString();
    doReturn(socketAddress).when(channel).remoteAddress();

    Attribute<Boolean> accessAttr = mock(Attribute.class);
    doReturn(false).when(accessAttr).get();
    doReturn(accessAttr).when(channel).attr(ServerAclHandler.SERVER_ACL_APPROVED_ATTRIBUTE_KEY);

    Attribute<VeniceConcurrentHashMap> aclCacheAttr = mock(Attribute.class);
    doAnswer((ignored) -> new VeniceConcurrentHashMap<>()).when(aclCacheAttr).get();
    doReturn(aclCacheAttr).when(channel).attr(STORE_ACL_CHECK_RESULT_ATTRIBUTE_KEY);

    doReturn(channel).when(ctx).channel();
    SslHandler sslHandler = mock(SslHandler.class);
    ChannelPipeline channelPipeline = mock(ChannelPipeline.class);
    doReturn(sslHandler).when(channelPipeline).get(SslHandler.class);
    SSLEngine sslEngine = mock(SSLEngine.class);
    SSLSession sslSession = mock(SSLSession.class);
    X509Certificate certificate = mock(X509Certificate.class);
    Certificate[] certificates = new Certificate[1];
    certificates[0] = certificate;
    doReturn(certificates).when(sslSession).getPeerCertificates();
    doReturn(sslSession).when(sslEngine).getSession();
    doReturn(sslEngine).when(sslHandler).engine();
    doReturn(channelPipeline).when(ctx).pipeline();
    doReturn(HttpMethod.GET).when(request).method();
    IdentityParser identityParser = mock(IdentityParser.class);
    doReturn("testPrincipalId").when(identityParser).parseIdentityFromCert(certificate);
    for (QueryAction queryAction: QueryAction.values()) {
      MockAccessController mockAccessController = new MockAccessController(queryAction);
      MockAccessController spyMockAccessController = spy(mockAccessController);
      ServerStoreAclHandler storeAclHandler =
          new ServerStoreAclHandler(identityParser, spyMockAccessController, metadataRepo, 1000);
      doReturn(buildTestURI(queryAction)).when(request).uri();
      storeAclHandler.channelRead0(ctx, request);

      LOGGER.info("Testing {} query action", queryAction);
      switch (queryAction) {
        case ADMIN:
        case CURRENT_VERSION:
        case HEALTH:
        case METADATA:
        case STORE_PROPERTIES:
        case TOPIC_PARTITION_INGESTION_CONTEXT:
        case HOST_HEARTBEAT_LAG:
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
      case STORE_PROPERTIES:
        return "/" + QueryAction.STORE_PROPERTIES.toString().toLowerCase() + "/" + TEST_STORE_NAME;
      case CURRENT_VERSION:
        return "/" + QueryAction.CURRENT_VERSION.toString().toLowerCase() + "/" + TEST_STORE_NAME;
      case TOPIC_PARTITION_INGESTION_CONTEXT:
        return "/" + QueryAction.TOPIC_PARTITION_INGESTION_CONTEXT.toString().toLowerCase() + "/" + TEST_STORE_VERSION
            + "/" + TEST_STORE_VERSION + "/1";
      case HOST_HEARTBEAT_LAG:
        return "/" + QueryAction.HOST_HEARTBEAT_LAG.toString().toLowerCase() + "/" + TEST_STORE_VERSION + "/"
            + TEST_STORE_PARTITION + "/false";
      default:
        throw new IllegalArgumentException("Invalid query action: " + queryAction);
    }
  }

  @Test
  public void testInvalidRequest() {
    ServerStoreAclHandler handler = new ServerStoreAclHandler(
        mock(IdentityParser.class),
        mock(DynamicAccessController.class),
        mock(ReadOnlyStoreRepository.class),
        1000);

    // Happy path is tested in "testAllRequestTypes". Only test the invalid paths

    // #parts == 2 but != HEALTH request
    assertNull(handler.validateRequest(new String[] { "", "invalid" }));

    // #parts == 1 (if request is made to "/")
    assertNull(handler.validateRequest(new String[] { "" }));

    // #parts == 1 (if request is made without "/". Not sure if this is possible too. But testing for completeness)
    assertNull(handler.validateRequest(new String[] { "invalid" }));

    // #parts >= 3, but invalid QueryAction
    assertNull(handler.validateRequest(new String[] { "", "invalid", "whatever" }));
  }

  @Test
  public void testValidateStoreAclForGRPC() throws SSLPeerUnverifiedException, AclException {
    Consumer<VeniceClientRequest> onAuthenticatedConsumer = spy(Consumer.class);
    ServerCall<VeniceClientRequest, Object> serverCall = spy(ServerCall.class);
    Metadata headers = new Metadata();

    IdentityParser identityParser = mock(IdentityParser.class);
    MockAccessController accessController = new MockAccessController(QueryAction.STORAGE);
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);

    SSLSession sslSession = mock(SSLSession.class);
    Attributes attributes = Attributes.newBuilder().set(Grpc.TRANSPORT_ATTR_SSL_SESSION, sslSession).build();
    doReturn(attributes).when(serverCall).getAttributes();

    X509Certificate certificate = mock(X509Certificate.class);
    Certificate[] certificates = new Certificate[] { certificate };
    doReturn(certificates).when(sslSession).getPeerCertificates();
    doReturn("identity").when(identityParser).parseIdentityFromCert(certificate);

    ServerStoreAclHandler handler =
        new ServerStoreAclHandler(identityParser, accessController, metadataRepository, 1000);

    // Empty store name
    VeniceClientRequest emptyStoreRequest = VeniceClientRequest.newBuilder().build();
    handler.validateStoreAclForGRPC(onAuthenticatedConsumer, emptyStoreRequest, serverCall, headers);
    verify(serverCall, times(1)).close(
        argThat((status) -> status.getCode() == INVALID_ARGUMENT && status.getDescription().equals("Invalid request")),
        eq(headers));
    clearInvocations(serverCall);

    // Empty method
    VeniceClientRequest emptyMethodRequest =
        VeniceClientRequest.newBuilder().setResourceName(TEST_STORE_VERSION).build();
    handler.validateStoreAclForGRPC(onAuthenticatedConsumer, emptyMethodRequest, serverCall, headers);
    verify(serverCall, times(1)).close(
        argThat((status) -> status.getCode() == INVALID_ARGUMENT && status.getDescription().equals("Invalid request")),
        eq(headers));
    clearInvocations(serverCall);

    // System store
    String systemStoreName = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(TEST_STORE_NAME);
    String storeVersion = Version.composeKafkaTopic(systemStoreName, 1);
    VeniceClientRequest systemStoreRequest =
        VeniceClientRequest.newBuilder().setResourceName(storeVersion).setMethod(HttpMethod.GET.name()).build();
    handler.validateStoreAclForGRPC(onAuthenticatedConsumer, systemStoreRequest, serverCall, headers);
    verify(onAuthenticatedConsumer, times(1)).accept(eq(systemStoreRequest));
    clearInvocations(onAuthenticatedConsumer);

    // Authenticated
    VeniceClientRequest authenticatedRequest =
        VeniceClientRequest.newBuilder().setResourceName(TEST_STORE_VERSION).setMethod(HttpMethod.GET.name()).build();
    handler.validateStoreAclForGRPC(onAuthenticatedConsumer, authenticatedRequest, serverCall, headers);
    verify(onAuthenticatedConsumer, times(1)).accept(eq(authenticatedRequest));
    clearInvocations(onAuthenticatedConsumer);
  }
}
