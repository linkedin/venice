package com.linkedin.venice.router.acl;

import static com.linkedin.venice.router.api.VenicePathParser.TASK_READ_QUOTA_THROTTLE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.acl.AclException;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.acl.handler.AbstractStoreAclHandler;
import com.linkedin.venice.authorization.IdentityParser;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.router.api.RouterResourceType;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslHandler;
import java.net.SocketAddress;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mockito.ArgumentMatcher;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class RouterStoreAclHandlerTest {
  private static final Logger LOGGER = LogManager.getLogger(RouterStoreAclHandlerTest.class);
  private IdentityParser identityParser;
  private DynamicAccessController accessController;
  private HelixReadOnlyStoreRepository metadataRepo;
  private ChannelHandlerContext ctx;
  private HttpRequest req;
  private String storeName;
  private Store store;
  private String clusterName;
  private boolean[] hasStore = { false };
  private boolean[] isBadUri = { false };

  private void resetAllConditions() {
    hasStore[0] = false;
    isBadUri[0] = false;
  }

  @BeforeMethod
  public void setUp() throws Exception {
    clusterName = "testCluster";
    storeName = "testStore";
    identityParser = mock(IdentityParser.class);
    accessController = mock(DynamicAccessController.class);
    ctx = mock(ChannelHandlerContext.class);
    req = mock(HttpRequest.class);
    store = mock(Store.class);

    when(accessController.init(any())).thenReturn(accessController);

    // Certificate
    ChannelPipeline pipe = mock(ChannelPipeline.class);
    when(ctx.pipeline()).thenReturn(pipe);
    SslHandler sslHandler = mock(SslHandler.class);
    when(pipe.get(SslHandler.class)).thenReturn(sslHandler);
    SSLEngine sslEngine = mock(SSLEngine.class);
    when(sslHandler.engine()).thenReturn(sslEngine);
    SSLSession sslSession = mock(SSLSession.class);
    when(sslEngine.getSession()).thenReturn(sslSession);
    X509Certificate cert = mock(X509Certificate.class);
    when(sslSession.getPeerCertificates()).thenReturn(new Certificate[] { cert });

    // Host
    Channel channel = mock(Channel.class);
    when(ctx.channel()).thenReturn(channel);
    SocketAddress address = mock(SocketAddress.class);
    when(channel.remoteAddress()).thenReturn(address);

    when(req.method()).thenReturn(HttpMethod.GET);

    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.contains(HttpConstants.VENICE_ALLOW_REDIRECT)).thenReturn(true);

    when(req.headers()).thenReturn(headers);
  }

  @Test
  public void storeMissing() throws Exception {
    hasStore[0] = false;
    enumerate();

    verify(ctx, times(1)).fireChannelRead(req);
    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.FORBIDDEN)));
    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.UNAUTHORIZED)));
    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.BAD_REQUEST)));
  }

  @Test
  public void isBadUri() throws Exception {
    isBadUri[0] = true;
    enumerate(hasStore);

    // should fail every time for BAD_REQUEST
    verify(ctx, times(2)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.BAD_REQUEST)));
  }

  @Test
  public void testAllCases() throws Exception {
    enumerate(hasStore, isBadUri);

    // !isBadUri && hasStore = 1 times
    // !isBadUri && !hasStore = 1 times
    verify(ctx, times(2)).fireChannelRead(req);

    // isBadUri = 2 times
    verify(ctx, times(2)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.BAD_REQUEST)));

    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.FORBIDDEN)));
    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.UNAUTHORIZED)));
  }

  private void update() throws Exception {
    when(accessController.hasAccess(any(), any(), any())).thenReturn(true);
    when(accessController.hasAcl(any())).thenReturn(true);
    when(accessController.isFailOpen()).thenReturn(true);
    when(metadataRepo.hasStore(any())).thenReturn(hasStore[0]);
    if (hasStore[0]) {
      when(metadataRepo.getStore(any())).thenReturn(store);
    } else {
      when(metadataRepo.getStore(any())).thenReturn(null);
    }

    String storeNameInRequest = storeName;
    when(store.isSystemStore()).thenReturn(false);
    if (isBadUri[0]) {
      when(req.uri()).thenReturn("/badUri");
    } else {
      when(req.uri()).thenReturn(String.format("/storage/%s/random", storeNameInRequest));
    }
  }

  /**
   * Generate every possible combination for a given list of booleans based on variables passed
   * to boolean[]... conditions. If all variables (8 in count) are passed, then there will be 256
   * combinations:
   *
   * for (int i = 0; i < 256; i++) {        | i= 0   1   2   3   4 ...
   *   _hasAccess=          (i>>0) % 2 == 1|    F   T   F   T   F ...
   *   _hasAcl=             (i>>1) % 2 == 1|    F   F   T   T   F ...
   *   _hasStore=           (i>>2) % 2 == 1|    F   F   F   F   T ...
   *   _isAccessControlled= (i>>3) % 2 == 1|    F   F   F   F   F ...
   *   _isFailOpen=         (i>>4) % 2 == 1|    F   F   F   F   F ...
   *   _isMetadata=         (i>>5) % 2 == 1|    F   F   F   F   F ...
   *   _isHealthCheck=      (i>>6) % 2 == 1|    F   F   F   F   F ...
   *   _isBadUri=           (i>>7) % 2 == 1|    F   F   F   F   F ...
   * }
   */
  private void enumerate(boolean[]... conditions) throws Exception {
    // enumerate for all possible combinations
    int len = conditions.length;
    for (int i = 0; i < Math.pow(2, len); i++) {
      for (int j = 0; j < len; j++) {
        conditions[j][0] = ((i >> j) & 1) == 1;
      }
      // New metadataRepo mock and aclHandler every update since thenThrow cannot be re-mocked.
      metadataRepo = mock(HelixReadOnlyStoreRepository.class);
      AbstractStoreAclHandler aclHandler =
          spy(new RouterStoreAclHandler(identityParser, accessController, metadataRepo));
      update();
      LOGGER.info("hasStore: {}, isBadUri: {}", hasStore[0], isBadUri[0]);
      aclHandler.channelRead0(ctx, req);
    }

    // reset all supported conditions to the default to remove changes from this test
    resetAllConditions();
  }

  private static class ContextMatcher implements ArgumentMatcher<FullHttpResponse> {
    private HttpResponseStatus status;

    public ContextMatcher(HttpResponseStatus status) {
      this.status = status;
    }

    @Override
    public boolean matches(FullHttpResponse argument) {
      return argument.status().equals(status);
    }
  }

  /**
   * Mock access controller to verify basic request parsing and handling for {@link RouterStoreAclHandler}
   */
  private static class MockAccessController implements DynamicAccessController {
    private RouterResourceType resourceType;

    public MockAccessController(RouterResourceType resourceType) {
      this.resourceType = resourceType;
    }

    @Override
    public boolean hasAccessToTopic(X509Certificate clientCert, String resource, String method) throws AclException {
      assertNotNull(clientCert, resourceType.toString());
      validateStringArg(resource, "resource");
      validateStringArg(method, "method");
      return true;
    }

    @Override
    public boolean hasAccessToAdminOperation(X509Certificate clientCert, String operation) throws AclException {
      assertNotNull(clientCert, resourceType.toString());
      validateStringArg(operation, "operation");
      return true;
    }

    @Override
    public boolean isAllowlistUsers(X509Certificate clientCert, String resource, String method) {
      assertNotNull(clientCert, resourceType.toString());
      validateStringArg(resource, "resource");
      validateStringArg(method, "method");
      return true;
    }

    @Override
    public String getPrincipalId(X509Certificate clientCert) {
      assertNotNull(clientCert, resourceType.toString());
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
      assertNotNull(arg, argName + " should not be null for resource type " + resourceType.toString());
      assertFalse(arg.isEmpty(), argName + " should not be empty string for resource type " + resourceType.toString());
    }
  }

  @Test
  public void testAllRequestTypes() throws SSLPeerUnverifiedException, AclException {
    Store store = mock(Store.class);
    ReadOnlyStoreRepository metadataRepo = mock(ReadOnlyStoreRepository.class);
    when(metadataRepo.getStore(storeName)).thenReturn(store);
    HelixReadOnlyStoreConfigRepository storeConfigRepository = mock(HelixReadOnlyStoreConfigRepository.class);
    StoreConfig storeConfig = mock(StoreConfig.class);
    when(storeConfig.getCluster()).thenReturn(clusterName);
    when(storeConfigRepository.getStoreConfig(storeName)).thenReturn(Optional.of(storeConfig));
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    HttpRequest request = mock(HttpRequest.class);
    Channel channel = mock(Channel.class);
    SocketAddress socketAddress = mock(SocketAddress.class);
    doReturn("testRemoteHost").when(socketAddress).toString();
    doReturn(socketAddress).when(channel).remoteAddress();
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
    for (RouterResourceType resourceType: RouterResourceType.values()) {
      clearInvocations(ctx);
      MockAccessController mockAccessController = new MockAccessController(resourceType);
      MockAccessController spyMockAccessController = spy(mockAccessController);
      RouterStoreAclHandler storeAclHandler =
          new RouterStoreAclHandler(identityParser, spyMockAccessController, metadataRepo);
      doReturn(buildTestURI(resourceType)).when(request).uri();
      storeAclHandler.channelRead0(ctx, request);

      LOGGER.info("Testing {} resource type", resourceType);
      switch (resourceType) {
        case TYPE_LEADER_CONTROLLER:
        case TYPE_LEADER_CONTROLLER_LEGACY:
        case TYPE_KEY_SCHEMA:
        case TYPE_VALUE_SCHEMA:
        case TYPE_LATEST_VALUE_SCHEMA:
        case TYPE_GET_UPDATE_SCHEMA:
        case TYPE_ALL_VALUE_SCHEMA_IDS:
        case TYPE_CLUSTER_DISCOVERY:
        case TYPE_STREAM_HYBRID_STORE_QUOTA:
        case TYPE_STREAM_REPROCESSING_HYBRID_STORE_QUOTA:
        case TYPE_STORE_STATE:
        case TYPE_PUSH_STATUS:
        case TYPE_ADMIN:
        case TYPE_RESOURCE_STATE:
        case TYPE_CURRENT_VERSION:
        case TYPE_BLOB_DISCOVERY:
        case TYPE_REQUEST_TOPIC:
          verify(spyMockAccessController, never()).hasAccess(any(), any(), any());
          break;
        case TYPE_STORAGE:
        case TYPE_COMPUTE:
          verify(spyMockAccessController).hasAccess(any(), eq(storeName), any());
          break;
        case TYPE_INVALID:
          verify(spyMockAccessController, never()).hasAccess(any(), any(), any());
          verify(ctx, times(1)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.BAD_REQUEST)));
          break;
        default:
          throw new IllegalArgumentException("Invalid resource type: " + resourceType);
      }
    }
  }

  private String buildTestURI(RouterResourceType resourceType) {
    switch (resourceType) {
      case TYPE_LEADER_CONTROLLER:
      case TYPE_LEADER_CONTROLLER_LEGACY:
      case TYPE_RESOURCE_STATE:
        return "/" + resourceType.toString().toLowerCase();
      case TYPE_KEY_SCHEMA:
      case TYPE_VALUE_SCHEMA:
      case TYPE_LATEST_VALUE_SCHEMA:
      case TYPE_GET_UPDATE_SCHEMA:
      case TYPE_ALL_VALUE_SCHEMA_IDS:
      case TYPE_STORE_STATE:
      case TYPE_CLUSTER_DISCOVERY:
      case TYPE_STREAM_HYBRID_STORE_QUOTA:
      case TYPE_CURRENT_VERSION:
      case TYPE_REQUEST_TOPIC:
        return "/" + resourceType.toString().toLowerCase() + "/" + storeName;
      case TYPE_STREAM_REPROCESSING_HYBRID_STORE_QUOTA:
      case TYPE_PUSH_STATUS:
        String topicName = Version.composeKafkaTopic(storeName, 1);
        return "/" + resourceType.toString().toLowerCase() + "/" + topicName;
      case TYPE_ADMIN:
        return "/" + resourceType.toString().toLowerCase() + "/" + TASK_READ_QUOTA_THROTTLE;
      case TYPE_STORAGE:
      case TYPE_COMPUTE:
        return "/" + resourceType.toString().toLowerCase() + "/" + storeName + "/ABCDEFG";
      case TYPE_BLOB_DISCOVERY:
        return "/" + resourceType.toString().toLowerCase() + "?store=" + storeName
            + "&store_version=1&store_partition=2";
      case TYPE_INVALID:
        return "/invalid";
      default:
        throw new IllegalArgumentException("Invalid resource type: " + resourceType);
    }
  }
}
