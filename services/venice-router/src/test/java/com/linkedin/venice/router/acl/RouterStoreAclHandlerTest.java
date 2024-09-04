package com.linkedin.venice.router.acl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.acl.handler.AbstractStoreAclHandler;
import com.linkedin.venice.authorization.IdentityParser;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.router.VeniceRouterConfig;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.net.ssl.SSLEngine;
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
  private String migratedClusterName;
  private VeniceRouterConfig routerConfig;
  private HelixReadOnlyStoreConfigRepository storeConfigRepository = mock(HelixReadOnlyStoreConfigRepository.class);
  private boolean[] hasStore = { false };
  private boolean[] storeMigrating = { false };
  private boolean[] storeMigrated = { false };
  private boolean[] isBadUri = { false };

  private void resetAllConditions() {
    hasStore[0] = false;
    storeMigrating[0] = false;
    storeMigrated[0] = false;
    isBadUri[0] = false;
  }

  @BeforeMethod
  public void setUp() throws Exception {
    clusterName = "testCluster";
    migratedClusterName = "migratedCluster";
    storeName = "testStore";
    identityParser = mock(IdentityParser.class);
    accessController = mock(DynamicAccessController.class);
    ctx = mock(ChannelHandlerContext.class);
    req = mock(HttpRequest.class);
    store = mock(Store.class);

    when(accessController.init(any())).thenReturn(accessController);

    routerConfig = mock(VeniceRouterConfig.class);
    when(routerConfig.getClusterName()).thenReturn(clusterName);

    Map<String, String> clusterToD2Map = new HashMap<>();
    clusterToD2Map.put(clusterName, "testClusterD2");
    clusterToD2Map.put(migratedClusterName, "migratedClusterD2");
    when(routerConfig.getClusterToD2Map()).thenReturn(clusterToD2Map);

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
    enumerate(storeMigrating, storeMigrated);

    verify(ctx, never()).fireChannelRead(req);
    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.FORBIDDEN)));
    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.UNAUTHORIZED)));

    // When !storeMigrated
    verify(ctx, times(2)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.BAD_REQUEST)));

    // When storeMigrated
    verify(ctx, times(2)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.MOVED_PERMANENTLY)));
  }

  @Test
  public void isBadUri() throws Exception {
    isBadUri[0] = true;
    enumerate(hasStore, storeMigrated, storeMigrating);

    // should fail every time for BAD_REQUEST
    verify(ctx, times(8)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.BAD_REQUEST)));
  }

  @Test
  public void testAllCases() throws Exception {
    enumerate(hasStore, storeMigrated, storeMigrating, isBadUri);

    // !isBadUri && hasStore && !storeMigrating = 2 times
    verify(ctx, times(2)).fireChannelRead(req);

    // !isBadUri && hasStore && storeMigrating = 2 times
    // !isBadUri && !hasStore && storeMigrated = 2 times
    verify(ctx, times(4)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.MOVED_PERMANENTLY)));

    // isBadUri = 8 times
    // !isBadUri && !hasStore && !storeMigrated = 2 times
    verify(ctx, times(10)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.BAD_REQUEST)));

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

    if (storeMigrating[0]) {
      when(store.isMigrating()).thenReturn(true);
    } else {
      when(store.isMigrating()).thenReturn(false);
    }

    if ((hasStore[0] && storeMigrating[0]) || storeMigrated[0]) {
      StoreConfig storeConfig = mock(StoreConfig.class);
      when(storeConfig.getCluster()).thenReturn(migratedClusterName);
      when(storeConfigRepository.getStoreConfig(storeName)).thenReturn(Optional.of(storeConfig));
    } else {
      when(storeConfigRepository.getStoreConfig(storeName)).thenReturn(Optional.empty());
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
      AbstractStoreAclHandler aclHandler = spy(
          new RouterStoreAclHandler(
              routerConfig,
              identityParser,
              accessController,
              metadataRepo,
              storeConfigRepository));
      update();
      LOGGER.info(
          "hasStore: {}, storeMigrating: {}, storeMigrated: {}, isBadUri: {}",
          hasStore[0],
          storeMigrating[0],
          storeMigrated[0],
          isBadUri[0]);
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
}
