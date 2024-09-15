package com.linkedin.venice.acl.handler;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.authorization.IdentityParser;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.helix.HelixReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslHandler;
import java.net.SocketAddress;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import org.mockito.ArgumentMatcher;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class AbstractStoreAclHandlerTest {
  private IdentityParser identityParser;
  private DynamicAccessController accessController;
  private HelixReadOnlyStoreRepository metadataRepo;
  private ChannelHandlerContext ctx;
  private HttpRequest req;
  private Store store;
  private boolean[] needsAcl = { true };
  private boolean[] hasAccess = { false };
  private boolean[] hasAcl = { false };
  private boolean[] hasStore = { false };
  private boolean[] isSystemStore = { false };
  private boolean[] isFailOpen = { false };
  private boolean[] isBadUri = { false };

  private String storeName;

  private void resetAllConditions() {
    needsAcl[0] = true;
    hasAccess[0] = false;
    hasAcl[0] = false;
    hasStore[0] = false;
    isSystemStore[0] = false;
    isFailOpen[0] = false;
    isBadUri[0] = false;
  }

  @BeforeMethod
  public void setUp() throws Exception {
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
  }

  @Test
  public void noAclNeeded() throws Exception {
    needsAcl[0] = false;
    enumerate(hasAccess, hasAcl, hasStore, isSystemStore, isFailOpen);

    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.FORBIDDEN)));
    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.UNAUTHORIZED)));
    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.BAD_REQUEST)));

    // No access control needed. There are 32 possible combinations of the 5 variables
    verify(ctx, times(32)).fireChannelRead(req);
  }

  @Test
  public void accessGranted() throws Exception {
    hasAccess[0] = true;
    enumerate(needsAcl, hasAcl, hasStore, isSystemStore, isFailOpen);

    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.FORBIDDEN)));
    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.UNAUTHORIZED)));
    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.BAD_REQUEST)));

    // !needsAcl = 16 times
    // needsAcl && !hasStore = 8 times
    // needsAcl && hasStore && isSystemStore = 4 times
    // needsAcl && hasStore && !isSystemStore = 4 times
    verify(ctx, times(32)).fireChannelRead(req);
  }

  @Test
  public void accessDenied() throws Exception {
    hasAccess[0] = false;
    enumerate(needsAcl, hasAcl, hasStore, isSystemStore, isFailOpen);

    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.BAD_REQUEST)));

    // No access control needed 16 times
    // needsAcl && !hasStore = 8 times
    // needsAcl && hasStore && isSystemStore = 4 times
    verify(ctx, times(28)).fireChannelRead(req);

    // UNAUTHORIZED when needsAcl && hasStore && !hasAccess && !hasAcl && !isFailOpen && !isSystemStore
    verify(ctx, times(1)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.UNAUTHORIZED)));

    // FORBIDDEN when needsAcl && hasStore && !hasAccess && (hasAcl || isFailOpen) && !isSystemStore
    verify(ctx, times(3)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.FORBIDDEN)));
  }

  @Test
  public void storeExists() throws Exception {
    hasStore[0] = true;
    enumerate(needsAcl, hasAccess, hasAcl, isFailOpen, isSystemStore);

    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.BAD_REQUEST)));

    // !needsAcl = 16 times
    // needsAcl && hasStore && isSystemStore = 8 times
    // needsAcl && hasStore && !isSystemStore && hasAccess = 4 times
    verify(ctx, times(28)).fireChannelRead(req);

    // needsAcl && hasStore && !isSystemStore && !hasAccess && !hasAcl && !isFailOpen = 1 time
    verify(ctx, times(1)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.UNAUTHORIZED)));

    // needsAcl && hasStore && !isSystemStore && !hasAccess && (hasAcl || isFailOpen) = 3 times
    verify(ctx, times(3)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.FORBIDDEN)));
  }

  @Test
  public void storeMissing() throws Exception {
    hasStore[0] = false;
    enumerate(needsAcl, hasAccess, hasAcl, isFailOpen, isSystemStore);

    // !needsAcl = 16 times
    // needsAcl && !hasStore = 16 times
    verify(ctx, times(32)).fireChannelRead(req);

    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.BAD_REQUEST)));
  }

  @Test
  public void aclDisabledForSystemStore() throws Exception {
    isSystemStore[0] = true;
    hasStore[0] = true;
    enumerate(needsAcl, hasAccess, hasAcl, isFailOpen);

    verify(ctx, never()).writeAndFlush(any());
    // No access control (!needsAcl) => 8 times, needsAcl && hasStore && isSystemStore => 8 times
    verify(ctx, times(16)).fireChannelRead(req);
  }

  @Test
  public void isBadUri() throws Exception {
    isBadUri[0] = true;
    enumerate(hasAcl, hasStore, hasAccess, isSystemStore, isFailOpen);

    // all 32 times should fail for BAD_REQUEST
    verify(ctx, times(32)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.BAD_REQUEST)));
  }

  @Test
  public void aclMissing() throws Exception {
    hasAcl[0] = false;
    enumerate(needsAcl, hasStore, hasAccess, isSystemStore, isFailOpen);

    // !needsAcl = 16 times
    // needsAcl && !hasStore = 8 times
    // needsAcl && hasStore && isSystemStore = 4 times
    // needsAcl && hasStore && !isSystemStore && hasAccess = 2 times
    verify(ctx, times(30)).fireChannelRead(req);

    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.BAD_REQUEST)));

    // needsAcl && hasStore && !isSystemStore && !hasAccess && !hasAcl && !isFailOpen
    verify(ctx, times(1)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.UNAUTHORIZED)));

    // needsAcl && hasStore && !isSystemStore && !hasAccess && (hasAcl || isFailOpen)
    verify(ctx, times(1)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.FORBIDDEN)));
  }

  @Test
  public void aclPresent() throws Exception {
    hasAcl[0] = true;
    enumerate(needsAcl, hasStore, hasAccess, isSystemStore, isFailOpen);

    // !needsAcl = 16 times
    // needsAcl && !hasStore
    // needsAcl && hasStore && isSystemStore = 4 times
    // needsAcl && hasStore && !isSystemStore && hasAccess = 2 times
    verify(ctx, times(30)).fireChannelRead(req);

    // needsAcl && !hasStore
    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.BAD_REQUEST)));

    // Since hasAcl = true, UNAUTHORIZED is not possible
    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.UNAUTHORIZED)));

    // needsAcl && hasStore && !isSystemStore && !hasAccess && hasAcl
    verify(ctx, times(2)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.FORBIDDEN)));
  }

  @Test
  public void testAllCases() throws Exception {
    enumerate(needsAcl, hasAcl, hasStore, hasAccess, isSystemStore, isFailOpen, isBadUri);

    // !needsAcl = 64 times
    // needsAcl && !isBadUri && !hasStore = 16 times
    // needsAcl && !isBadUri && hasStore && isSystemStore = 8 times
    // needsAcl && !isBadUri && hasStore && !isSystemStore && hasAccess = 4 times
    verify(ctx, times(92)).fireChannelRead(req);

    // needsAcl && isBadUri = 32 times
    verify(ctx, times(32)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.BAD_REQUEST)));

    // needsAcl && !isBadUri && hasStore && !isSystemStore && !hasAccess && !hasAcl && !isFailOpen = 1 time
    verify(ctx, times(1)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.UNAUTHORIZED)));

    // needsAcl && !isBadUri && hasStore && !isSystemStore && !hasAccess && (hasAcl || isFailOpen) = 3 times
    verify(ctx, times(3)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.FORBIDDEN)));
  }

  private void update() throws Exception {
    when(accessController.hasAccess(any(), any(), any())).thenReturn(hasAccess[0]);
    when(accessController.hasAcl(any())).thenReturn(hasAcl[0]);
    when(accessController.isFailOpen()).thenReturn(isFailOpen[0]);
    when(metadataRepo.hasStore(any())).thenReturn(hasStore[0]);
    if (hasStore[0]) {
      when(metadataRepo.getStore(any())).thenReturn(store);
    } else {
      when(metadataRepo.getStore(any())).thenReturn(null);
    }
    String storeNameInRequest = storeName;
    if (isSystemStore[0]) {
      storeNameInRequest = VeniceSystemStoreUtils.getMetaStoreName(storeName);
    }
    when(store.isSystemStore()).thenReturn(isSystemStore[0]);
    if (!needsAcl[0]) {
      when(req.uri()).thenReturn("/noAcl");
    } else if (isBadUri[0]) {
      when(req.uri()).thenReturn("/badUri");
    } else {
      when(req.uri()).thenReturn(String.format("/goodUri/%s/random", storeNameInRequest));
    }
  }

  /**
   * Generate every possible combination for a given list of booleans based on variables passed
   * to boolean[]... conditions. If all variables (7 in count) are passed, then there will be 128
   * combinations:
   *
   * for (int i = 0; i < 128; i++) {        | i= 0   1   2   3   4 ...
   *   _hasAccess=          (i>>0) % 2 == 1|    F   T   F   T   F ...
   *   _hasAcl=             (i>>1) % 2 == 1|    F   F   T   T   F ...
   *   _hasStore=           (i>>2) % 2 == 1|    F   F   F   F   T ...
   *   _isAccessControlled= (i>>3) % 2 == 1|    F   F   F   F   F ...
   *   _isFailOpen=         (i>>4) % 2 == 1|    F   F   F   F   F ...
   *   _isBadUri=           (i>>5) % 2 == 1|    F   F   F   F   F ...
   *   _needsAcl=           (i>>6) % 2 == 1|    F   F   F   F   F ...
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
      AbstractStoreAclHandler aclHandler = spy(new MockStoreAclHandler(identityParser, accessController, metadataRepo));
      update();
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

  private enum TestRequestType {
    GOOD_URI, NO_ACL
  }

  /**
   * Assume a service with the following endpoints:
   * /goodUri/{storeName}/random
   * /noAcl
   */
  private static class MockStoreAclHandler extends AbstractStoreAclHandler<TestRequestType> {
    public MockStoreAclHandler(
        IdentityParser identityParser,
        DynamicAccessController accessController,
        HelixReadOnlyStoreRepository metadataRepository) {
      super(identityParser, accessController, metadataRepository);
    }

    @Override
    protected boolean needsAclValidation(TestRequestType requestType) {
      return requestType != TestRequestType.NO_ACL;
    }

    @Override
    protected String extractStoreName(TestRequestType requestType, String[] requestParts) {
      return requestParts[2];
    }

    @Override
    protected TestRequestType validateRequest(String[] requestParts) {
      if (requestParts[1].equals("noAcl")) {
        return TestRequestType.NO_ACL;
      } else if (requestParts[1].equals("goodUri")) {
        return TestRequestType.GOOD_URI;
      } else {
        return null;
      }
    }
  }
}
