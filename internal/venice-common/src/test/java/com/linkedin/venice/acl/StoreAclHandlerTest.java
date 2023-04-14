package com.linkedin.venice.acl;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.acl.handler.StoreAclHandler;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
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


public class StoreAclHandlerTest {
  private DynamicAccessController accessController;
  private HelixReadOnlyStoreRepository metadataRepo;
  private ChannelHandlerContext ctx;
  private Channel channel;
  private HttpRequest req;
  private StoreAclHandler aclHandler;
  private Store store;

  private boolean[] hasAccess = { false };
  private boolean[] hasAcl = { false };
  private boolean[] hasStore = { false };
  private boolean[] isSystemStore = { false };
  private boolean[] isFailOpen = { false };
  private boolean[] isMetadata = { false };

  @BeforeMethod
  public void setUp() throws Exception {
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
    channel = mock(Channel.class);
    when(ctx.channel()).thenReturn(channel);
    SocketAddress address = mock(SocketAddress.class);
    when(channel.remoteAddress()).thenReturn(address);

    when(req.method()).thenReturn(HttpMethod.GET);
  }

  @Test
  public void accessGranted() throws Exception {
    hasAccess[0] = true;
    enumerate(hasAcl, hasStore, isSystemStore, isFailOpen, isMetadata);

    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.FORBIDDEN)));
    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.UNAUTHORIZED)));

    // Store doesn't exist 8 times
    verify(ctx, times(8)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.BAD_REQUEST)));

    // No access control 20 times + access control 4 times
    verify(ctx, times(24)).fireChannelRead(req);
  }

  @Test
  public void accessDenied() throws Exception {
    hasAccess[0] = false;
    enumerate(hasAcl, hasStore, isSystemStore, isFailOpen, isMetadata);

    // Store doesn't exist 8 times
    verify(ctx, times(8)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.BAD_REQUEST)));

    // No access control 20 times
    verify(ctx, times(20)).fireChannelRead(req);

    // 1 of the 4 rejects is due to internal error
    verify(ctx, times(1)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.UNAUTHORIZED)));

    // The other 3 are regular rejects
    verify(ctx, times(3)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.FORBIDDEN)));
  }

  @Test
  public void storeExists() throws Exception {
    hasStore[0] = true;
    enumerate(hasAccess, hasAcl, isFailOpen, isSystemStore, isMetadata);

    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.BAD_REQUEST)));

    // No access control 24 times, access control 4 times granted
    verify(ctx, times(28)).fireChannelRead(req);

    // 1 of the 4 rejects is due to internal error
    verify(ctx, times(1)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.UNAUTHORIZED)));

    // The other 3 are regular rejects
    verify(ctx, times(3)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.FORBIDDEN)));
  }

  @Test
  public void storeMissing() throws Exception {
    hasStore[0] = false;
    enumerate(hasAccess, hasAcl, isFailOpen, isSystemStore, isMetadata);

    verify(ctx, times(16)).fireChannelRead(req);
    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.FORBIDDEN)));
    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.UNAUTHORIZED)));
    verify(ctx, times(16)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.BAD_REQUEST)));
  }

  @Test
  public void aclDisabledForSystemStore() throws Exception {
    isSystemStore[0] = true;
    hasStore[0] = true;
    enumerate(hasAccess, hasAcl, isFailOpen, isMetadata);

    verify(ctx, never()).writeAndFlush(any());
    verify(ctx, times(16)).fireChannelRead(req);
  }

  @Test
  public void aclDisabledForMetadataEndpoint() throws Exception {
    isMetadata[0] = true;
    enumerate(hasAccess, hasAcl, isSystemStore, isFailOpen);

    verify(ctx, never()).writeAndFlush(any());
    verify(ctx, times(16)).fireChannelRead(req);
  }

  @Test
  public void aclMissing() throws Exception {
    enumerate(hasStore, hasAcl, hasAccess, isSystemStore, isFailOpen, isMetadata);

    verify(ctx, times(1)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.UNAUTHORIZED)));

    // One of the cases is impossible in reality. See StoreAclHandler.java comments
    verify(ctx, times(3)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.FORBIDDEN)));
  }

  private void update() throws Exception {
    when(accessController.hasAccess(any(), any(), any())).thenReturn(hasAccess[0]);
    when(accessController.hasAcl(any())).thenReturn(hasAcl[0]);
    when(accessController.isFailOpen()).thenReturn(isFailOpen[0]);
    when(metadataRepo.hasStore(any())).thenReturn(hasStore[0]);
    if (hasStore[0]) {
      when(metadataRepo.getStoreOrThrow(any())).thenReturn(store);
    } else {
      when(metadataRepo.getStoreOrThrow(any())).thenThrow(new VeniceNoStoreException("storename"));
    }
    when(store.isSystemStore()).thenReturn(isSystemStore[0]);
    if (isMetadata[0]) {
      when(req.uri()).thenReturn("/metadata/storename/random");
    } else {
      when(req.uri()).thenReturn("/random/storename/random");
    }
  }

  /**
   * Generate every possible combination for a given list of booleans
   *
   * for (int i = 0; i < 32; i++) {        | i= 0   1   2   3   4 ...
   *   _hasAccess=          (i>>0) % 2 == 1|    F   T   F   T   F ...
   *   _hasAcl=             (i>>1) % 2 == 1|    F   F   T   T   F ...
   *   _hasStore=           (i>>2) % 2 == 1|    F   F   F   F   T ...
   *   _isAccessControlled= (i>>3) % 2 == 1|    F   F   F   F   F ...
   *   _isFailOpen=         (i>>4) % 2 == 1|    F   F   F   F   F ...
   *   _isMetadata=         (i>>5) % 2 == 1|    F   F   F   F   F ...
   * }
   */
  private void enumerate(boolean[]... conditions) throws Exception {
    int len = conditions.length;
    for (int i = 0; i < Math.pow(2, len); i++) {
      for (int j = 0; j < len; j++) {
        conditions[j][0] = ((i >> j) & 1) == 1;
      }
      // New metadataRepo mock and aclHandler every update since thenThrow cannot be re-mocked.
      metadataRepo = mock(HelixReadOnlyStoreRepository.class);
      aclHandler = spy(new StoreAclHandler(accessController, metadataRepo));
      update();
      aclHandler.channelRead0(ctx, req);
    }
  }

  public static class ContextMatcher implements ArgumentMatcher<FullHttpResponse> {
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
