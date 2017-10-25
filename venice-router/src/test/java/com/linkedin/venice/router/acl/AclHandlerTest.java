package com.linkedin.venice.router.acl;

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
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class AclHandlerTest {
  private AccessController accessController;
  private HelixReadOnlyStoreRepository metadataRepo;
  private ChannelHandlerContext ctx;
  private HttpRequest req;
  private AclHandler aclHandler;
  private Store store;

  private boolean[] _hasAccess = {false};
  private boolean[] _hasAcl = {false};
  private boolean[] _hasStore = {false};
  private boolean[] _isAccessControlled = {false};
  private boolean[] _isFailOpen = {false};

  @BeforeMethod
  public void setup() throws Exception {
    accessController = mock(AccessController.class);
    metadataRepo = mock(HelixReadOnlyStoreRepository.class);
    ctx = mock(ChannelHandlerContext.class);
    req = mock(HttpRequest.class);
    store = mock(Store.class);

    when(accessController.init(any())).thenReturn(accessController);
    aclHandler = spy(new AclHandler(accessController, metadataRepo));

    when(metadataRepo.getStore(any())).then((Answer<Store>) invocation -> metadataRepo.hasStore("storename") ? store : null);

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
    when(sslSession.getPeerCertificates()).thenReturn(new Certificate[]{cert});

    // Host
    Channel channel = mock(Channel.class);
    when(ctx.channel()).thenReturn(channel);
    SocketAddress address = mock(SocketAddress.class);
    when(channel.remoteAddress()).thenReturn(address);

    when(req.uri()).thenReturn("/random/storename/random");
    when(req.method()).thenReturn(HttpMethod.GET);
  }

  @Test
  public void accessGranted() throws Exception {
    _hasAccess[0] = true;
    enumerate(_hasAcl, _hasStore, _isAccessControlled, _isFailOpen);

    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.FORBIDDEN)));
    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.INTERNAL_SERVER_ERROR)));

    // Store doesn't exist 8 times
    verify(ctx, times(8)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.BAD_REQUEST)));

    // No access control 4 times + access control 4 times
    verify(ctx, times(8)).fireChannelRead(req);
  }

  @Test
  public void accessDenied() throws Exception {
    _hasAccess[0] = false;
    enumerate(_hasAcl, _hasStore, _isAccessControlled, _isFailOpen);

    // Store doesn't exist 8 times
    verify(ctx, times(8)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.BAD_REQUEST)));

    // No access control 4 times
    verify(ctx, times(4)).fireChannelRead(req);

    // 1 of the 4 rejects is due to internal error
    verify(ctx, times(1)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.INTERNAL_SERVER_ERROR)));

    // The other 3 are regular rejects
    verify(ctx, times(3)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.FORBIDDEN)));
  }

  @Test
  public void storeExists() throws Exception {
    _hasStore[0] = true;
    enumerate(_hasAccess, _hasAcl, _isFailOpen, _isAccessControlled);

    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.BAD_REQUEST)));

    // No access control 8 times, access control 4 times granted
    verify(ctx, times(12)).fireChannelRead(req);

    // 1 of the 4 rejects is due to internal error
    verify(ctx, times(1)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.INTERNAL_SERVER_ERROR)));

    // The other 3 are regular rejects
    verify(ctx, times(3)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.FORBIDDEN)));
  }

  @Test
  public void storeMissing() throws Exception {
    _hasStore[0] = false;
    enumerate(_hasAccess, _hasAcl, _isFailOpen, _isAccessControlled);

    verify(ctx, never()).fireChannelRead(req);
    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.FORBIDDEN)));
    verify(ctx, never()).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.INTERNAL_SERVER_ERROR)));
    verify(ctx, times(16)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.BAD_REQUEST)));
  }

  @Test
  public void aclDisabled() throws Exception {
    _isAccessControlled[0] = false;
    _hasStore[0] = true;
    enumerate(_hasAccess, _hasAcl, _isFailOpen);

    verify(ctx, never()).writeAndFlush(any());
    verify(ctx, times(8)).fireChannelRead(req);
  }

  @Test
  public void aclMissing() throws Exception {
    enumerate(_hasStore, _hasAcl, _hasAccess, _isAccessControlled, _isFailOpen);

    verify(ctx, times(1)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.INTERNAL_SERVER_ERROR)));

    // One of the cases is impossible in reality. See AclHandler.java comments
    verify(ctx, times(3)).writeAndFlush(argThat(new ContextMatcher(HttpResponseStatus.FORBIDDEN)));
  }

  private void update() throws Exception {
    when(accessController.hasAccess(any(), any(), any())).thenReturn(_hasAccess[0]);
    when(accessController.hasAcl(any())).thenReturn(_hasAcl[0]);
    when(accessController.isFailOpen()).thenReturn(_isFailOpen[0]);
    when(metadataRepo.hasStore(any())).thenReturn(_hasStore[0]);
    when(store.isAccessControlled()).thenReturn(_isAccessControlled[0]);
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
   * }
   */
  private void enumerate(boolean[]... conditions) throws Exception {
    int len = conditions.length;
    for (int i = 0; i < Math.pow(2, len); i++) {
      for (int j = 0; j < len; j++) {
        conditions[j][0] = (i>>j) % 2 == 1;
      }
      update();
      aclHandler.channelRead0(ctx, req);
    }
  }

  public class ContextMatcher implements ArgumentMatcher<FullHttpResponse> {
    private HttpResponseStatus status;

    public ContextMatcher(HttpResponseStatus status) {
      this.status = status;
    }

    @Override
    public boolean matches(FullHttpResponse argument) {
      return argument.status().equals(status) ;
    }
  }
}