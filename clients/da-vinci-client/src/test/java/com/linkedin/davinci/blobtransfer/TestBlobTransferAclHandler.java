package com.linkedin.davinci.blobtransfer;

import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferRequestOrigin.CLIENT;
import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferRequestOrigin.SERVER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.acl.AclException;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.authorization.DefaultIdentityParser;
import com.linkedin.venice.authorization.IdentityParser;
import com.linkedin.venice.utils.VeniceProperties;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestBlobTransferAclHandler {
  private static final String REQUEST_URI = "/myStore/1/10/BLOCK_BASED_TABLE";

  @Test
  public void testServerOriginWhenApplicationsMatch() {
    // A peer whose application matches the local host's application is a trusted peer: a peer Venice server (both
    // "venice-server"), or two DVC peers of the same application for DVC-to-DVC transfer.
    Assert.assertEquals(BlobTransferAclHandler.determineRequestOrigin("venice-server", "venice-server"), SERVER);
    Assert.assertEquals(BlobTransferAclHandler.determineRequestOrigin("cdc-client-app", "cdc-client-app"), SERVER);
  }

  @Test
  public void testClientOriginWhenApplicationsDiffer() {
    // A Stateful CDC / Da Vinci client carries a different application identity than the server, so it is CLIENT.
    Assert.assertEquals(BlobTransferAclHandler.determineRequestOrigin("stateful-cdc-client", "venice-server"), CLIENT);
  }

  @Test
  public void testClientOriginWhenLocalApplicationUnresolved() {
    // Fail closed: a null/unresolved local application must never grant SERVER trust.
    Assert.assertEquals(BlobTransferAclHandler.determineRequestOrigin("venice-server", null), CLIENT);
    Assert.assertEquals(BlobTransferAclHandler.determineRequestOrigin(null, null), CLIENT);
  }

  @Test
  public void testCreateAclHandlerRequiresStoreAccessControllerWhenAcceptingClientRequests() {
    VeniceConfigLoader configLoader = mockAclConfigLoader(NonDefaultIdentityParser.class);
    IllegalArgumentException exception = Assert.expectThrows(
        IllegalArgumentException.class,
        () -> BlobTransferUtils.createAclHandler(configLoader, Optional.empty(), true));
    Assert.assertTrue(exception.getMessage().contains("storeAccessController is required"));
  }

  @Test
  public void testCreateAclHandlerRejectsDefaultIdentityParserOnServerPath() {
    VeniceConfigLoader configLoader = mockAclConfigLoader(DefaultIdentityParser.class);
    IllegalArgumentException exception = Assert.expectThrows(
        IllegalArgumentException.class,
        () -> BlobTransferUtils
            .createAclHandler(configLoader, Optional.of(mock(DynamicAccessController.class)), false));
    Assert.assertTrue(exception.getMessage().contains("non-default identity.parser.class"));
  }

  @Test
  public void testClientStoreAccessAllowedWhenControllerGrants() throws Exception {
    DynamicAccessController controller = mock(DynamicAccessController.class);
    when(controller.hasAccess(any(), eq("myStore"), eq("GET"))).thenReturn(true);
    BlobTransferAclHandler handler =
        new BlobTransferAclHandler(Optional.of(controller), new DefaultIdentityParser(), true);
    Assert.assertTrue(handler.isClientStoreAccessAllowed(mock(X509Certificate.class), "myStore", "GET"));
  }

  @Test
  public void testClientStoreAccessDeniedWhenControllerRejects() throws Exception {
    DynamicAccessController controller = mock(DynamicAccessController.class);
    when(controller.hasAccess(any(), eq("myStore"), eq("GET"))).thenReturn(false);
    BlobTransferAclHandler handler =
        new BlobTransferAclHandler(Optional.of(controller), new DefaultIdentityParser(), true);
    Assert.assertFalse(handler.isClientStoreAccessAllowed(mock(X509Certificate.class), "myStore", "GET"));
  }

  @Test
  public void testClientStoreAccessDeniedOnAclException() throws Exception {
    DynamicAccessController controller = mock(DynamicAccessController.class);
    when(controller.hasAccess(any(), eq("myStore"), eq("GET"))).thenThrow(new AclException("boom"));
    BlobTransferAclHandler handler =
        new BlobTransferAclHandler(Optional.of(controller), new DefaultIdentityParser(), true);
    Assert.assertFalse(handler.isClientStoreAccessAllowed(mock(X509Certificate.class), "myStore", "GET"));
  }

  @Test
  public void testExtractStoreName() {
    Assert.assertEquals(BlobTransferAclHandler.extractStoreName("/myStore/1/10/BLOCK_BASED_TABLE"), "myStore");
    Assert.assertNull(BlobTransferAclHandler.extractStoreName("/"));
  }

  @Test
  public void testClientOriginDeniedStoreAclReturns403AndDoesNotForward() throws Exception {
    // End-to-end through channelRead0: a CLIENT-origin caller (different subject, same issuer) whose store ACL is
    // denied must get 403 FORBIDDEN and must NOT be forwarded downstream.
    SslHandler sslHandler = mockSslHandler("CN=stateful-cdc-client, O=linkedin", "CN=venice-server, O=linkedin");
    DynamicAccessController controller = mock(DynamicAccessController.class);
    when(controller.hasAccess(any(), eq("myStore"), any())).thenReturn(false);
    BlobTransferAclHandler handler =
        new BlobTransferAclHandler(Optional.of(controller), new DefaultIdentityParser(), true);

    // sslHandler is placed AFTER the ACL handler so extractSslHandler (pipeline.get(SslHandler.class)) can find it,
    // without the mock intercepting the inbound request before the ACL handler runs.
    EmbeddedChannel channel = new EmbeddedChannel(handler, sslHandler);
    channel.writeInbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, REQUEST_URI));

    Object outbound = channel.readOutbound();
    Assert.assertTrue(outbound instanceof FullHttpResponse, "Expected an HTTP response");
    Assert.assertEquals(((FullHttpResponse) outbound).status(), HttpResponseStatus.FORBIDDEN);
    Assert.assertNull(channel.readInbound(), "Denied client-origin request must not be forwarded downstream");
    channel.finishAndReleaseAll();
  }

  @Test
  public void testClientOriginRejectedWhenAcceptDisabled() throws Exception {
    // A server has a store access controller, so it classifies same-issuer callers even when the accept flag is off:
    // client-origin requests are rejected before the store ACL is consulted.
    SslHandler sslHandler = mockSslHandler("CN=stateful-cdc-client, O=linkedin", "CN=venice-server, O=linkedin");
    DynamicAccessController controller = mock(DynamicAccessController.class);
    when(controller.hasAccess(any(), anyString(), anyString())).thenReturn(true);
    BlobTransferAclHandler handler =
        new BlobTransferAclHandler(Optional.of(controller), new DefaultIdentityParser(), false);

    EmbeddedChannel channel = new EmbeddedChannel(handler, sslHandler);
    channel.writeInbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, REQUEST_URI));

    Object outbound = channel.readOutbound();
    Assert.assertTrue(outbound instanceof FullHttpResponse, "Expected an HTTP response");
    Assert.assertEquals(((FullHttpResponse) outbound).status(), HttpResponseStatus.FORBIDDEN);
    Assert.assertNull(channel.readInbound(), "Client-origin request must not be forwarded when accept flag is off");
    verify(controller, never()).hasAccess(any(), anyString(), anyString());
    channel.finishAndReleaseAll();
  }

  @Test
  public void testServerOriginForwardedWhenAcceptDisabled() throws Exception {
    // The accept flag gates only client-origin requests: a peer server (same application identity) is forwarded even
    // when the flag is off, so server-to-server transfer is unaffected.
    SslHandler sslHandler = mockSslHandler("CN=venice-server, O=linkedin", "CN=venice-server, O=linkedin");
    DynamicAccessController controller = mock(DynamicAccessController.class);
    BlobTransferAclHandler handler =
        new BlobTransferAclHandler(Optional.of(controller), new DefaultIdentityParser(), false);

    AtomicReference<Object> forwarded = new AtomicReference<>();
    EmbeddedChannel channel = new EmbeddedChannel(handler, captureHandler(forwarded), sslHandler);
    channel.writeInbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, REQUEST_URI));

    Assert.assertNull(channel.readOutbound(), "A peer server must not be rejected when accept flag is off");
    Assert.assertNotNull(forwarded.get(), "A peer server must be forwarded downstream");
    verify(controller, never()).hasAccess(any(), anyString(), anyString());
    channel.finishAndReleaseAll();
  }

  @Test
  public void testSameIssuerAdmittedWhenNoControllerEvenIfIdentityDiffers() throws Exception {
    // Regression: a node with no store access controller (e.g. a Da Vinci peer) must admit any same-issuer peer, even
    // when the peer's certificate identity differs from the local node's (distinct per-host/per-application certs).
    // It must NOT reclassify the peer as CLIENT and reject it -- that would break pre-existing DVC-to-DVC transfer.
    SslHandler sslHandler = mockSslHandler("CN=davinci-host-a, O=linkedin", "CN=davinci-host-b, O=linkedin");
    // No store access controller -> legacy same-issuer trust applies.
    BlobTransferAclHandler handler = new BlobTransferAclHandler(Optional.empty(), new DefaultIdentityParser(), false);

    AtomicReference<Object> forwarded = new AtomicReference<>();
    EmbeddedChannel channel = new EmbeddedChannel(handler, captureHandler(forwarded), sslHandler);
    channel.writeInbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, REQUEST_URI));

    Assert.assertNull(channel.readOutbound(), "A same-issuer peer must not be rejected when no controller is present");
    Assert.assertNotNull(forwarded.get(), "A same-issuer peer must be forwarded downstream");
    channel.finishAndReleaseAll();
  }

  @Test
  public void testSameIssuerForwardedWhenNoController() throws Exception {
    SslHandler sslHandler = mockSslHandler("CN=stateful-cdc-client, O=linkedin", "CN=venice-server, O=linkedin");
    BlobTransferAclHandler handler = new BlobTransferAclHandler(Optional.empty(), new DefaultIdentityParser(), true);

    AtomicReference<Object> forwarded = new AtomicReference<>();
    EmbeddedChannel channel = new EmbeddedChannel(handler, captureHandler(forwarded), sslHandler);
    channel.writeInbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, REQUEST_URI));

    Assert.assertNull(channel.readOutbound(), "No-controller path keeps legacy same-issuer behavior");
    Assert.assertNotNull(forwarded.get(), "Same-issuer request must be forwarded when no controller is present");
    channel.finishAndReleaseAll();
  }

  /** Build a mock {@link SslHandler} whose session presents a caller and local certificate with the given subjects
   * and a shared issuer, as the ACL handler reads them. */
  private static SslHandler mockSslHandler(String callerSubject, String localSubject) throws Exception {
    X500Principal issuer = new X500Principal("CN=venice-ca, O=linkedin");
    X509Certificate caller = mock(X509Certificate.class);
    X509Certificate local = mock(X509Certificate.class);
    when(caller.getIssuerX500Principal()).thenReturn(issuer);
    when(local.getIssuerX500Principal()).thenReturn(issuer);
    when(caller.getSubjectX500Principal()).thenReturn(new X500Principal(callerSubject));
    when(local.getSubjectX500Principal()).thenReturn(new X500Principal(localSubject));

    SSLSession session = mock(SSLSession.class);
    when(session.getPeerCertificates()).thenReturn(new Certificate[] { caller });
    when(session.getLocalCertificates()).thenReturn(new Certificate[] { local });
    SSLEngine engine = mock(SSLEngine.class);
    when(engine.getSession()).thenReturn(session);
    SslHandler sslHandler = mock(SslHandler.class);
    when(sslHandler.engine()).thenReturn(engine);
    return sslHandler;
  }

  /** A capture handler that records the first request forwarded past the ACL handler. */
  private static ChannelInboundHandlerAdapter captureHandler(AtomicReference<Object> forwarded) {
    return new ChannelInboundHandlerAdapter() {
      @Override
      public void channelRead(ChannelHandlerContext context, Object msg) {
        forwarded.set(msg);
        ReferenceCountUtil.release(msg);
      }
    };
  }

  private static VeniceConfigLoader mockAclConfigLoader(Class<? extends IdentityParser> identityParserClass) {
    Properties properties = new Properties();
    properties.setProperty(ConfigKeys.BLOB_TRANSFER_SSL_ENABLED, "true");
    properties.setProperty(ConfigKeys.BLOB_TRANSFER_ACL_ENABLED, "true");
    properties.setProperty(ConfigKeys.IDENTITY_PARSER_CLASS, identityParserClass.getName());
    VeniceConfigLoader configLoader = mock(VeniceConfigLoader.class);
    when(configLoader.getCombinedProperties()).thenReturn(new VeniceProperties(properties));
    return configLoader;
  }

  public static class NonDefaultIdentityParser implements IdentityParser {
    @Override
    public String parseIdentityFromCert(X509Certificate certificate) {
      return certificate.getSubjectX500Principal().getName();
    }
  }
}
