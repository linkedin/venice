package com.linkedin.davinci.blobtransfer;

import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_REQUEST_ORIGIN;

import com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferRequestOrigin;
import com.linkedin.venice.acl.AclException;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.authorization.IdentityParser;
import com.linkedin.venice.listener.ServerHandlerUtils;
import com.linkedin.venice.request.RequestHelper;
import com.linkedin.venice.utils.NettyUtils;
import com.linkedin.venice.utils.SslUtils;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import java.net.URI;
import java.security.cert.X509Certificate;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Acl handler for blob transfer
 */
@ChannelHandler.Sharable
public class BlobTransferAclHandler extends SimpleChannelInboundHandler<HttpRequest> {
  private static final Logger LOGGER = LogManager.getLogger(BlobTransferAclHandler.class);
  private static final int STORE_NAME_PART_INDEX = 1;

  /** Per-store ACL controller for CLIENT-origin requests; empty on DVC peers, which admit same-issuer peers. */
  private final Optional<DynamicAccessController> storeAccessController;

  /**
   * Resolves the application identity from a caller's certificate. The format is parser-defined: the default returns
   * the subject DN, while a deployment may configure a parser that returns an application name (e.g. "venice-server").
   * Used to classify SERVER- vs CLIENT-origin by application identity.
   */
  private final IdentityParser identityParser;

  /**
   * Whether this server accepts client-origin blob requests. When false, a request classified as client-origin is
   * rejected with 403 and the per-store read ACL is not consulted.
   */
  private final boolean serverAcceptClientBlobRequestEnabled;

  public BlobTransferAclHandler(
      Optional<DynamicAccessController> storeAccessController,
      IdentityParser identityParser,
      boolean serverAcceptClientBlobRequestEnabled) {
    this.storeAccessController = storeAccessController;
    this.identityParser = identityParser;
    this.serverAcceptClientBlobRequestEnabled = serverAcceptClientBlobRequestEnabled;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, HttpRequest req) throws Exception {
    SslHandler sslHandler = ServerHandlerUtils.extractSslHandler(ctx);
    if (sslHandler == null) {
      LOGGER.error("No SSL handler in the incoming blob transfer request from {}", ctx.channel().remoteAddress());
      NettyUtils.setupResponseAndFlush(HttpResponseStatus.FORBIDDEN, new byte[0], false, ctx, true);
      return;
    }
    try {
      X509Certificate clientCert =
          SslUtils.getX509Certificate(sslHandler.engine().getSession().getPeerCertificates()[0]);
      X509Certificate localCert =
          SslUtils.getX509Certificate(sslHandler.engine().getSession().getLocalCertificates()[0]);

      boolean samePrincipal = clientCert.getIssuerX500Principal().equals(localCert.getIssuerX500Principal());
      if (samePrincipal) {
        // Servers classify same-issuer callers so the accept flag can reject CLIENT-origin requests when disabled.
        if (storeAccessController.isPresent() && rejectClientOrigin(ctx, req, clientCert, localCert)) {
          return;
        }
        LOGGER.info(
            "Client certification {} and local certification {} are the same. Acl check passed.",
            clientCert.getIssuerX500Principal(),
            localCert.getIssuerX500Principal());
        ReferenceCountUtil.retain(req);
        ctx.fireChannelRead(req);
      } else {
        String clientAddress = ctx.channel().remoteAddress().toString();
        LOGGER.error(
            "Unauthorized blob transfer access rejected: {} requested from {} "
                + "due to client certificate mismatch, expected {} but got {}",
            req.uri(),
            clientAddress,
            clientCert.getIssuerX500Principal(),
            localCert.getIssuerX500Principal());
        NettyUtils.setupResponseAndFlush(HttpResponseStatus.FORBIDDEN, new byte[0], false, ctx, true);
      }
    } catch (Exception e) {
      LOGGER.error("Error validating client certificate for blob transfer from {}", ctx.channel().remoteAddress(), e);
      NettyUtils.setupResponseAndFlush(HttpResponseStatus.FORBIDDEN, new byte[0], false, ctx, true);
    }
  }

  /**
   * For a same-issuer request on a server, classify the origin (tagging the channel for the downstream handler) and
   * apply the client-origin gates. Returns {@code true} — after sending a 403 — when a client-origin request is
   * rejected because the server is not accepting client requests or the per-store read ACL denies it. Returns
   * {@code false} when the request may proceed: a server-origin peer, or an authorized client-origin request.
   */
  private boolean rejectClientOrigin(
      ChannelHandlerContext ctx,
      HttpRequest req,
      X509Certificate clientCert,
      X509Certificate localCert) {
    if (classifyAndTagOrigin(ctx, clientCert, localCert) != BlobTransferRequestOrigin.CLIENT) {
      return false;
    }
    if (!serverAcceptClientBlobRequestEnabled) {
      LOGGER.warn(
          "Rejecting client-origin blob transfer request from {}: this server is not configured to accept client "
              + "requests.",
          ctx.channel().remoteAddress());
      NettyUtils.setupResponseAndFlush(HttpResponseStatus.FORBIDDEN, new byte[0], false, ctx, true);
      return true;
    }
    if (!isClientRequestAuthorized(ctx, req, clientCert)) {
      NettyUtils.setupResponseAndFlush(HttpResponseStatus.FORBIDDEN, new byte[0], false, ctx, true);
      return true;
    }
    return false;
  }

  /**
   * Resolve the caller and local application identities, classify the request origin, and tag the channel with it so
   * downstream handlers can read it.
   */
  private BlobTransferRequestOrigin classifyAndTagOrigin(
      ChannelHandlerContext ctx,
      X509Certificate clientCert,
      X509Certificate localCert) {
    String callerApplication = identityParser.parseIdentityFromCert(clientCert);
    String localApplication = identityParser.parseIdentityFromCert(localCert);
    BlobTransferRequestOrigin origin = determineRequestOrigin(callerApplication, localApplication);
    ctx.channel().attr(BLOB_TRANSFER_REQUEST_ORIGIN).set(origin);
    LOGGER.info("Classified blob transfer request from {} as {}-origin.", ctx.channel().remoteAddress(), origin);
    return origin;
  }

  /**
   * Whether a client-origin request passes the per-store read ACL. Logs the rejection reason and returns {@code false}
   * when the store name is unparseable or access is denied; logs a grant and returns {@code true} otherwise.
   */
  private boolean isClientRequestAuthorized(ChannelHandlerContext ctx, HttpRequest req, X509Certificate clientCert) {
    String storeName = extractStoreName(req.uri());
    if (storeName == null || !isClientStoreAccessAllowed(clientCert, storeName, req.method().name())) {
      LOGGER.warn(
          "Unauthorized client-origin blob transfer access rejected: {} requested from {} for store {}.",
          req.uri(),
          ctx.channel().remoteAddress(),
          storeName);
      return false;
    }
    LOGGER.debug("Client-origin blob transfer request granted store-level ACL for store {}.", storeName);
    return true;
  }

  /**
   * Classify the caller as SERVER- or CLIENT-origin by comparing application identities (resolved by the configured
   * {@link IdentityParser}). A caller whose application matches the local host's is a trusted peer (server-to-server or
   * same-application peer-to-peer) and is SERVER-origin; a different application is an external consumer and is
   * CLIENT-origin, subject to the per-store ACL and accept gate. Fails closed: a null or unresolved local application
   * yields CLIENT.
   */
  static BlobTransferRequestOrigin determineRequestOrigin(String callerApplication, String localApplication) {
    return localApplication != null && localApplication.equals(callerApplication)
        ? BlobTransferRequestOrigin.SERVER
        : BlobTransferRequestOrigin.CLIENT;
  }

  /**
   * Extract the Venice store name from a blob transfer request URI of the form
   * {@code /<store>/<version>/<partition>/<tableFormat>}. Returns {@code null} if the URI is malformed.
   */
  static String extractStoreName(String uri) {
    try {
      String[] requestParts = RequestHelper.getRequestParts(URI.create(uri));
      if (requestParts.length > STORE_NAME_PART_INDEX && !requestParts[STORE_NAME_PART_INDEX].isEmpty()) {
        return requestParts[STORE_NAME_PART_INDEX];
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to parse store name from blob transfer URI {}", uri, e);
    }
    return null;
  }

  /**
   * Enforce the per-store read ACL for a client-origin request. Fails closed when the ACL lookup errors.
   */
  boolean isClientStoreAccessAllowed(X509Certificate clientCert, String storeName, String method) {
    try {
      return storeAccessController.get().hasAccess(clientCert, storeName, method);
    } catch (AclException e) {
      LOGGER
          .error("Error checking store-level ACL for blob transfer of store {}; denying client request.", storeName, e);
      return false;
    }
  }
}
