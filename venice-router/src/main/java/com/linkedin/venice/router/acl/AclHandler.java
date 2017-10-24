package com.linkedin.venice.router.acl;

import com.linkedin.venice.helix.HelixReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.router.MetaDataHandler;
import com.linkedin.venice.router.api.VenicePathParserHelper;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.stream.Collectors;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.validation.constraints.NotNull;
import org.apache.log4j.Logger;

@ChannelHandler.Sharable
public class AclHandler extends SimpleChannelInboundHandler<HttpRequest> {
  private static final Logger logger = Logger.getLogger(AclHandler.class);
  private HelixReadOnlyStoreRepository _metadataRepository;
  private AccessController _accessController;

  public AclHandler(@NotNull AccessController accessController, @NotNull HelixReadOnlyStoreRepository metadataRepository) {
    _metadataRepository = metadataRepository;
    _accessController = accessController.init(
        metadataRepository.getAllStores().stream().map(Store::getName).collect(Collectors.toList()));
    _metadataRepository.registerStoreDataChangedListener(new StoreCreationDeletionListener());
  }

  /**
   * Verify if client has permission to access.
   *
   * @param ctx
   * @param req
   * @throws IOException
   */
  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpRequest req) throws SSLPeerUnverifiedException {
    X509Certificate clientCert = (X509Certificate) ctx.pipeline().get(SslHandler.class).engine().getSession().getPeerCertificates()[0];
    String storeName = new VenicePathParserHelper(req.uri()).getResourceName();
    String method = req.method().name();

    if (!_metadataRepository.getStore(storeName).isAccessControlled()) {
      // Ignore permission. Proceed
      ReferenceCountUtil.retain(req);
      ctx.fireChannelRead(req);
    } else {
      try {
        if (_accessController.hasAccess(clientCert, storeName, method)) {  // Keep this the outmost layer to maximize performance
          // Client has permission. Proceed
          ReferenceCountUtil.retain(req);
          ctx.fireChannelRead(req);
        } else {
          // Fact:
          //   Request gets rejected.
          // Possible Reasons:
          //   A. ACL not found. OR,
          //   B. ACL exists but caller does not have permission.
          //
          //   More specificly for case A, ACL not found because:
          //    *1. Requested resource does not exist, therefore the ACL is missing also. OR,
          //     2. Requested resource exists but does not have ACL for some reason.
          //
          // * In such case, the request will eventually fail regardless of ACL enabled or not, permission granted or not,
          //   therefore should fail fast.

          String client = ctx.channel().remoteAddress().toString(); //ip and port
          String errLine = String.format("%s requested %s %s", client, method, req.uri());

          if (_metadataRepository.hasStore(storeName)) {
            if (!_accessController.isFailOpen() && !_accessController.hasAcl(storeName)) {  // short circuit, order matters
              // Case A2
              // Conditions:
              //   0. (outside) Store exists and is being access controlled. AND,
              //   1. (left) The following policy is applied: if ACL not found, reject the request. AND,
              //   2. (right) ACL not found.
              // Result:
              //   Request is rejected by .hasAccess()
              // Root cause:
              //   Requested resource exists but does not have ACL.
              // Action:
              //   return 500 Internal Server Error
              logger.error("Requested store does not have ACL: " + errLine);
              logger.warn("\nExisting stores: " + _metadataRepository.getAllStores().stream().map(Store::getName).sorted()
                  .collect(Collectors.toList()) + "\nAccess-controlled stores: "
                  + _accessController.getAccessControlledStores().stream().sorted().collect(Collectors.toList()));
              MetaDataHandler.setupResponseAndFlush(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                  "ACL not found!\nPlease report the error!".getBytes(), false, ctx);
            } else {
              // Case B
              // Conditions:
              //   1. Fail closed, and ACL found. OR,
              //   2. Fail open, and ACL found. OR,
              //   3. Fail open, and ACL not found.
              // Analyses:
              //   (1) ACL exists, therefore result is determined by ACL.
              //       Since the request has been rejected, it must be due to lack of permission.
              //   (2) ACL exists, therefore result is determined by ACL.
              //       Since the request has been rejected, it must be due to lack of permission.
              //   (3) In such case, request would NOT be rejected in the first place,
              //       according to the definition in AccessController interface.
              //       Contradiction to the fact, therefore this case is impossible.
              // Root cause:
              //   Caller does not have permission to access the resource.
              // Action:
              //   return 403 Forbidden
              logger.debug("Unauthorized access rejected: " + errLine);
              MetaDataHandler.setupResponseAndFlush(HttpResponseStatus.FORBIDDEN, new byte[0], false, ctx);
            }
          } else {
            // Case A1
            logger.debug("Requested store does not exist: " + errLine);
            MetaDataHandler.setupResponseAndFlush(HttpResponseStatus.BAD_REQUEST, ("Invalid Venice store name: " + storeName).getBytes(), false, ctx);
          }
        }
      } catch (AclException e) {
        String client = ctx.channel().remoteAddress().toString(); //ip and port
        String errLine = String.format("%s requested %s %s", client, method, req.uri());

        if (_metadataRepository.hasStore(storeName)) {
          if (_accessController.isFailOpen()) {
            logger.warn("Exception occurred! Access granted: " + errLine + "\n" + e);
            ReferenceCountUtil.retain(req);
            ctx.fireChannelRead(req);
          } else {
            logger.warn("Exception occurred! Access rejected: " + errLine + "\n" + e);
            MetaDataHandler.setupResponseAndFlush(HttpResponseStatus.FORBIDDEN, new byte[0], false, ctx);
          }
        } else {
          // Case A1
          logger.debug("Requested store does not exist: " + errLine);
          MetaDataHandler.setupResponseAndFlush(HttpResponseStatus.BAD_REQUEST, ("Invalid Venice store name: " + storeName).getBytes(), false, ctx);
        }
      }
    }
  }

  class StoreCreationDeletionListener implements StoreDataChangedListener {

    @Override
    public void handleStoreCreated(Store store) {
      logger.debug("Added \"" + store.getName() + "\" to store list. New store list: " +
          _metadataRepository.getAllStores().stream().map(Store::getName).sorted().collect(Collectors.toList()));
      logger.debug("Previous ACL list: " + _accessController.getAccessControlledStores());
      try {
        _accessController.addAcl(store.getName());
      } catch (AclException e) {
        logger.error("Cannot add store to resource list: " + store.getName());
      }
      logger.debug("*EXPECTED* current ACL list: " + _accessController.getAccessControlledStores() +
          " + " + store.getName() ); // Actual ACL list cannot be determined yet
    }

    @Override
    public void handleStoreDeleted(String storeName) {
      logger.debug("Removed \"" + storeName + "\" from store list. New store list: " +
          _metadataRepository.getAllStores().stream().map(Store::getName).sorted().collect(Collectors.toList()));
      logger.debug("Previous ACL list: " + _accessController.getAccessControlledStores());
      try {
        _accessController.removeAcl(storeName);
      } catch (AclException e) {
        logger.error("Cannot remove store from resource list: " + storeName);
      }
      logger.debug("Current ACL list: " + _accessController.getAccessControlledStores());
    }

    @Override
    public void handleStoreChanged(Store store) {
    }
  }
}
