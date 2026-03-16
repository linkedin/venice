package com.linkedin.venice.listener;

import com.linkedin.venice.authorization.IdentityParser;
import com.linkedin.venice.stats.ServerConnectionStats;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.LogContext;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


@ChannelHandler.Sharable
public class ServerConnectionStatsHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOGGER = LogManager.getLogger(ServerConnectionStatsHandler.class);
  public static final AttributeKey<Boolean> CHANNEL_ACTIVATED = AttributeKey.valueOf("channelActivated");
  public static final AttributeKey<Long> CHANNEL_INIT_START_TS = AttributeKey.valueOf("channelInitStartTs");
  private final IdentityParser identityParser;
  private final ServerConnectionStats serverConnectionStats;
  private final String routerPrincipalName;

  private final Set<ChannelHandlerContext> newConnections =
      new ConcurrentSkipListSet<>(Comparator.comparingInt(Object::hashCode));
  private final Set<ChannelHandlerContext> trackedConnections =
      new ConcurrentSkipListSet<>(Comparator.comparingInt(Object::hashCode));

  private final ScheduledExecutorService connectionScanner;

  public ServerConnectionStatsHandler(
      IdentityParser identityParser,
      ServerConnectionStats serverConnectionStats,
      String routerPrincipalName,
      LogContext logContext) {
    this.identityParser = identityParser;
    this.serverConnectionStats = serverConnectionStats;
    this.routerPrincipalName = routerPrincipalName;
    this.connectionScanner = Executors
        .newSingleThreadScheduledExecutor(new DaemonThreadFactory("ServerConnectionStatsHandler-Scanner", logContext));

    connectionScanner.scheduleAtFixedRate(() -> {
      try {
        if (newConnections.isEmpty()) {
          return;
        }
        Set<ChannelHandlerContext> newConnectionsCopy = new HashSet<>(newConnections);
        for (ChannelHandlerContext ctx: newConnectionsCopy) {
          SslHandler sslHandler = extractSslHandler(ctx);
          if (sslHandler == null) {
            /**
             * ssl handler is not available yet, which means the ssl handshake is still in progress, so
             * we will track it in next iteration.
             */
            continue;
          }
          String principalName = getPrincipal(sslHandler);
          if (principalName == null) {
            /**
             * principal name is not available yet, which means the ssl handshake is still in progress, so
             * we will track it in next iteration.
             */
            continue;
          }
          if (principalName.contains(routerPrincipalName)) {
            serverConnectionStats.incrementRouterConnectionCount();
          } else {
            serverConnectionStats.incrementClientConnectionCount();
          }
          trackedConnections.add(ctx);
          newConnections.remove(ctx);
          Attribute<Boolean> activated = ctx.channel().attr(CHANNEL_ACTIVATED);
          activated.set(true);
        }
      } catch (Exception e) {
        LOGGER.error("Got exception when scanning new connections", e);
      }
    }, 0, 1, java.util.concurrent.TimeUnit.SECONDS);
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    newConnections.add(ctx);
    /**
     * The reason to record connection request here is that we want to record all the connection requests,
     * regardless of whether Server is able to extract a valid cert or not later on.
     */
    serverConnectionStats.newConnectionRequest();
    super.channelActive(ctx);
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof SslHandshakeCompletionEvent && ((SslHandshakeCompletionEvent) evt).isSuccess()) {
      Long initStartTs = ctx.channel().attr(CHANNEL_INIT_START_TS).getAndSet(null);
      if (initStartTs != null) {
        serverConnectionStats.recordNewConnectionSetupLatency(LatencyUtils.getElapsedTimeFromNSToMS(initStartTs));
      }
    }
    super.userEventTriggered(ctx, evt);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    Attribute<Boolean> activated = ctx.channel().attr(CHANNEL_ACTIVATED);
    if (activated.get() != null && activated.get()) {
      activated.set(false);
      SslHandler sslHandler = extractSslHandler(ctx);
      if (sslHandler == null) {
        LOGGER.error("Failed to extract ssl handler in function: channelInactive");
      } else {
        String principalName = getPrincipal(sslHandler);
        if (principalName == null) {
          LOGGER.error("Failed to extract principal name from ssl handler in function: channelInactive");
        } else if (principalName.contains(routerPrincipalName)) {
          serverConnectionStats.decrementRouterConnectionCount();
        } else {
          serverConnectionStats.decrementClientConnectionCount();
        }
      }
    }

    newConnections.remove(ctx);
    trackedConnections.remove(ctx);
    super.channelInactive(ctx);
  }

  protected SslHandler extractSslHandler(ChannelHandlerContext ctx) {
    return ServerHandlerUtils.extractSslHandler(ctx);
  }

  private String getPrincipal(SslHandler sslHandler) {
    try {
      SSLSession session = sslHandler.engine().getSession();
      String remoteCN = null;
      for (Certificate cert: session.getPeerCertificates()) {
        if (cert instanceof X509Certificate) {
          if (identityParser != null) {
            remoteCN = identityParser.parseIdentityFromCert((X509Certificate) cert);
            break;
          } else {
            X500Principal cn = ((X509Certificate) cert).getSubjectX500Principal();
            if (cn != null) {
              remoteCN = cn.getName();
              break;
            }
          }
        }
      }
      return remoteCN;
    } catch (Exception e) {
      return null;
    }
  }
}
