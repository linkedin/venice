package com.linkedin.venice.router;

import static com.linkedin.venice.router.api.VenicePathParser.ACTION_DISABLE;
import static com.linkedin.venice.router.api.VenicePathParser.ACTION_ENABLE;
import static com.linkedin.venice.router.api.VenicePathParser.TASK_READ_QUOTA_THROTTLE;
import static com.linkedin.venice.router.api.VenicePathParserHelper.parseRequest;
import static com.linkedin.venice.utils.NettyUtils.setupResponseAndFlush;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.RequestConstants;
import com.linkedin.venice.acl.AccessController;
import com.linkedin.venice.acl.AclException;
import com.linkedin.venice.router.api.RouterResourceType;
import com.linkedin.venice.router.api.VenicePathParserHelper;
import com.linkedin.venice.router.stats.AdminOperationsStats;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.SslUtils;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


@ChannelHandler.Sharable
public class AdminOperationsHandler extends SimpleChannelInboundHandler<HttpRequest> {
  private static final Logger LOGGER = LogManager.getLogger(AdminOperationsHandler.class);
  private static final byte[] EMPTY_BYTES = new byte[0];
  private static final RedundantExceptionFilter EXCEPTION_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

  public static final String READ_THROTTLING_ENABLED = "readThrottlingEnabled";
  public static final String EARLY_THROTTLE_ENABLED = "earlyThrottleEnabled";

  private final AccessController accessController;
  private final AdminOperationsStats adminOperationsStats;
  private final RouterServer routerServer;
  private final VeniceRouterConfig routerConfig;
  private final ScheduledExecutorService executor;
  private ScheduledFuture routerReadQuotaThrottlingLeaseFuture;

  private final boolean initialReadThrottlingEnabled;
  private final boolean initialEarlyThrottleEnabled;

  public AdminOperationsHandler(
      AccessController accessController,
      RouterServer server,
      AdminOperationsStats adminOperationsStats) {
    this.accessController = accessController;
    this.adminOperationsStats = adminOperationsStats;
    routerServer = server;
    routerConfig = server.getConfig();
    routerReadQuotaThrottlingLeaseFuture = null;

    initialReadThrottlingEnabled = routerConfig.isReadThrottlingEnabled();
    initialEarlyThrottleEnabled = routerConfig.isEarlyThrottleEnabled();

    if (initialReadThrottlingEnabled || initialEarlyThrottleEnabled) {
      executor = Executors.newSingleThreadScheduledExecutor();
    } else {
      executor = null;
    }
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpRequest req) throws IOException {
    HttpMethod method = req.method();

    VenicePathParserHelper pathHelper = parseRequest(req);

    final RouterResourceType resourceType = pathHelper.getResourceType();
    final String adminTask = pathHelper.getResourceName();

    if (resourceType != RouterResourceType.TYPE_ADMIN) {
      // Pass request to the next channel if it's not an admin operation
      ReferenceCountUtil.retain(req);
      ctx.fireChannelRead(req);
      return;
    }

    adminOperationsStats.recordAdminRequest();

    // Since AdminOperationsHandler comes after health check, it should not receive requests without a task
    if (StringUtils.isEmpty(adminTask)) {
      adminOperationsStats.recordErrorAdminRequest();
      sendUserErrorResponse("Admin operations must specify a task", ctx);
      return;
    }

    if (accessController != null) {
      try {
        SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
        if (sslHandler == null) {
          throw new AclException("Non SSL Admin request received");
        }

        X509Certificate clientCert =
            SslUtils.getX509Certificate(sslHandler.engine().getSession().getPeerCertificates()[0]);
        if (!accessController.hasAccessToAdminOperation(clientCert, adminTask)) {
          throw new AclException(
              accessController.getPrincipalId(clientCert) + " does not have access to admin operation: " + adminTask);
        }
      } catch (AclException e) {
        LOGGER.warn(
            "Exception occurred! Access rejected: {} requestedMethod: {} uri:{}. ",
            ctx.channel().remoteAddress(),
            method,
            req.uri(),
            e);
        sendErrorResponse(HttpResponseStatus.FORBIDDEN, "Access Rejected", ctx);
        return;
      }
    }

    LOGGER.info(
        "Received admin operation request from {} - method: {} task: {} action: {}",
        ctx.channel().remoteAddress(),
        method,
        adminTask,
        pathHelper.getKey());
    if (HttpMethod.GET.equals(method)) {
      handleGet(pathHelper, ctx);
    } else if (HttpMethod.POST.equals(method)) {
      handlePost(pathHelper, ctx, pathHelper.extractQueryParameters(req));
    } else {
      sendUserErrorResponse("Unsupported request method " + method, ctx);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
    adminOperationsStats.recordErrorAdminRequest();
    InetSocketAddress sockAddr = (InetSocketAddress) (ctx.channel().remoteAddress());
    String remoteAddr = sockAddr.getHostName() + ":" + sockAddr.getPort();
    if (!EXCEPTION_FILTER.isRedundantException(sockAddr.getHostName(), e)) {
      LOGGER.error(
          "Got exception while handling admin operation request from {}, and error: {}",
          remoteAddr,
          e.getMessage());
    }
    setupResponseAndFlush(INTERNAL_SERVER_ERROR, EMPTY_BYTES, false, ctx);
    ctx.close();
  }

  private void handleGet(VenicePathParserHelper pathHelper, ChannelHandlerContext ctx) throws IOException {
    final String task = pathHelper.getResourceName();
    final String action = pathHelper.getKey();

    if (TASK_READ_QUOTA_THROTTLE.equals(task)) {
      if (StringUtils.isEmpty(action)) {
        sendReadQuotaThrottleStatus(ctx);
      } else {
        sendUserErrorResponse("GET admin task " + TASK_READ_QUOTA_THROTTLE + " can not specify an action", ctx);
      }
    } else {
      sendUnimplementedErrorResponse(task, ctx);
    }
  }

  private void handlePost(VenicePathParserHelper pathHelper, ChannelHandlerContext ctx, Map<String, String> queryParams)
      throws IOException {
    final String task = pathHelper.getResourceName();
    final String action = pathHelper.getKey();

    if (StringUtils.isEmpty(action)) {
      sendUserErrorResponse("Admin operations must have an action", ctx);
      return;
    }

    if (TASK_READ_QUOTA_THROTTLE.equals(task)) {
      if (ACTION_ENABLE.equals(action)) {
        // A REST call to enable quota will only enable it if the router was initially configured to enable quota
        resetReadQuotaThrottling(queryParams);
        sendReadQuotaThrottleStatus(ctx);
      } else if (ACTION_DISABLE.equals(action)) {
        disableReadQuotaThrottling();
        sendReadQuotaThrottleStatus(ctx);
      } else {
        sendUserErrorResponse("Unsupported action " + action + " for task " + task, ctx);
      }
    } else {
      sendUnimplementedErrorResponse(task, ctx);
    }
  }

  private void resetReadQuotaThrottling(Map<String, String> queryParams) {
    String delay = queryParams.get(RequestConstants.DELAY_EXECUTION);
    if (delay != null) {
      if (routerReadQuotaThrottlingLeaseFuture != null && !routerReadQuotaThrottlingLeaseFuture.isDone()) {
        LOGGER.info("Cancelling existing read quota timer.");
        routerReadQuotaThrottlingLeaseFuture.cancel(true);
      }

      routerReadQuotaThrottlingLeaseFuture =
          executor.schedule((Runnable) this::resetReadQuotaThrottling, Long.parseLong(delay), TimeUnit.MILLISECONDS);
    } else {
      resetReadQuotaThrottling();
    }
  }

  private void resetReadQuotaThrottling() {
    routerConfig.setReadThrottlingEnabled(initialReadThrottlingEnabled);
    routerConfig.setEarlyThrottleEnabled(initialEarlyThrottleEnabled);
    routerServer.setReadRequestThrottling(initialReadThrottlingEnabled);
  }

  /**
   * This method temporarily disables read quota throttling. When disabling the read quota throttling, a lease is set.
   * When the lease expires, read quota throttling is reset to its initial state.
   */
  private void disableReadQuotaThrottling() {
    if (routerReadQuotaThrottlingLeaseFuture != null && !routerReadQuotaThrottlingLeaseFuture.isDone()) {
      LOGGER.info("Cancelling existing read quota timer.");
      routerReadQuotaThrottlingLeaseFuture.cancel(true);
    }

    routerConfig.setReadThrottlingEnabled(false);
    routerServer.setReadRequestThrottling(false);
    routerConfig.setEarlyThrottleEnabled(false);

    if (initialReadThrottlingEnabled || initialEarlyThrottleEnabled) {
      routerReadQuotaThrottlingLeaseFuture = executor.schedule(
          (Runnable) this::resetReadQuotaThrottling,
          routerConfig.getReadQuotaThrottlingLeaseTimeoutMs(),
          TimeUnit.MILLISECONDS);
    }
  }

  private void sendReadQuotaThrottleStatus(ChannelHandlerContext ctx) throws IOException {
    HashMap<String, String> payload = new HashMap<>();
    payload.put(READ_THROTTLING_ENABLED, String.valueOf(routerConfig.isReadThrottlingEnabled()));
    payload.put(EARLY_THROTTLE_ENABLED, String.valueOf(routerConfig.isEarlyThrottleEnabled()));

    sendSuccessResponse(payload, ctx);
  }

  private void sendUserErrorResponse(String message, ChannelHandlerContext ctx) throws IOException {
    adminOperationsStats.recordErrorAdminRequest();
    HttpResponseStatus status = BAD_REQUEST;
    String errorPrefix = "Error " + status.code() + ": ";
    String errorDesc = message + ". Bad request (not conforming to supported command patterns)!";
    sendErrorResponse(status, errorPrefix + errorDesc, ctx);
  }

  private void sendSuccessResponse(Map<String, String> payload, ChannelHandlerContext ctx) throws IOException {
    sendResponse(OK, payload, ctx);
  }

  private void sendUnimplementedErrorResponse(String task, ChannelHandlerContext ctx) throws IOException {
    adminOperationsStats.recordErrorAdminRequest();
    HttpResponseStatus status = NOT_IMPLEMENTED;
    String errorPrefix = "Error " + status.code() + ": ";
    String errorDesc = "Request " + task + " unimplemented !";
    sendErrorResponse(status, errorPrefix + errorDesc, ctx);
  }

  private void sendErrorResponse(HttpResponseStatus status, String errorMsg, ChannelHandlerContext ctx)
      throws IOException {
    adminOperationsStats.recordErrorAdminRequest();
    Map<String, String> payload = new HashMap<>();
    payload.put("error", errorMsg);

    sendResponse(status, payload, ctx);
  }

  private void sendResponse(HttpResponseStatus status, Map<String, String> payload, ChannelHandlerContext ctx)
      throws JsonProcessingException {
    if (payload == null) {
      setupResponseAndFlush(status, EMPTY_BYTES, true, ctx);
    } else {
      setupResponseAndFlush(status, OBJECT_MAPPER.writeValueAsString(payload).getBytes(), true, ctx);
    }
  }
}
