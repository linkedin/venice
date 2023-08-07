package com.linkedin.venice.listener;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.grpc.GrpcHandlerContext;
import com.linkedin.venice.listener.grpc.GrpcHandlerPipeline;
import com.linkedin.venice.listener.grpc.GrpcStatsContext;
import com.linkedin.venice.listener.grpc.VeniceGrpcHandler;
import com.linkedin.venice.listener.request.AdminRequest;
import com.linkedin.venice.listener.request.ComputeRouterRequestWrapper;
import com.linkedin.venice.listener.request.DictionaryFetchRequest;
import com.linkedin.venice.listener.request.GetRouterRequest;
import com.linkedin.venice.listener.request.HealthCheckRequest;
import com.linkedin.venice.listener.request.MetadataFetchRequest;
import com.linkedin.venice.listener.request.MultiGetRouterRequestWrapper;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.listener.response.HttpShortcutResponse;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.protocols.VeniceClientRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * Monitors the stream, when it gets enough bytes that form a genuine object,
 * it deserializes the object and passes it along the stack.
 *
 * {@link SimpleChannelInboundHandler#channelRead(ChannelHandlerContext, Object)} will release the incoming request object:
 * {@link FullHttpRequest} for each request.
 * The downstream handler is not expected to use this object any more.
 */
public class RouterRequestHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest>
    implements VeniceGrpcHandler {
  private final StatsHandler statsHandler;
  private final Map<String, Integer> storeToEarlyTerminationThresholdMSMap;

  public RouterRequestHttpHandler(StatsHandler handler, Map<String, Integer> storeToEarlyTerminationThresholdMSMap) {
    super();
    this.statsHandler = handler;
    this.storeToEarlyTerminationThresholdMSMap = storeToEarlyTerminationThresholdMSMap;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    ctx.writeAndFlush(new HttpShortcutResponse(cause.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR));
    ctx.close();
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  private void setupRequestTimeout(RouterRequest routerRequest) {
    String storeName = routerRequest.getStoreName();
    Integer timeoutThresholdInMS = storeToEarlyTerminationThresholdMSMap.get(storeName);
    if (timeoutThresholdInMS != null) {
      routerRequest.setRequestTimeoutInNS(
          statsHandler.getRequestStartTimeInNS() + TimeUnit.MILLISECONDS.toNanos(timeoutThresholdInMS));
    }
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) throws Exception {
    try {
      QueryAction action = getQueryActionFromRequest(req);
      statsHandler.setRequestSize(req.content().readableBytes());
      switch (action) {
        case STORAGE: // GET /storage/store/partition/key
          HttpMethod requestMethod = req.method();
          if (requestMethod.equals(HttpMethod.GET)) {
            // TODO: evaluate whether we can replace single-get by multi-get
            GetRouterRequest getRouterRequest = GetRouterRequest.parseGetHttpRequest(req);
            setupRequestTimeout(getRouterRequest);
            statsHandler.setRequestInfo(getRouterRequest);
            ctx.fireChannelRead(getRouterRequest);
          } else if (requestMethod.equals(HttpMethod.POST)) {
            // Multi-get
            MultiGetRouterRequestWrapper multiGetRouterReq = MultiGetRouterRequestWrapper.parseMultiGetHttpRequest(req);
            setupRequestTimeout(multiGetRouterReq);
            statsHandler.setRequestInfo(multiGetRouterReq);
            ctx.fireChannelRead(multiGetRouterReq);
          } else {
            throw new VeniceException("Unknown request method: " + requestMethod + " for " + QueryAction.STORAGE);
          }
          break;
        case COMPUTE: // compute request
          if (req.method().equals(HttpMethod.POST)) {
            ComputeRouterRequestWrapper computeRouterReq = ComputeRouterRequestWrapper.parseComputeRequest(req);
            setupRequestTimeout(computeRouterReq);
            statsHandler.setRequestInfo(computeRouterReq);
            ctx.fireChannelRead(computeRouterReq);
          } else {
            throw new VeniceException("Only support POST method for " + QueryAction.COMPUTE);
          }
          break;
        case HEALTH:
          statsHandler.setHealthCheck(true);
          HealthCheckRequest healthCheckRequest = new HealthCheckRequest();
          ctx.fireChannelRead(healthCheckRequest);
          break;
        case DICTIONARY:
          DictionaryFetchRequest dictionaryFetchRequest = DictionaryFetchRequest.parseGetHttpRequest(req);
          statsHandler.setStoreName(dictionaryFetchRequest.getStoreName());
          ctx.fireChannelRead(dictionaryFetchRequest);
          break;
        case ADMIN:
          AdminRequest adminRequest = AdminRequest.parseAdminHttpRequest(req);
          statsHandler.setStoreName(adminRequest.getStoreName());
          ctx.fireChannelRead(adminRequest);
          break;
        case METADATA:
          MetadataFetchRequest metadataFetchRequest = MetadataFetchRequest.parseGetHttpRequest(req);
          statsHandler.setStoreName(metadataFetchRequest.getStoreName());
          ctx.fireChannelRead(metadataFetchRequest);
          break;
        default:
          throw new VeniceException("Unrecognized query action");
      }
    } catch (VeniceException e) {
      ctx.writeAndFlush(new HttpShortcutResponse(e.getMessage(), HttpResponseStatus.BAD_REQUEST));
    }
  }

  @Override
  public void grpcRead(GrpcHandlerContext ctx, GrpcHandlerPipeline pipeline) {
    VeniceClientRequest clientRequest = ctx.getVeniceClientRequest();
    GrpcStatsContext statsContext = ctx.getGrpcStatsContext();

    RouterRequest routerRequest = clientRequest.getIsBatchRequest()
        ? MultiGetRouterRequestWrapper.parseMultiGetGrpcRequest(clientRequest)
        : GetRouterRequest.grpcGetRouterRequest(clientRequest);

    statsContext.setRequestInfo(routerRequest);

    ctx.setRouterRequest(routerRequest);
    pipeline.processRequest(ctx);
  }

  @Override
  public void grpcWrite(GrpcHandlerContext ctx, GrpcHandlerPipeline pipeline) {
    pipeline.processResponse(ctx);
  }

  /**
   * This function is used to support http keep-alive.
   * For now, the connection will keep open if the idle time is less than the configured
   * threshold, we might need to consider to close it after a long period of time,
   * such as 12 hours.
   * @param ctx
   * @param evt
   * @throws Exception
   */
  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof IdleStateEvent) {
      IdleStateEvent e = (IdleStateEvent) evt;
      if (e.state() == IdleState.ALL_IDLE) {
        // Close the connection after idling for a certain period
        ctx.close();
        return;
      }
    }
    super.userEventTriggered(ctx, evt);
  }

  static QueryAction getQueryActionFromRequest(HttpRequest req) {
    // Sometimes req.uri() gives a full uri (eg https://host:port/path) and sometimes it only gives a path
    // Generating a URI lets us always take just the path.
    String[] requestParts = URI.create(req.uri()).getPath().split("/");
    HttpMethod reqMethod = req.method();
    if ((!reqMethod.equals(HttpMethod.GET) && !reqMethod.equals(HttpMethod.POST)) || requestParts.length < 2) {
      String actions =
          Arrays.stream(QueryAction.values()).map(e -> e.toString().toLowerCase()).collect(Collectors.joining(", "));
      throw new VeniceException(
          "Only able to parse GET or POST requests for actions: " + actions + ". " + "Cannot parse request for: "
              + req.uri());
    }

    try {
      return QueryAction.valueOf(requestParts[1].toUpperCase());
    } catch (IllegalArgumentException illegalArgumentException) {
      String actions =
          Arrays.stream(QueryAction.values()).map(e -> e.toString().toLowerCase()).collect(Collectors.joining(", "));
      throw new VeniceException(
          "Only able to parse GET or POST requests for actions: " + actions + ". " + "Cannot support action: "
              + requestParts[1],
          illegalArgumentException);
    }
  }
}
