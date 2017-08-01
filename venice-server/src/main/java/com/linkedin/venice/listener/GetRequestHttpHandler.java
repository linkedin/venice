package com.linkedin.venice.listener;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.request.GetRouterRequest;
import com.linkedin.venice.listener.request.MultiGetRouterRequestWrapper;
import com.linkedin.venice.listener.response.HttpShortcutResponse;
import com.linkedin.venice.meta.QueryAction;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

import java.net.URI;


/**
 * Monitors the stream, when it gets enough bytes that form a genuine object,
 * it deserializes the object and passes it along the stack.
 *
 * {@link SimpleChannelInboundHandler#channelRead(ChannelHandlerContext, Object)} will release the incoming request object:
 * {@link FullHttpRequest} for each request.
 * The downstream handler is not expected to use this object any more.
 */
public class GetRequestHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
  private static final String API_VERSION = "1";
  private final StatsHandler statsHandler;

  public GetRequestHttpHandler(StatsHandler handler) {
    super();
    this.statsHandler = handler;
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

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) throws Exception {
    try {
      QueryAction action = getQueryActionFromRequest(req);
      switch (action){
        case STORAGE:  // GET /storage/store/partition/key
          HttpMethod requestMethod = req.method();
          if (requestMethod.equals(HttpMethod.GET)) {
            // TODO: evaluate whether we can replace single-get by multi-get
            GetRouterRequest getRouterRequest = GetRouterRequest.parseGetHttpRequest(req);
            statsHandler.setStoreName(getRouterRequest.getStoreName());
            statsHandler.setRequestType(getRouterRequest.getRequestType());
            ctx.fireChannelRead(getRouterRequest);
          } else if (requestMethod.equals(HttpMethod.POST)){
            // Multi-get
            MultiGetRouterRequestWrapper multiGetRouterReq = MultiGetRouterRequestWrapper.parseMultiGetHttpRequest(req);
            statsHandler.setStoreName(multiGetRouterReq.getStoreName());
            statsHandler.setRequestType(multiGetRouterReq.getRequestType());
            statsHandler.setRequestKeyCount(multiGetRouterReq.getKeyCount());
            ctx.fireChannelRead(multiGetRouterReq);
          } else {
            throw new VeniceException("Unknown request method: " + requestMethod + " for " + QueryAction.STORAGE);
          }
          break;
        case HEALTH:
          statsHandler.setHealthCheck(true);
          ctx.writeAndFlush(new HttpShortcutResponse("OK", HttpResponseStatus.OK));
          break;
        default:
          throw new VeniceException("Unrecognized query action");
      }
    } catch (VeniceException e){
      ctx.writeAndFlush(new HttpShortcutResponse(
          e.getMessage(),
          HttpResponseStatus.BAD_REQUEST
      ));
    }
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
      IdleStateEvent e = (IdleStateEvent)evt;
      if (e.state() == IdleState.ALL_IDLE) {
        // Close the connection after idling for a certain period
        ctx.close();
        return;
      }
    }
    super.userEventTriggered(ctx, evt);
  }

  static QueryAction getQueryActionFromRequest(HttpRequest req){
    // Sometimes req.uri() gives a full uri (eg https://host:port/path) and sometimes it only gives a path
    // Generating a URI lets us always take just the path.
    String[] requestParts = URI.create(req.uri()).getPath().split("/");
    HttpMethod reqMethod = req.method();
    if ((reqMethod.equals(HttpMethod.GET) || reqMethod.equals(HttpMethod.POST)) &&
        requestParts.length >=2 &&
        requestParts[1].equalsIgnoreCase(QueryAction.STORAGE.toString())) {
      return QueryAction.STORAGE;
    } else if (req.method().equals(HttpMethod.GET) &&
        requestParts.length >=2 &&
        requestParts[1].equalsIgnoreCase(QueryAction.HEALTH.toString())) {
      return QueryAction.HEALTH;
    } else {
      throw new VeniceException("Only able to parse GET requests for actions: storage, health.  Cannot parse request for: " + req.uri());
    }
  }
}
