package com.linkedin.venice.listener;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.RequestConstants;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.message.GetRequestObject;
import com.linkedin.venice.meta.QueryAction;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Monitors the stream, when it gets enough bytes that form a genuine object,
 * it deserializes the object and passes it along the stack.
 */

public class GetRequestHttpHandler extends ChannelInboundHandlerAdapter {
  private static final String API_VERSION = "1";

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
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (msg instanceof HttpRequest) {
      HttpRequest req = (HttpRequest) msg;
      try {
        verifyApiVersion(req.headers(), API_VERSION);
        QueryAction action = getQueryActionFromRequest(req);
        switch (action){
          case STORAGE:  // GET /storage/store/partition/key
            GetRequestObject request = parseReadFromUri(req.getUri());
            ctx.fireChannelRead(request);
            break;
          case HEALTH:
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

  static GetRequestObject parseReadFromUri(String uri){
    String[] requestParts = uri.split("/");
    if (requestParts.length == 5) {//   [0]""/[1]"action"/[2]"store"/[3]"partition"/[4]"key"
      GetRequestObject request = new GetRequestObject();
      request.setStore(requestParts[2]);
      request.setPartition(requestParts[3]);
      request.setKey(getKeyBytesFromUrlKeyString(requestParts[4]));
      return request;
    } else {
      throw new VeniceException("Not a valid request for a STORAGE action: " + uri);
    }
  }

  static QueryAction getQueryActionFromRequest(HttpRequest req){
    String[] requestParts = req.getUri().split("/");
    if (req.getMethod().equals(HttpMethod.GET) &&
        requestParts.length >=2 &&
        requestParts[1].equalsIgnoreCase(QueryAction.STORAGE.toString())) {
      return QueryAction.STORAGE;
    } else if (req.getMethod().equals(HttpMethod.GET) &&
        requestParts.length >=2 &&
        requestParts[1].equalsIgnoreCase(QueryAction.HEALTH.toString())) {
      return QueryAction.HEALTH;
    } else {
      throw new VeniceException("Only able to parse GET requests for actions: storage, health");
    }
  }

  /***
   * throws VeniceException if we don't handle the specified api version
   * @param headers
   */
  static void verifyApiVersion(HttpHeaders headers, String expectedVersion){
    if (headers.contains(HttpConstants.VENICE_API_VERSION)) { /* if not present, assume latest version */
      String clientApiVersion = headers.get(HttpConstants.VENICE_API_VERSION);
      if (!clientApiVersion.equals(expectedVersion)) {
        throw new VeniceException("Storage node is not compatible with requested API version: " + clientApiVersion);
      }
    }
  }

  static Base64.Decoder b64decoder = Base64.getUrlDecoder();
  static byte[] getKeyBytesFromUrlKeyString(String keyString){
    QueryStringDecoder queryStringParser = new QueryStringDecoder(keyString, StandardCharsets.UTF_8);
    String format = RequestConstants.DEFAULT_FORMAT;
    if (queryStringParser.parameters().containsKey(RequestConstants.FORMAT_KEY)) {
      format = queryStringParser.parameters().get(RequestConstants.FORMAT_KEY).get(0);
    }
    switch (format) {
      case RequestConstants.B64_FORMAT:
        return b64decoder.decode(queryStringParser.path());
      default:
        return queryStringParser.path().getBytes(StandardCharsets.UTF_8);
    }
  }
}
