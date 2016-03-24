package com.linkedin.venice.listener;

import com.linkedin.venice.RequestConstants;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.message.GetRequestObject;
import com.linkedin.venice.meta.QueryAction;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;


/**
 * Monitors the stream, when it gets enough bytes that form a genuine object,
 * it deserializes the object and passes it along the stack.
 */

public class GetRequestHttpHandler extends ChannelInboundHandlerAdapter {

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    ctx.writeAndFlush(new HttpError(cause.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR));
    ctx.close();
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  /***
   * request format: GET /storename/key/partition
   * Note: this doesn't work for arbitrary byte keys.  Only works for ascii/utf8 keys
   * @param ctx
   * @param msg
   * @throws Exception
   */
  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (msg instanceof HttpRequest) {
      HttpRequest req = (HttpRequest) msg;
      try {
        QueryAction action = getQueryActionFromRequest(req);
        switch (action){
          case READ:  // GET /read/store/partition/key
            GetRequestObject request = parseReadFromUri(req.getUri());
            ctx.fireChannelRead(request);
            break;
          default:
            throw new VeniceException("Unrecognized query action");
        }
      } catch (VeniceException e){
        ctx.writeAndFlush(new HttpError(
            e.getMessage(),
            HttpResponseStatus.BAD_REQUEST
        ));
      }
    }
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
      throw new VeniceException("Not a valid request for a READ action: " + uri);
    }
  }

  static QueryAction getQueryActionFromRequest(HttpRequest req){
    String[] requestParts = req.getUri().split("/");
    if (req.getMethod().equals(HttpMethod.GET) &&
        requestParts.length >=2 &&
        requestParts[1].equals("read")){
      return QueryAction.READ;
    } else {
      throw new VeniceException("Only able to parse GET requests for action: read");
    }
  }

  static Base64.Decoder b64decoder = Base64.getDecoder();
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
