package com.linkedin.venice.listener;

import com.google.common.base.Charsets;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.message.GetRequestObject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;


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

      if (req.getMethod().equals(HttpMethod.GET)){
        String[] requestParts = req.getUri().split("/");
        if (requestParts.length >=2 && requestParts[1].equals("read")){
          if (requestParts.length == 5) {//   [0]""/[1]"action"/[2]"store"/[3]"partition"/[4]"key"
            GetRequestObject request = new GetRequestObject();
            request.setStore(requestParts[2]);
            request.setPartition(requestParts[3]);
            request.setKey(getKeyBytesFromUrlKeyString(requestParts[4]));
            ctx.fireChannelRead(request);
          } else {//end if length
            ctx.writeAndFlush(new HttpError(
                "Request format for action:read is read/resource-name/partition/key[?f=format]",
                HttpResponseStatus.BAD_REQUEST));
          }
        } else {//end if read
          ctx.writeAndFlush(new HttpError(
            "Only able to parse action: read",
            HttpResponseStatus.BAD_REQUEST));
        }
      } else {//end if GET
        ctx.writeAndFlush(new HttpError(
          "Only able to handle GET requests",
          HttpResponseStatus.BAD_REQUEST));
      }
    }//end if HttpRequest
  }



  static Base64.Decoder decoder = Base64.getDecoder();
  static byte[] getKeyBytesFromUrlKeyString(String keyString){
    if (keyString.contains("?")) {
      String[] keyParts = keyString.split("[?]");
      String key = keyParts[0];
      for (String param : keyParts[1].split("[&]")){
        if (param.equals("f=b64")){
          return decoder.decode(key);
        }
      }
      return key.getBytes(StandardCharsets.UTF_8);
    }
    return keyString.getBytes(StandardCharsets.UTF_8);
  }

}
