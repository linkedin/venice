package com.linkedin.venice.listener;

import com.linkedin.venice.message.GetRequestObject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import java.util.List;


/**
 * Monitors the stream, when it gets enough bytes that form a genuine object,
 * it deserializes the object and passes it along the stack.
 */

public class GetRequestHttpHandler extends ChannelInboundHandlerAdapter {

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    cause.printStackTrace();
    ctx.close();
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
        if (requestParts.length == 4) {//   ""/"store"/"key"/"partition"

          GetRequestObject request = new GetRequestObject();
          request.setStore(requestParts[1]);
          request.setKey(requestParts[2].getBytes());
          request.setPartition(requestParts[3]);

          ctx.fireChannelRead(request);

        }
      }

    }
  }


}
